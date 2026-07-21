"""
Phase 2 processing engine: ingest a :class:`ForgeBlueprint` and run the heavy work
(``generate_inputs`` + ``configure_build``) on *this* machine.

This is the counterpart to the Phase-1 resolver (``forge_blueprint_resolve``). It runs
on the user's machine of choice, where the **host** (machine config + data paths) is
resolved from :mod:`cstar_forge.config` — nothing host-specific is read from the
config file. The reviewed, host-independent ``ForgeBlueprint`` provides everything else.

Strategy (see ``docs/forge-blueprint-inventory.md`` §3): the existing
``ForgeExecutor`` already performs host resolution, grid building, input
generation, and namelist/cppdefs writing. So Phase 2:

1. reconstructs a ``ForgeExecutor`` from the config's atomic inputs (identity,
   run window, grid kwargs, boundaries, partitioning, CDR) — the builder resolves
   the host and the artifact-derived values (``s_coord``, file paths, ``run_output_dir``,
   ``output_root_name``) itself;
2. runs ``ensure_source_data`` → ``generate_inputs``;
3. **overlays the reviewed ``model_settings``** from the config onto the builder via
   ``configure_build(compile_time_settings=…, run_time_settings=…)`` — so any edits a
   user made to the config (physics/output/cppdefs/timestep) win over the builder's
   re-derived defaults, while the processing-filled sections (grid/initial/forcing/
   s_coord/title/output_root_name, which the config deliberately omits) come from the
   builder.

The result is the usual blueprint + NetCDF inputs + ``namelist.nml`` + ``cppdefs.opt``,
ready for ``cstar blueprint run``.
"""

from __future__ import annotations

import copy
import logging
import warnings
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import (
    Any,
    Protocol,
    runtime_checkable,
)

from cstar_forge.forge.forge_blueprint import ForgeBlueprint
from cstar_forge.forge.host import HostPaths
from cstar_forge.forge.namelist_model import validate_run_time_sections


@runtime_checkable
class ForgeBlueprintExecutor(Protocol):
    """The execution surface that :func:`process_forge_blueprint` drives.

    This is the seam between the host-independent ``ForgeBlueprint`` and whatever
    actually generates inputs / configures the build on the run machine. Today
    ``cstar_forge.forge.executor.ForgeExecutor`` satisfies it; when the engine moves into
    C-Star as an application, that app provides its own implementation and the only
    change here is the default factory.

    (``runtime_checkable`` so ``process_forge_blueprint`` can assert an executor exposes
    these methods — name presence only; signatures are duck-typed.)
    """

    def ensure_source_data(self, *args: Any, **kwargs: Any) -> Any: ...

    def generate_inputs(self, *args: Any, **kwargs: Any) -> Any: ...

    def configure_build(self, *args: Any, **kwargs: Any) -> Any: ...

    def path_roms_marbl_blueprint(self, *args: Any, **kwargs: Any) -> Any: ...


# A factory maps a host-independent ForgeBlueprint + the injected host to a ready-to-run
# executor. The forge default builds a ForgeExecutor; a C-Star app would provide its own.
ExecutorFactory = Callable[[ForgeBlueprint, HostPaths | None], ForgeBlueprintExecutor]

logger = logging.getLogger(__name__)

# Sections the config omits because they are filled at processing time; listed here
# only for documentation/clarity (the overlay never touches them).
PROCESSING_FILLED_SECTIONS = (
    "grid",
    "initial",
    "forcing",
    "s_coord",
    "title",
    "output_root_name",
)

# Leaf keys that ``generate_inputs`` (Phase 2a, in ``input_data.py``) derives from the
# *actual* generated forcing/tidal objects — never from ``ForgeBlueprint.model_settings``.
# The resolver (Phase 1) never computes real values for these: it leaves them at
# whatever the ModelSpec's disabled placeholder says (``river_frc``/``cdr_frc``/
# ``cdr_output``) or at a merely *declared*, not actually-generated, value (``tides``'
# ``ntides`` from a tidal item, if the item set one). Unlike ``PROCESSING_FILLED_SECTIONS``,
# these sections DO exist in ``model_settings`` (so ``configure_build``'s ``allow_new=False``
# path finds them and deep-merges into them) and carry some fields with real, reviewable
# ModelSpec defaults (e.g. ``cdr_frc.relocate_to_wet_pts``) — so only the specific
# generation-derived leaves are excluded from the overlay, not the whole section.
#
# Without this exclusion, ``configure_build``'s overlay (which applies the *entire*
# stored ``model_settings`` snapshot on top of whatever ``generate_inputs`` just derived)
# silently reverts a correctly-generated river/CDR configuration back to "disabled" and
# can restore a stale tidal constituent count — see docs/forge-blueprint-parameter-audit.md
# §3a for the full trace, and ``tests/test_forge_blueprint.py::TestForgeBlueprintEngine
# ::test_split_model_settings_excludes_generation_derived_leaves`` /
# ``test_configure_build_does_not_clobber_generated_river_and_cdr_settings`` for the
# regression coverage.
GENERATION_DERIVED_LEAF_KEYS: dict[str, tuple[str, ...]] = {
    "river_frc": (
        "river_source",
        "analytical",
        "nriv",
        "rvol_vname",
        "rvol_tname",
        "rtrc_vname",
        "rtrc_tname",
    ),
    "cdr_frc": (
        "cdr_source",
        "cdr_file",
        "ncdr_parm",
        "forcing_parameterized",
        "cdr_volume",
    ),
    "cdr_output": ("do_cdr",),
    # Only ntides is genuinely generation-derived (the real tidal-constituent count
    # is only known once TPXO data is actually extracted). bry_tides/pot_tides/
    # ana_tides are static booleans -- the resolver/model_settings is their single
    # official source (e.g. a child grid forces bry_tides=False there).
    "tides": ("ntides",),
}


def sources_to_forcing_override(cfg: ForgeBlueprint) -> dict[str, Any]:
    """Convert cfg.forcing to the forcing_override dict for RomsMarblInputData.

    Always returns a dict with ``initial_conditions`` and ``forcing`` keys mirroring
    the model.yaml inputs block. ``cfg.forcing`` is fully resolved by the Phase-1
    resolver (from the model default or an authored/edited selection), so the executor
    always drives input generation from this dict and never reads ``model_spec.inputs``.
    """

    def _src(spec) -> dict[str, Any]:
        d: dict[str, Any] = {"name": spec.name, "climatology": spec.climatology}
        if spec.glorys_layout:
            d["glorys_layout"] = spec.glorys_layout
        return d

    def _item(item) -> dict[str, Any]:
        # mode="json" coerces enum-typed fields (SurfaceType, BoundaryType, …) to their
        # string values. Plain model_dump() would leave them as enum *instances*, which
        # then leak into output filenames (f"{key}-{type}") and into roms-tools' SafeDumper
        # (which cannot represent a Forge enum) → the "cannot represent an object" warning.
        # Dates are NOT carried here (injected later via `extra`), so json-coercion is safe.
        d = item.model_dump(exclude={"source"}, mode="json")
        d["source"] = _src(item.source)
        return {k: v for k, v in d.items() if v is not None}

    def _ic(spec) -> dict[str, Any]:
        # Mirror _item, but IC carries a second SourceSpec (bgc_source) that also
        # needs _src conversion. Forwarding the typed fields
        # (bgc_interpolation_method, allow_flex_time) and the options passthrough
        # here is what lets authored/UI IC choices actually reach input_data —
        # previously only source/bgc_source were propagated, so any other IC field
        # set in the wizard was silently dropped on the ForgeBlueprint path.
        d = spec.model_dump(exclude={"source", "bgc_source"}, mode="json")
        d["source"] = _src(spec.source)
        if spec.bgc_source:
            d["bgc_source"] = _src(spec.bgc_source)
        return {k: v for k, v in d.items() if v is not None}

    f = cfg.forcing
    ic = _ic(f.initial_conditions)

    forc: dict[str, Any] = {}
    for cat, items in [
        ("surface", f.surface),
        ("boundary", f.boundary),
        ("tidal", f.tidal),
        ("river", f.river),
    ]:
        if items:
            forc[cat] = [_item(it) for it in items]

    return {"initial_conditions": ic, "forcing": forc}


def forge_blueprint_to_builder_kwargs(cfg: ForgeBlueprint) -> dict[str, Any]:
    """Map a ``ForgeBlueprint``'s atomic inputs to ``ForgeExecutor`` constructor kwargs.

    Host/machine/path values are intentionally NOT passed — the builder resolves
    those from :mod:`cstar_forge.config` on the run host.
    """
    kwargs = dict(
        description=cfg.identity.description,
        name=cfg.name,
        grid_name=cfg.domain.grid_name,
        grid_kwargs=dict(cfg.domain.grid_kwargs),
        topography_source=getattr(
            cfg.domain.topography_source, "value", cfg.domain.topography_source
        ),
        topography_path=cfg.domain.topography_path,
        open_boundaries=cfg.domain.open_boundaries.model_dump(),
        partitioning=cfg.domain.partitioning.model_dump(),
        start_time=cfg.run.start_date,
        end_time=cfg.run.end_date,
        cdr_forcing=cfg.forcing.cdr_forcing,
        forcing_override=sources_to_forcing_override(cfg),
        model_reference_date=cfg.run.model_reference_date,
        source_dataset_keys=list(cfg.datasets),
        resolved_datasets={
            name: rd.model_dump() for name, rd in cfg.forcing.resolved_datasets.items()
        },
        resolved_settings=copy.deepcopy(cfg.model_settings),
        code_spec=cfg.code,
    )
    # nesting: the builder expects grid_kwargs_child to carry an optional "metadata"
    # block (which the ForgeBlueprint stores separately) — re-embed it.
    if cfg.domain.grid_kwargs_child is not None:
        child = dict(cfg.domain.grid_kwargs_child)
        meta = dict(cfg.domain.metadata_child or {})
        if cfg.domain.nesting_include_pressure_fluxes:
            meta["include_pressure_fluxes"] = True
        if meta:
            child["metadata"] = meta
        kwargs["grid_kwargs_child"] = child
    if cfg.domain.grid_kwargs_parent is not None:
        kwargs["grid_kwargs_parent"] = dict(cfg.domain.grid_kwargs_parent)
    return kwargs


def split_model_settings(cfg: ForgeBlueprint) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split the flat ``model_settings`` into (run_time_overrides, compile_time_overrides).

    ``cppdefs`` is the only compile-time section; everything else is a namelist
    (run-time) section. These are passed to ``configure_build`` as overrides so the
    reviewed config wins over the builder's re-derived defaults.

    Excludes the leaf keys in ``GENERATION_DERIVED_LEAF_KEYS`` from the run-time
    overrides: those are only ever meaningfully known once ``generate_inputs`` has run
    against the real forcing data, so the stored (pre-generation) snapshot must not
    overwrite them. See ``GENERATION_DERIVED_LEAF_KEYS`` for why.
    """
    run_overrides = {k: copy.deepcopy(v) for k, v in cfg.model_settings.items()}
    cppdefs = run_overrides.pop("cppdefs", None)
    compile_overrides = (
        {"cppdefs": copy.deepcopy(cppdefs)} if cppdefs is not None else {}
    )
    for section, leaf_keys in GENERATION_DERIVED_LEAF_KEYS.items():
        sub = run_overrides.get(section)
        if isinstance(sub, dict):
            for key in leaf_keys:
                sub.pop(key, None)
    return run_overrides, compile_overrides


def verify_content_hash(cfg: ForgeBlueprint) -> str | None:
    """If the config carries a recorded integrity hash and it no longer matches the
    recomputed hash of the results-affecting data, return a warning message (the file
    appears hand-edited since write-out); otherwise None.
    """
    recorded = cfg.provenance.content_hash
    if recorded:
        actual = cfg.content_hash()
        if recorded != actual:
            return (
                "forge_blueprint integrity check FAILED: the results-affecting data does "
                "not match the recorded hash — the file appears to have been hand-edited "
                f"since it was written (recorded {recorded[:12]}…, computed {actual[:12]}…). "
                "Processing will continue with the data as read."
            )
    return None


def _default_executor_factory(
    cfg: ForgeBlueprint, host: HostPaths | None = None
) -> ForgeBlueprintExecutor:
    """Forge's default executor: a ``ForgeExecutor`` built from the config's
    atomic inputs + the injected host. Imported lazily so the lightweight bits above
    (settings split) stay importable without the full forge stack.
    """
    from cstar_forge.forge.executor import ForgeExecutor

    return ForgeExecutor.from_forge_blueprint(cfg, host=host)


def process_forge_blueprint(
    spec: ForgeBlueprint | str | Path,
    *,
    host: HostPaths | None = None,
    ensure_data: bool = True,
    generate: bool = True,
    configure: bool = True,
    clobber: bool = False,
    use_dask: bool = True,
    subchunk: bool = False,
    validate: bool = True,
    executor_factory: ExecutorFactory | None = None,
    only_inputs: Iterable[str] | None = None,
) -> ForgeBlueprintExecutor:
    """Run Phase-2 processing for a ``ForgeBlueprint`` (object or path to a YAML file).

    Drives a :class:`ForgeBlueprintExecutor` through ``ensure_source_data`` →
    ``generate_inputs`` → ``configure_build`` (the reviewed ``model_settings`` overlaid
    via the last).

    Returns the executor (``ForgeExecutor`` by default), so callers can reach
    ``.path_roms_marbl_blueprint()`` / ``.prep_cstar_environment(...)`` / ``.run()``.

    Parameters
    ----------
    host :
        The resolved ``HostPaths`` (data dirs + machine identity), *injected* by the
        caller — this module does not resolve the host itself, so it carries no
        ``cstar_forge.config`` dependency and relocates cleanly into C-Star. Forge's
        entry points (``cstar_forge.run``) supply it via ``config.resolve_host()``;
        C-Star will supply its own. Only used here for logging; the executor resolves
        its own paths. When ``None``, the host line is not logged.
    validate :
        If True (default), fail fast — validate the config's ``model_settings``
        against the run-time schema *before* any downloads/generation.
    executor_factory :
        Maps the ``ForgeBlueprint`` to an executor; defaults to the forge
        ``ForgeExecutor``. Injectable for tests and for the eventual C-Star app
        (which supplies its own executor for the same ``ForgeBlueprint`` blueprint).
    only_inputs :
        One-off subset mode: category names (see
        ``input_data.resolve_input_selection`` for the accepted aliases —
        ``grid``, ``initial_conditions``/``ic``, ``surface``, ``boundary``/``bry``,
        ``tidal``/``tides``, ``river``, ``cdr``) to generate, skipping the rest.
        Validated up front (before ``ensure_source_data``) so a typo fails fast. The
        ``grid`` step always runs regardless of selection (every other input depends
        on the in-memory grid object). When set, ``configure`` is forced to ``False``
        — a partial input set would only produce an incomplete, misleading
        downstream blueprint; ``persist()`` (which writes ``B_{name}.yaml``) only
        runs inside ``configure_build``, so a subset run can never overwrite an
        existing complete blueprint. Re-run without ``only_inputs`` later to
        generate the remaining inputs (existing ones are reused, per the normal
        skip-existing logic) and emit the blueprint.
    subchunk :
        Interim hack (see ``glorys_subchunk.py``): just-in-time build a
        kerchunk-subchunked reference for multi-file GLORYS sources and read from
        it instead of the raw per-day files. Default False.
    """
    cfg = spec if isinstance(spec, ForgeBlueprint) else ForgeBlueprint.from_yaml(spec)

    integrity = verify_content_hash(cfg)
    if integrity:
        logger.warning(integrity)
        warnings.warn(integrity, UserWarning, stacklevel=2)

    if validate:
        problems = validate_run_time_sections(cfg.model_settings)
        if problems:
            raise ValueError(
                "forge_blueprint.model_settings has invalid values (fix before "
                "processing):\n  " + "\n  ".join(problems)
            )

    resolved_only = None
    if only_inputs is not None:
        from cstar_forge.forge.input_data import resolve_input_selection

        resolved_only = resolve_input_selection(only_inputs)
        if configure:
            logger.warning(
                "only_inputs=%s given: skipping configure_build (a subset of "
                "inputs cannot produce a complete blueprint). Re-run without "
                "only_inputs once the desired inputs exist to generate the rest "
                "and emit the blueprint.",
                sorted(resolved_only),
            )
        configure = False

    if host is not None:
        logger.info("Resolved host:\n%s", host.summary(casename=cfg.casename))

    factory = executor_factory or _default_executor_factory
    executor = factory(cfg, host)
    if not isinstance(executor, ForgeBlueprintExecutor):
        raise TypeError(
            f"executor_factory returned {type(executor).__name__}, which does not "
            "implement the ForgeBlueprintExecutor interface (ensure_source_data / "
            "generate_inputs / configure_build / path_roms_marbl_blueprint)."
        )

    if ensure_data:
        executor.ensure_source_data()
    if generate:
        executor.generate_inputs(
            clobber=clobber, use_dask=use_dask, subchunk=subchunk, only=resolved_only
        )
    if configure:
        run_overrides, compile_overrides = split_model_settings(cfg)
        executor.configure_build(
            compile_time_settings=compile_overrides,
            run_time_settings=run_overrides,
            n_tracers=cfg.n_tracers,
        )
    return executor
