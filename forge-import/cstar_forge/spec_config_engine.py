"""
Phase 2 processing engine: ingest a :class:`SpecConfig` and run the heavy work
(``generate_inputs`` + ``configure_build``) on *this* machine.

This is the counterpart to the Phase-1 resolver (``spec_config_resolve``). It runs
on the user's machine of choice, where the **host** (machine config + data paths) is
resolved from :mod:`cstar_forge.config` — nothing host-specific is read from the
config file. The reviewed, host-independent ``SpecConfig`` provides everything else.

Strategy (see ``docs/spec-config-inventory.md`` §3): the existing
``CstarSpecBuilder`` already performs host resolution, grid building, input
generation, and namelist/cppdefs writing. So Phase 2:

1. reconstructs a ``CstarSpecBuilder`` from the config's atomic inputs (identity,
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

import argparse
import copy
import logging
import warnings
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Protocol, Tuple, Union, runtime_checkable

from .namelist_model import validate_run_time_sections
from .spec_config import SpecConfig
from .spec_config_resolve import n_tracers_from_model_settings


@runtime_checkable
class SpecConfigExecutor(Protocol):
    """The execution surface that :func:`process_spec_config` drives.

    This is the seam between the host-independent ``SpecConfig`` and whatever
    actually generates inputs / configures the build on the run machine. Today
    ``cstar_forge._core.CstarSpecBuilder`` satisfies it; when the engine moves into
    C-Star as an application, that app provides its own implementation and the only
    change here is the default factory.

    (``runtime_checkable`` so ``process_spec_config`` can assert an executor exposes
    these methods — name presence only; signatures are duck-typed.)
    """

    def ensure_source_data(self, *args: Any, **kwargs: Any) -> Any: ...

    def generate_inputs(self, *args: Any, **kwargs: Any) -> Any: ...

    def configure_build(self, *args: Any, **kwargs: Any) -> Any: ...

    def path_blueprint(self, *args: Any, **kwargs: Any) -> Any: ...


# A factory maps a host-independent SpecConfig to a ready-to-run executor. The
# forge default builds a CstarSpecBuilder; a C-Star app would provide its own.
ExecutorFactory = Callable[[SpecConfig], SpecConfigExecutor]

logger = logging.getLogger(__name__)

# Sections the config omits because they are filled at processing time; listed here
# only for documentation/clarity (the overlay never touches them).
PROCESSING_FILLED_SECTIONS = ("grid", "initial", "forcing", "s_coord",
                              "title", "output_root_name")


def sources_to_forcing_override(cfg: SpecConfig) -> Optional[Dict[str, Any]]:
    """Convert cfg.forcing to the forcing_override dict for RomsMarblInputData.

    Returns None when sources are model defaults (composition.forcing.origin ==
    "model_default"), so the builder falls back to model_spec.inputs as before.
    When the user has made a ForcingSpec selection or edits, returns a dict with
    ``initial_conditions`` and ``forcing`` keys mirroring the model.yml inputs block.
    """
    if cfg.composition.forcing.origin == "model_default":
        return None

    def _src(spec) -> Dict[str, Any]:
        d: Dict[str, Any] = {"name": spec.name, "climatology": spec.climatology}
        if spec.glorys_layout:
            d["glorys_layout"] = spec.glorys_layout
        return d

    def _item(item) -> Dict[str, Any]:
        d = item.model_dump(exclude={"source"})
        d["source"] = _src(item.source)
        return {k: v for k, v in d.items() if v is not None}

    f = cfg.forcing
    ic_spec = f.initial_conditions
    ic: Dict[str, Any] = {"source": _src(ic_spec.source)}
    if ic_spec.bgc_source:
        ic["bgc_source"] = _src(ic_spec.bgc_source)

    forc: Dict[str, Any] = {}
    for cat, items in [("surface", f.surface), ("boundary", f.boundary),
                       ("tidal", f.tidal), ("river", f.river)]:
        if items:
            forc[cat] = [_item(it) for it in items]

    return {"initial_conditions": ic, "forcing": forc}


def spec_config_to_builder_kwargs(cfg: SpecConfig) -> Dict[str, Any]:
    """Map a ``SpecConfig``'s atomic inputs to ``CstarSpecBuilder`` constructor kwargs.

    Host/machine/path values are intentionally NOT passed — the builder resolves
    those from :mod:`cstar_forge.config` on the run host.
    """
    kwargs = dict(
        description=cfg.identity.description,
        model_name=cfg.identity.model_name,
        grid_name=cfg.identity.grid_name,
        grid_kwargs=dict(cfg.domain.grid_kwargs),
        open_boundaries=cfg.domain.open_boundaries.model_dump(),
        partitioning=cfg.domain.partitioning.model_dump(),
        start_time=cfg.run.start_date,
        end_time=cfg.run.end_date,
        ensemble_id=cfg.identity.ensemble_id,
        cdr_forcing=cfg.forcing.cdr_forcing,
        forcing_override=sources_to_forcing_override(cfg),
        model_reference_date=cfg.run.model_reference_date,
    )
    # nesting: the builder expects grid_kwargs_child to carry an optional "metadata"
    # block (which the SpecConfig stores separately) — re-embed it.
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


def split_model_settings(cfg: SpecConfig) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Split the flat ``model_settings`` into (run_time_overrides, compile_time_overrides).

    ``cppdefs`` is the only compile-time section; everything else is a namelist
    (run-time) section. These are passed to ``configure_build`` as overrides so the
    reviewed config wins over the builder's re-derived defaults.
    """
    run_overrides = {k: copy.deepcopy(v) for k, v in cfg.model_settings.items()}
    cppdefs = run_overrides.pop("cppdefs", None)
    compile_overrides = {"cppdefs": copy.deepcopy(cppdefs)} if cppdefs is not None else {}
    return run_overrides, compile_overrides


def verify_content_hash(cfg: SpecConfig) -> Optional[str]:
    """If the config carries a recorded integrity hash and it no longer matches the
    recomputed hash of the results-affecting data, return a warning message (the file
    appears hand-edited since write-out); otherwise None."""
    recorded = cfg.provenance.content_hash
    if recorded:
        actual = cfg.content_hash()
        if recorded != actual:
            return (
                "spec_config integrity check FAILED: the results-affecting data does "
                "not match the recorded hash — the file appears to have been hand-edited "
                f"since it was written (recorded {recorded[:12]}…, computed {actual[:12]}…). "
                "Processing will continue with the data as read."
            )
    return None


def resolve_host(cfg: Optional[SpecConfig] = None) -> Dict[str, Any]:
    """Resolve the host (machine tag + data paths) from :mod:`cstar_forge.config`.

    If ``cfg`` is given, also include the host-derived run paths
    (``run_output_dir`` / namelist ``output_root_name``) computed from the scratch path.
    """
    from . import config

    p = config.paths
    info: Dict[str, Any] = {
        "system": config.system,
        "paths": {k: str(getattr(p, k)) for k in ("source_data", "input_data", "scratch", "catalog")},
    }
    mc = getattr(config, "machine_config", None)
    if mc is not None:
        info["machine"] = {
            "account": getattr(mc, "account", None),
            "pes_per_node": getattr(mc, "pes_per_node", None),
            "queues": getattr(mc, "queues", None),
        }
    if cfg is not None:
        info["run_output_dir"] = str(cfg.run_output_dir(p.scratch))
        info["output_root_name"] = cfg.output_root_name(p.scratch)
        info["casename"] = cfg.casename
    return info


def host_summary(cfg: Optional[SpecConfig] = None) -> str:
    """A human-readable one-block summary of the resolved host."""
    h = resolve_host(cfg)
    lines = [f"Host: {h['system']}"]
    if "machine" in h and h["machine"].get("account"):
        lines.append(f"  account: {h['machine']['account']}  pes/node: {h['machine'].get('pes_per_node')}")
    for k, v in h["paths"].items():
        lines.append(f"  {k:11s} -> {v}")
    if cfg is not None:
        lines.append(f"  casename     -> {h['casename']}")
        lines.append(f"  run_output   -> {h['run_output_dir']}")
    return "\n".join(lines)


def _default_executor_factory(cfg: SpecConfig) -> SpecConfigExecutor:
    """Forge's default executor: a ``CstarSpecBuilder`` built from the config's
    atomic inputs. Imported lazily so the lightweight bits above (host resolution,
    settings split) stay importable without the full forge stack."""
    from ._core import CstarSpecBuilder

    return CstarSpecBuilder(**spec_config_to_builder_kwargs(cfg))


def process_spec_config(
    spec: Union[SpecConfig, str, Path],
    *,
    ensure_data: bool = True,
    generate: bool = True,
    configure: bool = True,
    clobber: bool = False,
    use_dask: bool = True,
    partition_files: bool = False,
    validate: bool = True,
    executor_factory: Optional[ExecutorFactory] = None,
) -> SpecConfigExecutor:
    """Run Phase-2 processing for a ``SpecConfig`` (object or path to a YAML file).

    Resolves the host from :mod:`cstar_forge.config`, then drives a
    :class:`SpecConfigExecutor` through ``ensure_source_data`` → ``generate_inputs``
    → ``configure_build`` (the reviewed ``model_settings`` overlaid via the last).

    Returns the executor (``CstarSpecBuilder`` by default), so callers can reach
    ``.path_blueprint('build')`` / ``.prep_cstar_environment(...)`` / ``.run()``.

    Parameters
    ----------
    validate :
        If True (default), fail fast — validate the config's ``model_settings``
        against the run-time schema *before* any downloads/generation.
    executor_factory :
        Maps the ``SpecConfig`` to an executor; defaults to the forge
        ``CstarSpecBuilder``. Injectable for tests and for the eventual C-Star app
        (which supplies its own executor for the same ``SpecConfig`` blueprint).
    """
    cfg = spec if isinstance(spec, SpecConfig) else SpecConfig.from_yaml(spec)

    integrity = verify_content_hash(cfg)
    if integrity:
        logger.warning(integrity)
        warnings.warn(integrity, UserWarning, stacklevel=2)

    if validate:
        problems = validate_run_time_sections(cfg.model_settings)
        if problems:
            raise ValueError(
                "spec_config.model_settings has invalid values (fix before "
                "processing):\n  " + "\n  ".join(problems)
            )

    logger.info("Resolved host:\n%s", host_summary(cfg))

    factory = executor_factory or _default_executor_factory
    executor = factory(cfg)
    if not isinstance(executor, SpecConfigExecutor):
        raise TypeError(
            f"executor_factory returned {type(executor).__name__}, which does not "
            "implement the SpecConfigExecutor interface (ensure_source_data / "
            "generate_inputs / configure_build / path_blueprint)."
        )

    if ensure_data:
        executor.ensure_source_data()
    if generate:
        executor.generate_inputs(clobber=clobber, use_dask=use_dask,
                                 partition_files=partition_files)
    if configure:
        run_overrides, compile_overrides = split_model_settings(cfg)
        executor.configure_build(compile_time_settings=compile_overrides,
                                 run_time_settings=run_overrides,
                                 n_tracers=n_tracers_from_model_settings(cfg.model_settings))
    return executor


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cstar_forge.spec_config_engine",
        description="Phase 2: process a spec_config.yml on this machine "
                    "(generate inputs + configure build).")
    parser.add_argument("spec_config", help="path to a spec_config.yml")
    parser.add_argument("--no-data", action="store_true", help="skip ensure_source_data")
    parser.add_argument("--no-generate", action="store_true", help="skip generate_inputs")
    parser.add_argument("--no-configure", action="store_true", help="skip configure_build")
    parser.add_argument("--clobber", action="store_true", help="overwrite existing input files")
    parser.add_argument("--no-dask", action="store_true", help="disable dask in input generation")
    parser.add_argument("--host-only", action="store_true",
                        help="just print the resolved host and exit")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = SpecConfig.from_yaml(args.spec_config)
    print(host_summary(cfg))
    if args.host_only:
        return 0

    builder = process_spec_config(
        cfg,
        ensure_data=not args.no_data,
        generate=not args.no_generate,
        configure=not args.no_configure,
        clobber=args.clobber,
        use_dask=not args.no_dask,
    )
    if not args.no_configure:
        print(f"\nBuild blueprint: {builder.path_blueprint(stage='build')}")
        print("Run it with:  cstar blueprint run <path>")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
