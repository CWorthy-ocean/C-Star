"""
Input data generation classes for CSFORGE models.

This module provides classes for generating input data files for ocean models.
The base InputData class defines the interface, and RomsMarblInputData provides
the ROMS-MARBL specific implementation.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import time
import warnings
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import cstar.applications.roms_marbl.models as cstar_models
import roms_tools as rt
import xarray as xr
import yaml
from cstar.orchestration.models import Resource
from pydantic import BaseModel, ConfigDict, Field

from cstar_forge.forge import source_data
from cstar_forge.forge.forge_blueprint import OpenBoundaries
from cstar_forge.utils import mem_log

log = logging.getLogger(__name__)

# Basename stem for CDR NetCDF: ``{domain_name}_cdr.nc``. The full name must contain the
# substring ``cdr.nc`` so C-Star's ROMS build check on ``cdr_frc.opt`` passes.
CDR_FORCING_NETCDF_STEM = "cdr"

# Matches the part of a candidate filename's stem that follows a planned output's
# stem, for the known roms-tools multi-file suffixes: grouped time chunks
# (``_YYYYMM``/``_YYYY``, e.g. ``_202001``), climatology (``_clim``), and
# ``partition_netcdf`` tiles (``.N``, e.g. ``.0``). Empty match = exact stem.
# Deliberately does NOT match arbitrary suffixes like ``_child`` -- see
# ``RomsMarblInputData._matches_planned_output``.
_PLANNED_OUTPUT_TAIL_RE = re.compile(r"^(_\d+|_clim|\.\d+)?$")


def filter_paths_by_time_window(
    paths: list[Path],
    start: datetime,
    end: datetime,
) -> list[Path]:
    """
    Subset per-day source files to those whose filename date falls in [start, end].

    Daily-staged sources (e.g. GLORYS, see ``SourceData._construct_glorys_path``)
    encode the date as a trailing ``YYYYMMDD`` in the stem. Dates are compared at
    day resolution, inclusive on both ends. If any filename has no parseable date,
    or the filter would leave nothing, the original list is returned unchanged —
    callers must never end up with fewer files than the consumer needs.
    """
    dated: list[tuple[Path, datetime]] = []
    for p in paths:
        m = re.search(r"(\d{8})(?!.*\d{8})", Path(p).stem)  # last YYYYMMDD in stem
        if m is None:
            return paths
        try:
            dated.append((p, datetime.strptime(m.group(1), "%Y%m%d")))
        except ValueError:
            return paths
    lo, hi = start.date(), end.date()
    kept = [p for p, d in dated if lo <= d.date() <= hi]
    if not kept:
        return paths
    return kept


def netcdf_filename_component(component: str) -> str:
    """
    Sanitize a domain or input-name segment for ``{a}_{b}.nc`` basenames.

    Generated NetCDF files must not contain ``.`` except the ``.nc`` suffix (e.g. version
    strings like ``v0.1`` become ``v0_1``).
    """
    return str(component).replace(".", "_")


class RomsMarblBlueprintInputData(BaseModel):
    """
    Subset of RomsMarblBlueprint containing only input data fields.

    This includes only the fields related to input data generation:
    - grid
    - initial_conditions
    - forcing
    - cdr_forcing
    """

    model_config = ConfigDict(extra="forbid")

    grid: cstar_models.Dataset | None = Field(default=None, validate_default=False)
    """Grid dataset."""

    initial_conditions: cstar_models.Dataset | None = Field(
        default=None, validate_default=False
    )
    """Initial conditions dataset."""

    forcing: cstar_models.ForcingConfiguration | None = Field(
        default=None, validate_default=False
    )
    """Forcing configuration."""

    cdr_forcing: cstar_models.Dataset | None = Field(
        default=None, validate_default=False
    )
    """CDR forcing dataset."""

    nesting_info: cstar_models.Dataset | None = Field(
        default=None, validate_default=False
    )
    """Nesting info dataset (only set when a child grid is present)."""


@dataclass
class InputData:
    """
    Base class for generating input data files for ocean models.

    This class defines the interface for input data generation. Subclasses
    should implement the model-specific generation methods.
    """

    # Core configuration
    domain_name: str
    start_date: Any
    end_date: Any

    # Output directory for generated NetCDFs — injected by the caller (executor).
    # Required: input_data no longer resolves paths from cstar_forge.config (so C-Star can
    # supply its own when the forge application relocates). kw_only so subclasses can
    # declare required positional fields without dataclass default-ordering conflicts.
    input_data_dir: Path = field(kw_only=True)

    def __post_init__(self):
        """Create the injected output directory."""
        self.input_data_dir = Path(self.input_data_dir)
        self.input_data_dir.mkdir(parents=True, exist_ok=True)

    def generate_all(self):
        """
        Generate all input files for this model.

        Subclasses should implement this method to generate all required inputs.
        """
        raise NotImplementedError("Subclasses must implement generate_all()")

    def _forcing_filename(self, input_name: str) -> Path:
        """Construct the NetCDF filename for a given input name."""
        d = netcdf_filename_component(self.domain_name)
        stem = netcdf_filename_component(input_name)
        return self.input_data_dir / f"{d}_{stem}.nc"

    def _ensure_empty_or_clobber(self, clobber: bool) -> bool:
        """
        Ensure the input_data_dir is either empty or, if clobber=True,
        remove existing .nc files.
        """
        existing = list(self.input_data_dir.glob("*.nc"))

        if existing and not clobber:
            # Count is all *.nc in the directory; reuse applies only to *planned* outputs
            # (see generate_all), which may be fewer — e.g. partitioned/suffixed names or
            # leftover files from other runs.
            print(
                f"ℹ️  Input directory contains {len(existing)} .nc file(s): {self.input_data_dir}\n"
                "   (Continuing without clobber; per-step reuse follows the planned output list.)"
            )
            return True

        if existing and clobber:
            print(
                f"⚠️  Clobber=True: removing {len(existing)} existing .nc files in "
                f"{self.input_data_dir}..."
            )
            for f in existing:
                f.unlink()

        return True


# Input generation registry
class InputStep:
    """Metadata for a single ROMS input generation step."""

    def __init__(self, name: str, order: int, label: str, handler: Callable):
        self.name = name  # canonical key used for filenames & paths
        self.order = order  # execution order
        self.label = label  # human-readable label
        self.handler = (
            handler  # function expecting `self` (RomsMarblInputData instance)
        )


INPUT_REGISTRY: dict[str, InputStep] = {}

# User-facing aliases for `--only-inputs` / `only=` selection, mapped onto the
# canonical INPUT_REGISTRY keys. Category-level only (no per-item selection).
_INPUT_SELECTION_ALIASES: dict[str, str] = {
    "grid": "grid",
    "initial_conditions": "initial_conditions",
    "ic": "initial_conditions",
    "initial": "initial_conditions",
    "surface": "forcing.surface",
    "forcing.surface": "forcing.surface",
    "boundary": "forcing.boundary",
    "bry": "forcing.boundary",
    "forcing.boundary": "forcing.boundary",
    "tidal": "forcing.tidal",
    "tides": "forcing.tidal",
    "forcing.tidal": "forcing.tidal",
    "river": "forcing.river",
    "rivers": "forcing.river",
    "forcing.river": "forcing.river",
    "cdr": "cdr_forcing",
    "cdr_forcing": "cdr_forcing",
}


def resolve_input_selection(names: Iterable[str]) -> set[str]:
    """
    Normalize a user-supplied ``--only-inputs``/``only=`` selection to the
    canonical ``INPUT_REGISTRY`` key set.

    Lowercases each name and maps it through ``_INPUT_SELECTION_ALIASES``.
    Raises ``ValueError`` listing the valid names if any token is unrecognized.
    """
    resolved: set[str] = set()
    unknown: list[str] = []
    for raw in names:
        key = _INPUT_SELECTION_ALIASES.get(raw.strip().lower())
        if key is None:
            unknown.append(raw)
        else:
            resolved.add(key)
    if unknown:
        valid = sorted(set(_INPUT_SELECTION_ALIASES))
        raise ValueError(
            f"Unknown input selection {unknown!r}. Valid names: {', '.join(valid)}"
        )
    return resolved


def register_input(name: str, order: int, label: str | None = None):
    """
    Decorator to register an input-generation step.

    Parameters
    ----------
    name : str
        Key for this input (e.g., 'grid', 'initial_conditions', 'forcing.surface').
        This will be used in filenames, and to index the registry.
    order : int
        Execution order in `generate_all()`. Lower numbers run first.
    label : str, optional
        Human-readable label for progress messages. If omitted, `name` is used.
    """

    def decorator(func: Callable):
        step_label = label or name
        INPUT_REGISTRY[name] = InputStep(
            name=name,
            order=order,
            label=step_label,
            handler=func,
        )
        return func

    return decorator


@dataclass
class RomsMarblInputData(InputData):
    """
    ROMS-MARBL specific input data generation.

    This class handles generation of all ROMS-MARBL input files including:
    - Grid
    - Initial conditions
    - Surface forcing
    - Boundary forcing
    - Tidal forcing
    - River forcing
    - CDR forcing
    - Corrections
    """

    grid: rt.Grid
    boundaries: OpenBoundaries
    source_data: source_data.SourceData
    roms_marbl_blueprint_dir: Path
    partitioning: cstar_models.PartitioningParameterSet
    cdr_forcing: dict | None = None
    forcing_override: dict[str, Any] | None = None
    """The fully-resolved initial-conditions + forcing selection driving input generation.
    Keys mirror the inputs block structure: 'initial_conditions', 'forcing' (with sub-keys
    'surface', 'boundary', 'tidal', 'river'). Always supplied on the ForgeBlueprint path (the
    resolver fills it from the model default or an authored selection); the grid is generated
    from the injected ``grid`` object regardless."""
    model_reference_date: datetime | None = None
    """ROMS model reference date (t=0). Forwarded to every rt object that accepts it.
    If None, roms-tools defaults to 2000-01-01."""
    grid_parent: rt.Grid | None = None
    grid_child: rt.Grid | None = None
    metadata_child: dict[str, Any] | None = None
    use_dask: bool = True
    use_pio: bool = False
    """Whether ROMS is built against ParallelIO. Every roms-tools save is left at
    its default format (NETCDF4/HDF5 -- fast); when ``use_pio`` is True, each
    output is additionally routed through :meth:`_pio_mangle`/:meth:`_pio_finalize`
    to produce a CDF-5 (``NETCDF3_64BIT_DATA``) file via an ``nccopy -k cdf5``
    subprocess instead of roms-tools' native (much slower at scale) CDF-5 writer.
    Off by default; mirrors ``ForgeExecutor._use_pio``."""
    subchunk: bool = False
    """Interim/experimental (see ``glorys_subchunk.py``): when True, just-in-time
    build a kerchunk-subchunked reference for multi-file GLORYS sources and hand its
    path through in place of the raw per-day files. No roms-tools patching involved
    -- kerchunk's own xarray backend auto-detects the reference, so roms-tools reads
    it via its normal loader. Off by default; enabled via ``--subchunk``."""
    stage_ic_sources: bool = False
    """I/O performance experiment: when True, copy the initial-conditions source
    files (physics + bgc) into ``input_data_dir/staged_sources`` (the per-run
    working directory, typically on scratch) before constructing
    ``rt.InitialConditions``, and point it at the copies. Existing same-size
    copies are reused. Off by default; enabled via ``--stage-ic-sources``."""
    verbose: bool = False
    """Runtime diagnostic flag (forwarded from ``ForgeExecutor.verbose``): forwarded
    as ``verbose=`` to the roms-tools calls that support it (make_nesting_info), and
    gates timing/memory instrumentation (``mem_log``) around every roms-tools
    constructor and ``.save()`` in this class. Off by default; enabled via
    ``--verbose``."""

    # Memoized subchunk reference paths, keyed by dataset key (e.g. "GLORYS_REGIONAL"),
    # so IC + boundary + bgc-physics-boundary reuse one reference instead of rebuilding.
    _subchunk_refs: dict[str, Path] = field(default_factory=dict, init=False)

    # Blueprint elements containing input data
    roms_marbl_blueprint_elements: RomsMarblBlueprintInputData = field(init=False)

    # Settings dictionaries
    _settings_compile_time: dict = field(init=False)
    _settings_run_time: dict = field(init=False)

    # Coarse grid dimension flag (set during surface forcing generation)
    include_coarse_dims: bool | None = field(default=None)
    _clobber: bool = field(default=False, init=False)
    _existing_planned_outputs: set[Path] = field(default_factory=set, init=False)
    _planned_output_paths: set[Path] = field(default_factory=set, init=False)
    """All planned NetCDF outputs for this run (resolved paths), computed once in
    ``generate_all``. Used to exclude a *different* planned output's file (e.g. a
    child grid) from counting as evidence that *this* planned output exists."""

    def __post_init__(self):
        """Initialize paths, storage, and input list."""
        super().__post_init__()

        input_list = []

        # Grid is always generated from the injected ``grid`` object (built by the
        # executor); its handler ignores the entry kwargs, so an empty payload suffices.
        input_list.append(("grid", {}))

        # Initial conditions and forcing come from the fully-resolved forcing_override
        # (filled by the Phase-1 resolver from the model default or an authored selection).
        if self.forcing_override is None:
            raise ValueError(
                "RomsMarblInputData requires a forcing_override (resolved initial "
                "conditions + forcing); none was provided."
            )
        fo = self.forcing_override
        if fo.get("initial_conditions"):
            input_list.append(("initial_conditions", dict(fo["initial_conditions"])))
        for category, items in (fo.get("forcing") or {}).items():
            for item in items or []:
                input_list.append((f"forcing.{category}", dict(item)))

        # Optional user-provided CDR forcing via builder kwarg.
        # Merge with model-specified cdr_list if that input already exists.
        if self.cdr_forcing:
            input_list.append(("cdr_forcing", {"cdr_kwargs": self.cdr_forcing}))

        self.input_list = input_list

        # Sanity check: verify all function keys are registered
        unique_keys = {fk for fk, _ in self.input_list}
        registry_keys = set(INPUT_REGISTRY.keys())
        missing = sorted(unique_keys - registry_keys)
        if missing:
            raise ValueError(
                "The following inputs are listed in `input_list` but "
                f"have no registered handlers: {', '.join(missing)}"
            )

        # Initialize roms_marbl_blueprint_elements with empty datasets
        forcing_keys = {"boundary", "surface", "tidal", "river", "corrections"}
        forcing_dict = {}
        for key in unique_keys:
            # Extract subkey for forcing categories
            if key.startswith("forcing."):
                subkey = key.split(".", 1)[1]
                if subkey in forcing_keys:
                    forcing_dict[subkey] = cstar_models.Dataset(data=[])

        # Check that required forcing categories are present
        if forcing_dict:
            if "boundary" not in forcing_dict:
                raise ValueError(
                    "Missing required 'boundary' forcing category. "
                    "Boundary forcing must be specified in forcing_override."
                )
            if "surface" not in forcing_dict:
                raise ValueError(
                    "Missing required 'surface' forcing category. "
                    "Surface forcing must be specified in forcing_override."
                )

        # Create ForcingConfiguration if we have forcing categories
        forcing_config = None
        if forcing_dict:
            forcing_config = cstar_models.ForcingConfiguration(**forcing_dict)

        # Initialize roms_marbl_blueprint_elements
        self.roms_marbl_blueprint_elements = RomsMarblBlueprintInputData(
            grid=cstar_models.Dataset(data=[]) if "grid" in unique_keys else None,
            initial_conditions=cstar_models.Dataset(data=[])
            if "initial_conditions" in unique_keys
            else None,
            forcing=forcing_config,
            cdr_forcing=cstar_models.Dataset(data=[])
            if "cdr_forcing" in unique_keys
            else None,
        )

        # Initialize settings dictionaries to empty dicts
        self._settings_compile_time = {}
        self._settings_run_time = {}

    def _pio_mangle(self, path: Path) -> Path:
        """When ``use_pio``, insert an ``_nc4`` token before the final ``.nc`` suffix
        of ``path`` so roms-tools writes its (fast, NETCDF4-default) output under a
        distinct name -- leaving the real target name free for :meth:`_pio_finalize`
        to claim only once the CDF-5 conversion has actually succeeded. A no-op when
        PIO is off.
        """
        if not self.use_pio:
            return path
        return path.with_name(path.stem + "_nc4" + path.suffix)

    def _pio_finalize(self, result):
        """Convert roms-tools' ``_nc4``-mangled NETCDF4 output(s) to CDF-5 in place
        of roms-tools' own (much slower at scale) CDF-5 writer.

        For each path in ``result`` (a single path, or a list/tuple of them --
        the container shape is preserved), runs ``nccopy -k cdf5`` from the
        ``_nc4``-mangled file to the corresponding de-mangled final name, then
        deletes the ``_nc4`` source. De-mangling only touches the basename, so an
        ``_nc4`` substring anywhere in the containing directory path is untouched.
        Raises on a non-zero ``nccopy`` exit; the ``_nc4`` file is left in place
        (and nothing is recorded as final) so a re-run regenerates cleanly. A no-op
        (returns ``result`` unchanged) when PIO is off.
        """
        if not self.use_pio:
            return result

        is_scalar = not isinstance(result, (list, tuple))
        entries = [result] if is_scalar else list(result)

        finalized: list[str] = []
        for entry in entries:
            nc4_path = Path(entry)
            if nc4_path.suffix != ".nc":
                nc4_path = nc4_path.with_suffix(".nc")
            final_path = nc4_path.with_name(nc4_path.name.replace("_nc4", "", 1))

            with mem_log(f"pio_nccopy[{nc4_path.name}]", enabled=self.verbose):
                subprocess.run(
                    ["nccopy", "-k", "cdf5", str(nc4_path), str(final_path)],
                    check=True,
                )
            nc4_path.unlink()

            finalized.append(str(final_path))

        return finalized[0] if is_scalar else finalized

    def generate_all(
        self,
        clobber: bool = False,
        partition_files: bool = False,
        test: bool = False,
        only: set[str] | None = None,
    ):
        """
        Generate all ROMS input files for this grid using the registered
        steps whose names appear in `input_list`.

        Parameters
        ----------
        clobber : bool, optional
            If True, overwrite existing input files.
        partition_files : bool, optional
            If True, partition input files across tiles.
        test : bool, optional
            If True, truncate the loop after 2 iterations for testing purposes.
        only : set[str], optional
            Canonical ``INPUT_REGISTRY`` keys (see ``resolve_input_selection``) to
            restrict generation to. The ``grid`` step always runs regardless (cheap,
            and reused if already on disk), since every other step depends on the
            in-memory grid object. When ``None`` (default), all registered steps in
            ``input_list`` run as before.
        """
        log.debug(
            "generate_all: entering for %r (clobber=%s, test=%s, only=%s)",
            self.domain_name,
            clobber,
            test,
            only,
        )
        self._clobber = clobber
        if not self._ensure_empty_or_clobber(clobber):
            return None, {}, {}

        # Build list of (step, kwargs) tuples, sorted by order
        step_kwargs_list = []
        for function_key, kwargs in self.input_list:
            if function_key in INPUT_REGISTRY:
                step = INPUT_REGISTRY[function_key]
                step_kwargs_list.append((step, kwargs))

        step_kwargs_list.sort(key=lambda x: x[0].order)
        total = len(step_kwargs_list) + (1 if partition_files else 0)

        # Compute planned outputs once at the start of execution, and record which already exist.
        planned = self._planned_netcdf_outputs(step_kwargs_list)
        self._planned_output_paths = {path.resolve() for path in planned}
        self._existing_planned_outputs = {
            path.resolve()
            for path in planned
            if self._planned_netcdf_already_present(path)
        }
        n_planned = len(planned)
        n_already = len(self._existing_planned_outputs)
        if n_already:
            print(
                f"ℹ️  Planned NetCDF outputs this run: {n_planned}; "
                f"{n_already} already on disk (exact match, or a grouped/climatology/"
                "partition suffix, e.g. _202001.nc, _clim.nc, .0.nc) — "
                "generation/save will be skipped for those."
            )

        # Execute
        for idx, (step, kwargs) in enumerate(step_kwargs_list, start=1):
            if step.name == "forcing.boundary" and not any(
                self.boundaries.model_dump().values()
            ):
                print(
                    f"\n⏭️  [{idx}/{total}] Skipping boundary forcing (all open boundaries are False)."
                )
                continue
            if test and step.name != "forcing.boundary":
                continue
            if only is not None and step.name != "grid" and step.name not in only:
                print(
                    f"\n⏭️  [{idx}/{total}] Skipping {step.label} (not in --only-inputs)."
                )
                continue
            print(f"\n▶️  [{idx}/{total}] {step.label}...")
            log.debug("generate_all: step %d/%d %r starting", idx, total, step.name)
            # Coarse per-step timing/memory. On Linux this block's own peak reset can
            # mask an early inner sub-block's peak (e.g. a large constructor that frees
            # before a smaller .save() runs) -- the finer per-operation mem_log blocks
            # inside each step (constructor/save, above) remain the accurate signal.
            with mem_log(f"step:{step.name}", enabled=self.verbose):
                step.handler(self, key=step.name, **kwargs)
            # Truncate after 2 iterations if test mode is enabled
            if test and idx >= 2:
                print(f"\n⚠️  Test mode: truncated after {idx} iterations\n")
                break
        # Partition step (optional)
        if partition_files:
            print(f"\n▶️  [{total}/{total}] Partitioning input files across tiles...")
            self._partition_files()
            print("\n✅ All input files generated and partitioned.\n")
        else:
            print("\n✅ All input files generated.\n")

        return (
            self.roms_marbl_blueprint_elements,
            self._settings_compile_time,
            self._settings_run_time,
        )

    def _planned_netcdf_outputs(
        self, step_kwargs_list: list[tuple[InputStep, dict[str, Any]]]
    ) -> list[Path]:
        """Return the planned NetCDF outputs for this generation run."""
        planned: list[Path] = []
        for step, kwargs in step_kwargs_list:
            if step.name == "grid":
                planned.append(self._forcing_filename("grid"))
                if self.grid_child is not None:
                    planned.append(self._forcing_filename("grid_child"))
                    planned.append(self._forcing_filename("nesting"))
                continue

            if step.name == "initial_conditions":
                planned.append(self._forcing_filename("initial_conditions"))
                continue

            if step.name == "forcing.boundary" and (
                self.grid_parent is not None
                or not any(self.boundaries.model_dump().values())
            ):
                # Keep planned outputs consistent with generate_all(), which skips this step
                # for child/nested domains (boundaries come from the parent's extraction)
                # and when all open boundaries are disabled.
                continue

            if step.name in {"forcing.surface", "forcing.boundary"}:
                forcing_type = kwargs.get("type") if isinstance(kwargs, dict) else None
                suffix = (
                    f"{step.name.split('.', 1)[1]}-{forcing_type}"
                    if forcing_type
                    else step.name.split(".", 1)[1]
                )
                planned.append(self._forcing_filename(suffix))
                continue

            if step.name.startswith("forcing."):
                planned.append(self._forcing_filename(step.name.split(".", 1)[1]))
                continue

            if step.name == "cdr_forcing":
                planned.append(self._forcing_filename(CDR_FORCING_NETCDF_STEM))

        # Preserve order while deduplicating
        deduped: list[Path] = []
        for p in planned:
            if p not in deduped:
                deduped.append(p)
        return deduped

    def _matches_planned_output(self, planned: Path, candidate: Path) -> bool:
        """
        True if ``candidate`` is valid disk evidence that ``planned`` exists.

        Accepts an exact match, or a roms_tools-style multi-file suffix sharing
        ``planned``'s stem: grouped time chunks (``_YYYYMM``/``_YYYY``), climatology
        (``_clim``), or ``partition_netcdf`` tiles (``.N``) -- see
        ``_PLANNED_OUTPUT_TAIL_RE``.

        Excludes two things that would otherwise cause a false "already present":
        - Leftover ``_nc4``-mangled files (see ``_pio_mangle``): the pre-nccopy
          intermediate from a prior run that didn't finish converting, not a valid
          finished output.
        - Any *other* planned output's file (e.g. a child grid's NetCDF must not
          count as evidence the parent grid's NetCDF exists, even though both share
          the ``{domain}_grid`` prefix) -- checked via ``_planned_output_paths``.
        """
        if candidate.suffix != ".nc" or "_nc4" in candidate.name:
            return False
        resolved = candidate.resolve()
        if resolved != planned.resolve() and resolved in self._planned_output_paths:
            return False
        if not candidate.stem.startswith(planned.stem):
            return False
        tail = candidate.stem[len(planned.stem) :]
        return bool(_PLANNED_OUTPUT_TAIL_RE.fullmatch(tail))

    def _planned_netcdf_already_present(self, path: Path) -> bool:
        """
        True if this planned output is already on disk: exact path, or a
        roms_tools-style multi-file suffix sharing the same stem (see
        ``_matches_planned_output``).
        """
        if path.exists():
            return True
        return any(
            self._matches_planned_output(path, candidate)
            for candidate in path.parent.glob(f"{path.stem}*.nc")
        )

    def _should_reuse_existing_output(self, path: Path) -> bool:
        """Return True when this planned output already exists and clobber=False."""
        if self._clobber:
            return False
        return path.resolve() in self._existing_planned_outputs

    def _existing_output_paths(self, path: Path) -> list[str]:
        """
        Return existing NetCDF paths that correspond to a planned output path.

        Some roms_tools writers produce suffixed outputs that share the same stem.
        For example, planning may include ``foo_surface-physics.nc`` while existing
        files are ``foo_surface-physics_202001.nc`` etc. See
        ``_matches_planned_output`` for exactly which suffixes count and which are
        excluded.
        """
        if self._clobber:
            return []

        matches: list[Path] = []
        if path.exists():
            matches.append(path)
        else:
            matches.extend(
                sorted(
                    candidate
                    for candidate in path.parent.glob(f"{path.stem}*.nc")
                    if self._matches_planned_output(path, candidate)
                )
            )

        # De-duplicate while preserving order.
        unique: list[str] = []
        for match in matches:
            match_str = str(match)
            if match_str not in unique:
                unique.append(match_str)
        return unique

    def _interp_frc_surface_reuse(
        self, input_args: dict[str, Any], nc_path: Path
    ) -> int:
        """
        Infer blk/bgc ``interp_frc`` when reusing NetCDF without a ``SurfaceForcing`` instance.

        Uses ``coarse_grid_mode`` when unambiguous; for ``auto``, peeks at the existing file.
        """
        mode = input_args.get("coarse_grid_mode", "auto")
        if mode == "never":
            return 0
        if mode == "always":
            return 1
        try:
            with xr.open_dataset(nc_path, decode_times=False) as ds:
                sizes = getattr(ds, "sizes", ds.dims)
                for dim in ("xi_coarse", "eta_coarse"):
                    if dim in sizes:
                        return 1
        except Exception:
            pass
        return 0

    def _yaml_filename(self, input_name: str) -> Path:
        """Construct the YAML filename for a given input key."""
        self.roms_marbl_blueprint_dir.mkdir(parents=True, exist_ok=True)
        return self.roms_marbl_blueprint_dir / f"_{input_name}.yaml"

    def _resolve_source_block(
        self,
        block: str | dict[str, Any],
        time_window: tuple[datetime, datetime] | None = None,
    ) -> dict[str, Any]:
        """
        Normalize a "source"/"bgc_source" block and inject a 'path'
        based on SourceData.

        When ``time_window`` is given and the resolved path is a per-day file list,
        it is trimmed to the files covering that window (see
        ``filter_paths_by_time_window``) — e.g. initial conditions only need
        ``ini_time``'s day, not the whole run's daily files. A trimmed list skips
        the subchunk branch: the subchunk reference is memoized per dataset key
        and shared with full-window consumers (boundary forcing), so it must only
        ever be built from the full list.
        """
        if isinstance(block, str):
            name = block
            out: dict[str, Any] = {"name": name}
        elif isinstance(block, dict):
            out = dict(block)
            name = out.get("name")
            if not name:
                raise ValueError(f"Source block {block!r} is missing a 'name' field.")
        else:
            raise TypeError(f"Unsupported source block type: {type(block)}")

        # A blank/None path means "derive it" — SourceSpec.model_dump() always emits
        # path=None, so drop the empty key here to let the streamable/setdefault logic
        # below either inject the derived path or omit it (streamable) correctly.
        if out.get("path") in (None, ""):
            out.pop("path", None)

        glorys_layout = out.get("glorys_layout") if name.upper() == "GLORYS" else None

        # If streamable and no path was explicitly provided in YAML, don't add path field.
        # streamable_for_source prefers the pinned ForgeBlueprint resolved_datasets
        # snapshot over a live source_registry check (see SourceData.streamable_for_source).
        if self.source_data.streamable_for_source(name, glorys_layout=glorys_layout):
            if "path" not in out:
                return out
            return out

        path = self.source_data.path_for_source(name, glorys_layout=glorys_layout)
        if path is not None:
            if time_window is not None and isinstance(path, list) and len(path) >= 2:
                trimmed = filter_paths_by_time_window(path, *time_window)
                if len(trimmed) < len(path):
                    log.info(
                        "Trimmed %s source from %d to %d file(s) for time window "
                        "[%s, %s]",
                        name,
                        len(path),
                        len(trimmed),
                        time_window[0],
                        time_window[1],
                    )
                path = trimmed
            elif (
                self.subchunk
                and name.upper() == "GLORYS"
                and isinstance(path, list)
                and len(path) >= 2
            ):
                path = self._subchunked_glorys_path(name, glorys_layout, path)
            out.setdefault("path", path)
        return out

    def _subchunked_glorys_path(
        self,
        name: str,
        glorys_layout: str | None,
        path: list[Path],
    ) -> Path:
        """Build (or reuse) a subchunked kerchunk reference for a multi-file GLORYS
        path, in place of the raw per-day files. roms-tools reads the reference path
        via its normal loader (kerchunk's xarray backend auto-detects it); no
        patching involved -- see ``glorys_subchunk.py``.

        Memoized per dataset key so initial_conditions + forcing.boundary (+ the BGC
        physics-boundary companion) share one reference instead of each rebuilding it.
        """
        from cstar_forge.forge.glorys_subchunk import build_ref_for_files

        key = self.source_data.dataset_key_for_source(name, glorys_layout=glorys_layout)
        ref = self._subchunk_refs.get(key)
        if ref is None:
            ref = build_ref_for_files(
                path,
                out_dir=self.source_data.source_data_dir,
                key=key,
                start=self.source_data.start_time,
                end=self.source_data.end_time,
            )
            self._subchunk_refs[key] = ref
        return ref

    def _build_input_args(
        self,
        key: str,
        extra: dict[str, Any] | None = None,
        base_kwargs: dict[str, Any] | None = None,
        time_window: tuple[datetime, datetime] | None = None,
    ) -> dict[str, Any]:
        """
        Merge per-input defaults with runtime arguments.

        Uses base_kwargs (always provided from input_list).
        Resolves "source" and "bgc_source" through SourceData.
        Merges with extra, where extra overrides defaults.
        """
        # base_kwargs always comes from input_list entries.
        cfg = dict(base_kwargs) if base_kwargs is not None else {}

        # Resolve source blocks (convert SourceSpec Pydantic models to dicts with paths).
        # Skip None values — optional bgc_source etc. are absent when not configured.
        for field_name in ("source", "bgc_source"):
            if field_name in cfg and cfg[field_name] is not None:
                # If it's a Pydantic model (SourceSpec), convert to dict first
                if hasattr(cfg[field_name], "model_dump"):
                    cfg[field_name] = cfg[field_name].model_dump()
                cfg[field_name] = self._resolve_source_block(
                    cfg[field_name], time_window=time_window
                )

        # Unpack any `options` passthrough dict from the item config before merging.
        # These are forwarded verbatim to the rt constructor and win over typed defaults
        # but lose to `extra` (which contains hardcoded run-time injections like dates).
        item_options = cfg.pop("options", None) or {}

        # extra overrides defaults; item_options sit between cfg and extra
        merged = {**cfg, **item_options}
        if extra:
            return {**merged, **extra}
        return merged

    def _stage_source_files(self, input_args: dict[str, Any]) -> dict[str, Any]:
        """
        Copy resolved source/bgc_source files into the working dir and repoint at them.

        I/O performance experiment (``stage_ic_sources``): the working directory
        (scratch) is expected to have better read performance than the project
        space the staged source data lives on. Copies land in
        ``input_data_dir/staged_sources``; a copy that already exists with the
        same size is reused. Blocks without a ``path`` (streamable sources) are
        left untouched. Mutates and returns ``input_args``.
        """
        staging_dir = self.input_data_dir / "staged_sources"
        copied = reused = 0
        copied_bytes = 0
        start = time.perf_counter()

        def _stage(path):
            nonlocal copied, reused, copied_bytes
            if isinstance(path, (list, tuple)):
                return [_stage(p) for p in path]
            src = Path(path)
            dest = staging_dir / src.name
            if dest.exists() and dest.stat().st_size == src.stat().st_size:
                reused += 1
                return dest
            staging_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            copied += 1
            copied_bytes += dest.stat().st_size
            return dest

        for field_name in ("source", "bgc_source"):
            block = input_args.get(field_name)
            if isinstance(block, dict) and block.get("path") is not None:
                block["path"] = _stage(block["path"])
        log.info(
            "Staged IC sources to %s: %d file(s) copied (%.0f MB), %d reused, in %.2fs",
            staging_dir,
            copied,
            copied_bytes / 1024**2,
            reused,
            time.perf_counter() - start,
        )
        return input_args

    # These are registered with @register_input decorator
    def _mrd_extra(self) -> dict[str, Any]:
        """Extra kwargs containing model_reference_date when one is configured."""
        if self.model_reference_date is not None:
            return {"model_reference_date": self.model_reference_date}
        return {}

    @register_input(name="grid", order=10, label="Writing ROMS grid")
    def _generate_grid(self, key: str = "grid", **kwargs):
        """Generate grid input file."""
        out_path = self._forcing_filename(input_name="grid")
        yaml_path = self._yaml_filename(key)

        try:
            self.grid.to_yaml(yaml_path)
        except Exception as e:
            warnings.warn(
                f"Failed to save grid YAML to {yaml_path}: {e}",
                UserWarning,
                stacklevel=2,
            )

        if self._should_reuse_existing_output(out_path):
            print(f"   ↪ Reusing existing file: {out_path}")
        else:
            with mem_log("grid.save", enabled=self.verbose):
                grid_saved = self.grid.save(self._pio_mangle(out_path))
                self._pio_finalize(grid_saved)

        out_path_nesting = None
        if self.grid_child is not None:
            out_path_child = self._forcing_filename(input_name="grid_child")
            yaml_path_child = self._yaml_filename(key + "_child")

            try:
                self.grid_child.to_yaml(yaml_path_child)
            except Exception as e:
                warnings.warn(
                    f"Failed to save child grid YAML to {yaml_path_child}: {e}",
                    UserWarning,
                    stacklevel=2,
                )

            if self._should_reuse_existing_output(out_path_child):
                print(f"   ↪ Reusing existing file: {out_path_child}")
            else:
                with mem_log("grid_child.save", enabled=self.verbose):
                    grid_child_saved = self.grid_child.save(
                        self._pio_mangle(out_path_child)
                    )
                    self._pio_finalize(grid_child_saved)

            out_path_nesting = self._forcing_filename(input_name="nesting")
            if self._should_reuse_existing_output(out_path_nesting):
                print(f"   ↪ Reusing existing file: {out_path_nesting}")
            else:
                # This section of code is needed when doing nesting with BGC.  ROMS_Tools has a flag called "include_bgc" which
                # defaults to false when we are making child boundary conditions, but it needs to be set to true in order to
                # save the BGC variables.
                nesting_kwargs = dict(self.metadata_child or {})
                has_marbl = bool(
                    (self._settings_compile_time.get("cppdefs") or {}).get(
                        "marbl", False
                    )
                )
                if has_marbl:
                    # ROMS-Tools: include_bgc=True sets output_vars to include "bgc" on nesting.nc.
                    nesting_kwargs.setdefault("include_bgc", True)
                nesting_kwargs.setdefault("verbose", self.verbose)
                out_path_nesting_mangled = self._pio_mangle(out_path_nesting)
                with mem_log("make_nesting_info", enabled=self.verbose):
                    rt.make_nesting_info(
                        self.grid,
                        self.grid_child,
                        str(out_path_nesting_mangled),
                        **nesting_kwargs,
                    )
                    self._pio_finalize(out_path_nesting_mangled)
            self.roms_marbl_blueprint_elements.nesting_info = cstar_models.Dataset(
                data=[Resource(location=str(out_path_nesting), partitioned=False)]
            )

        # Append Resource directly to roms_marbl_blueprint_elements.grid
        resource = Resource(location=str(out_path), partitioned=False)
        self.roms_marbl_blueprint_elements.grid.data.append(resource)

        self._settings_run_time["grid"] = dict(
            grid_file=out_path,
        )

        if "cppdefs" not in self._settings_compile_time:
            self._settings_compile_time["cppdefs"] = {}
        self._settings_compile_time["cppdefs"]["obc_west"] = self.boundaries.west
        self._settings_compile_time["cppdefs"]["obc_east"] = self.boundaries.east
        self._settings_compile_time["cppdefs"]["obc_north"] = self.boundaries.north
        self._settings_compile_time["cppdefs"]["obc_south"] = self.boundaries.south

        if "param" not in self._settings_run_time:
            self._settings_run_time["param"] = {}
        self._settings_run_time["param"]["llm"] = self.grid.nx
        self._settings_run_time["param"]["mmm"] = self.grid.ny
        self._settings_run_time["param"]["n"] = self.grid.N
        self._settings_run_time["param"]["np_xi"] = self.partitioning.n_procs_x
        self._settings_run_time["param"]["np_eta"] = self.partitioning.n_procs_y

        if out_path_nesting is not None:
            if "extract_data" not in self._settings_run_time:
                self._settings_run_time["extract_data"] = {}
            self._settings_run_time["extract_data"]["do_extract"] = True
            self._settings_run_time["extract_data"]["extract_file"] = "nesting.nc"
            self._settings_run_time["extract_data"]["n_chd"] = self.grid_child.N
            self._settings_run_time["extract_data"]["theta_s_chd"] = (
                self.grid_child.theta_s
            )
            self._settings_run_time["extract_data"]["theta_b_chd"] = (
                self.grid_child.theta_b
            )
            self._settings_run_time["extract_data"]["hc_chd"] = self.grid_child.hc

        self._settings_run_time["s_coord"] = dict(
            tcline=self.grid.hc,
            theta_b=self.grid.theta_b,
            theta_s=self.grid.theta_s,
        )

    @register_input(
        name="initial_conditions", order=20, label="Generating initial conditions"
    )
    def _generate_initial_conditions(self, key: str = "initial_conditions", **kwargs):
        """Generate initial conditions input file."""
        yaml_path = self._yaml_filename(key)
        output_path = self._forcing_filename(input_name="initial_conditions")
        extra = dict(
            ini_time=self.start_date,
            use_dask=self.use_dask,
            **self._mrd_extra(),
        )
        # roms-tools selects the closest time in [ini_time, ini_time + 24h], so
        # per-day source lists only need the day-of and next-day files.
        input_args = self._build_input_args(
            key,
            extra=extra,
            base_kwargs=kwargs,
            time_window=(self.start_date, self.start_date + timedelta(days=1)),
        )

        if self._should_reuse_existing_output(output_path):
            print(f"   ↪ Reusing existing file: {output_path}")
            paths = [str(output_path)]
            ic = None
        else:
            if self.stage_ic_sources:
                input_args = self._stage_source_files(input_args)
            log.info("InitialConditions kwargs: %r", input_args)
            with mem_log("InitialConditions()", enabled=self.verbose):
                ic = rt.InitialConditions(grid=self.grid, **input_args)

            # See here: https://github.com/CWorthy-ocean/roms-tools/issues/553
            try:
                ic.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save initial conditions YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )

            with mem_log("InitialConditions.save", enabled=self.verbose):
                paths = self._pio_finalize(ic.save(self._pio_mangle(output_path)))

        # Append Resources directly to roms_marbl_blueprint_elements.initial_conditions
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                self.roms_marbl_blueprint_elements.initial_conditions.data.append(
                    resource
                )
        else:
            resource = Resource(location=paths, partitioned=False)
            self.roms_marbl_blueprint_elements.initial_conditions.data.append(resource)

        self._settings_run_time["initial"] = dict(
            initial_file=paths[0],
        )

    @register_input(
        name="forcing.surface", order=30, label="Generating surface forcing"
    )
    def _generate_surface_forcing(self, key: str = "forcing.surface", **kwargs):
        """Generate surface forcing input files."""
        # Extract subkey from "forcing.surface" -> "surface"
        subkey = key.split(".", 1)[1] if "." in key else key

        extra = dict(
            start_time=self.start_date,
            end_time=self.end_date,
            use_dask=self.use_dask,
            **self._mrd_extra(),
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        type = input_args.get("type")
        if type is None:
            raise ValueError(
                f"Missing required 'type' key in input_args for '{key}'. "
                f"Expected 'type' to be 'physics' or 'bgc'."
            )
        if type not in {"physics", "bgc", "restoring"}:
            raise ValueError(
                f"Invalid 'type' value '{type}' in input_args for '{key}'. "
                f"Expected 'type' to be 'physics', 'bgc', or 'restoring'."
            )

        source_name = input_args.get("source").get("name")
        if input_args.get("type") == "bgc" and source_name == "MBL_co2":
            yaml_path = self._yaml_filename(f"{key}-{type}-co2")
            output_path = self._forcing_filename(input_name=f"surface-{type}-co2")
        else:
            yaml_path = self._yaml_filename(f"{key}-{type}")
            output_path = self._forcing_filename(input_name=f"surface-{type}")

        existing_paths = self._existing_output_paths(output_path)
        frc = None
        if existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            if not yaml_path.exists():
                warnings.warn(
                    f"Surface forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                    "constructing SurfaceForcing once to write YAML (this may be slow).",
                    UserWarning,
                    stacklevel=2,
                )
                with mem_log(
                    "SurfaceForcing() [yaml-sidecar-only]", enabled=self.verbose
                ):
                    frc = rt.SurfaceForcing(grid=self.grid, **input_args)
                try:
                    frc.to_yaml(yaml_path)
                except Exception as e:
                    warnings.warn(
                        f"Failed to save surface forcing YAML to {yaml_path}: {e}",
                        UserWarning,
                        stacklevel=2,
                    )
        else:
            with mem_log("SurfaceForcing()", enabled=self.verbose):
                frc = rt.SurfaceForcing(grid=self.grid, **input_args)
            try:
                frc.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save surface forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )
            with mem_log("SurfaceForcing.save", enabled=self.verbose):
                paths = self._pio_finalize(frc.save(self._pio_mangle(output_path)))

        if input_args["type"] == "restoring":
            if "sss" in input_args["restoring_forces"]:
                self._settings_compile_time["cppdefs"]["sal_restore"] = True
        elif input_args["type"] == "bgc" and input_args["source"]["name"] == "MBL_co2":
            if "cppdefs" not in self._settings_compile_time:
                self._settings_compile_time["cppdefs"] = {}
            self._settings_compile_time["cppdefs"]["co2_tvarying"] = True

        # Append Resources directly to roms_marbl_blueprint_elements.forcing[subkey]

        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                    resource
                )
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                resource
            )

        if frc is not None and hasattr(frc, "use_coarse_grid"):
            interp_frc = 1 if frc.use_coarse_grid else 0
        else:
            interp_frc = self._interp_frc_surface_reuse(input_args, Path(paths[0]))

        # Only touch 'bgc' if the model has MARBL/BGC — read from cppdefs.marbl
        # (the compile-time flag), which is the single source of truth.
        has_bgc_compile = bool(
            (self._settings_compile_time.get("cppdefs") or {}).get("marbl", False)
        )

        # Set interp_frc in the appropriate section based on forcing type
        # blk_frc.interp_frc is for physics surface forcing
        # bgc.interp_frc is for bgc surface forcing (only if model has bgc)
        # Both should have the same value when present (enforced by check below)
        if "blk_frc" not in self._settings_run_time:
            self._settings_run_time["blk_frc"] = {}
        if has_bgc_compile and "bgc" not in self._settings_run_time:
            self._settings_run_time["bgc"] = {}

        # Check for consistency: all surface forcing types should use the same coarse grid setting
        if "interp_frc" in self._settings_run_time["blk_frc"]:
            if interp_frc != self._settings_run_time["blk_frc"]["interp_frc"]:
                raise ValueError(
                    "Mismatch in coarse grid settings between surface forcing types"
                )
        if has_bgc_compile and "interp_frc" in self._settings_run_time["bgc"]:
            if interp_frc != self._settings_run_time["bgc"]["interp_frc"]:
                raise ValueError(
                    "Mismatch in coarse grid settings between surface forcing types"
                )

        # Set interp_frc for the appropriate section based on type (only set bgc if model has bgc)
        if "bgc" in type and has_bgc_compile:
            self._settings_run_time["bgc"]["interp_frc"] = interp_frc
        else:
            self._settings_run_time["blk_frc"]["interp_frc"] = interp_frc

        self.include_coarse_dims = interp_frc == 1

        if "forcing" not in self._settings_run_time:
            self._settings_run_time["forcing"] = {}

        if "bgc" in type:
            self._settings_run_time["forcing"]["surface_forcing_bgc_path"] = (
                paths[0] if isinstance(paths, (list, tuple)) else paths
            )
        else:
            self._settings_run_time["forcing"]["surface_forcing_path"] = (
                paths[0] if isinstance(paths, (list, tuple)) else paths
            )

    def _build_physics_boundary_companion(self, key: str, extra: dict[str, Any]):
        """Build a physics ``rt.BoundaryForcing`` to anchor density-space BGC
        boundary interpolation (roms-tools >=4 ``physics_forcing=``).

        Locates the physics boundary item registered under ``key`` in
        ``self.input_list`` and constructs a BoundaryForcing from it, reusing the
        same run-time ``extra`` (dates, boundaries, dask). Returns ``None`` (with a
        warning) when no physics boundary item exists, in which case roms-tools
        falls back to depth-space interpolation.
        """
        physics_kwargs = next(
            (
                dict(kw)
                for k, kw in self.input_list
                if k == key and str(kw.get("type") or "physics") == "physics"
            ),
            None,
        )
        if physics_kwargs is None:
            warnings.warn(
                "Density-space BGC boundary interpolation was requested but no physics "
                "boundary item was found to anchor it; roms-tools will fall back to "
                "depth-space interpolation.",
                UserWarning,
                stacklevel=2,
            )
            return None
        physics_args = self._build_input_args(
            key, extra=extra, base_kwargs=physics_kwargs
        )
        with mem_log("BoundaryForcing() [physics companion]", enabled=self.verbose):
            return rt.BoundaryForcing(grid=self.grid, **physics_args)

    @register_input(
        name="forcing.boundary", order=40, label="Generating boundary forcing"
    )
    def _generate_boundary_forcing(self, key: str = "forcing.boundary", **kwargs):
        """Generate boundary forcing input files."""
        # Child/nested domains receive their boundaries from the parent's data
        # extraction (nesting.nc), so they must not generate boundary forcing
        # from reanalysis. A domain is "child" iff it has a parent grid.
        if self.grid_parent is not None:
            return
        # Extract subkey from "forcing.boundary" -> "boundary"
        subkey = key.split(".", 1)[1] if "." in key else key

        extra = dict(
            start_time=self.start_date,
            end_time=self.end_date,
            boundaries=self.boundaries.model_dump()
            if hasattr(self.boundaries, "model_dump")
            else self.boundaries,
            use_dask=self.use_dask,
            **self._mrd_extra(),
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        type = input_args.get("type")
        if type is None:
            raise ValueError(
                f"Missing required 'type' key in input_args for '{key}'. "
                f"Expected 'type' to be 'physics' or 'bgc'."
            )
        if type not in {"physics", "bgc"}:
            raise ValueError(
                f"Invalid 'type' value '{type}' in input_args for '{key}'. "
                f"Expected 'type' to be 'physics' or 'bgc'."
            )

        # Density-space BGC boundary interpolation (roms-tools >=4) needs a physics
        # BoundaryForcing companion to supply the target T/S density coordinate. Build
        # one from this key's physics item and pass it as `physics_forcing`. Without it,
        # roms-tools silently falls back to depth interpolation.
        bgc_interp = str(input_args.get("bgc_interpolation_method") or "depth")
        if type == "bgc" and bgc_interp in {"density", "density_mld"}:
            input_args["physics_forcing"] = self._build_physics_boundary_companion(
                key, extra
            )

        yaml_path = self._yaml_filename(f"{key}-{type}")
        output_path = self._forcing_filename(input_name=f"boundary-{type}")

        existing_paths = self._existing_output_paths(output_path)
        if existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            if not yaml_path.exists():
                warnings.warn(
                    f"Boundary forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                    "constructing BoundaryForcing once to write YAML (this may be slow).",
                    UserWarning,
                    stacklevel=2,
                )
                with mem_log(
                    "BoundaryForcing() [yaml-sidecar-only]", enabled=self.verbose
                ):
                    bry = rt.BoundaryForcing(grid=self.grid, **input_args)
                try:
                    bry.to_yaml(yaml_path)
                except Exception as e:
                    warnings.warn(
                        f"Failed to save boundary forcing YAML to {yaml_path}: {e}",
                        UserWarning,
                        stacklevel=2,
                    )
        else:
            with mem_log("BoundaryForcing()", enabled=self.verbose):
                bry = rt.BoundaryForcing(grid=self.grid, **input_args)
            try:
                bry.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save boundary forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )
            with mem_log("BoundaryForcing.save", enabled=self.verbose):
                paths = self._pio_finalize(bry.save(self._pio_mangle(output_path)))
        # Append Resources directly to roms_marbl_blueprint_elements.forcing[subkey]
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                    resource
                )
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                resource
            )

        if "forcing" not in self._settings_run_time:
            self._settings_run_time["forcing"] = {}

        if "bgc" in type:
            self._settings_run_time["forcing"]["boundary_forcing_bgc_path"] = (
                paths[0] if isinstance(paths, (list, tuple)) else paths
            )
        else:
            self._settings_run_time["forcing"]["boundary_forcing_path"] = (
                paths[0] if isinstance(paths, (list, tuple)) else paths
            )

    @register_input(name="forcing.tidal", order=50, label="Generating tidal forcing")
    def _generate_tidal_forcing(self, key: str = "forcing.tidal", **kwargs):
        """Generate tidal forcing input files."""
        subkey = key.split(".", 1)[1] if "." in key else key
        yaml_path = self._yaml_filename(key)
        output_path = self._forcing_filename(subkey)
        extra = dict(
            use_dask=self.use_dask,
            **self._mrd_extra(),
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        existing_paths = self._existing_output_paths(output_path)
        tidal: rt.TidalForcing = None
        if existing_paths and yaml_path.exists():
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            with yaml_path.open() as f:
                # roms_tools may emit multi-document YAML (e.g. version header + Grid/TidalForcing).
                ntides = None
                for doc in yaml.safe_load_all(f):
                    if doc and isinstance(doc, dict) and "TidalForcing" in doc:
                        ntides = doc["TidalForcing"].get("ntides")
                        break
                if ntides is None:
                    raise ValueError(
                        f"No TidalForcing.ntides found in YAML (expected multi-document "
                        f"roms_tools output): {yaml_path}"
                    )
        elif existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            warnings.warn(
                f"Tidal forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                "constructing TidalForcing once to write YAML (this may be slow).",
                UserWarning,
                stacklevel=2,
            )
            with mem_log("TidalForcing() [yaml-sidecar-only]", enabled=self.verbose):
                tidal = rt.TidalForcing(grid=self.grid, **input_args)
            try:
                tidal.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save tidal forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )
        else:
            with mem_log("TidalForcing()", enabled=self.verbose):
                tidal = rt.TidalForcing(grid=self.grid, **input_args)
            try:
                tidal.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save tidal forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )
            with mem_log("TidalForcing.save", enabled=self.verbose):
                paths = self._pio_finalize(tidal.save(self._pio_mangle(output_path)))

        # Append Resources directly to roms_marbl_blueprint_elements.forcing[subkey]
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                    resource
                )
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                resource
            )

        # Update settings_dict with the actually-generated tidal-constituent count.
        # bry_tides/pot_tides/ana_tides are NOT set here -- they're static booleans
        # owned by the resolver/model_settings (GENERATION_DERIVED_LEAF_KEYS only
        # covers ntides), so a child grid's bry_tides=False override isn't clobbered.
        self._settings_run_time.setdefault("tides", {})["ntides"] = (
            ntides if tidal is None else tidal.ntides
        )

        if "forcing" not in self._settings_run_time:
            self._settings_run_time["forcing"] = {}
        self._settings_run_time["forcing"]["tidal_forcing_path"] = (
            paths[0] if isinstance(paths, (list, tuple)) else paths
        )

    @register_input(name="forcing.river", order=60, label="Generating river forcing")
    def _generate_river_forcing(self, key: str = "forcing.river", **kwargs):
        """Generate river forcing input files."""
        # Extract subkey from "forcing.river" -> "river"
        subkey = key.split(".", 1)[1] if "." in key else key
        yaml_path = self._yaml_filename(key)
        output_path = self._forcing_filename(subkey)
        extra = dict(
            start_time=self.start_date,
            end_time=self.end_date,
            **self._mrd_extra(),
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        existing_paths = self._existing_output_paths(output_path)

        if existing_paths and yaml_path.exists():
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = list(existing_paths)
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=FutureWarning, module="xarray"
                )
                with xr.open_dataset(Path(paths[0]), decode_timedelta=False) as ds:
                    if "river_volume" not in ds.variables:
                        raise ValueError("river_volume is not in the dataset")
                    if "river_tracer" not in ds.variables:
                        raise ValueError("river_tracer is not in the dataset")
                    nriv = int(ds.sizes["nriver"])
            if "river_frc" not in self._settings_run_time:
                self._settings_run_time["river_frc"] = {}
            self._settings_run_time["river_frc"]["river_source"] = True
            self._settings_run_time["river_frc"]["analytical"] = False
            self._settings_run_time["river_frc"]["nriv"] = nriv
            self._settings_run_time["river_frc"]["rvol_vname"] = "river_volume"
            self._settings_run_time["river_frc"]["rvol_tname"] = "river_time"
            self._settings_run_time["river_frc"]["rtrc_vname"] = "river_tracer"
            self._settings_run_time["river_frc"]["rtrc_tname"] = "river_time"
            if "forcing" not in self._settings_run_time:
                self._settings_run_time["forcing"] = {}
            self._settings_run_time["forcing"]["river_path"] = (
                paths[0] if isinstance(paths, (list, tuple)) else paths
            )
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                    resource
                )
            return

        try:
            with mem_log("RiverForcing()", enabled=self.verbose):
                river = rt.RiverForcing(grid=self.grid, **input_args)
        except ValueError as e:
            warnings.warn(
                f"Skipping river forcing generation due to invalid river configuration: {e}",
                UserWarning,
                stacklevel=2,
            )
            if self.roms_marbl_blueprint_elements.forcing is not None:
                self.roms_marbl_blueprint_elements.forcing.river = None
            river = rt.RiverForcing.__new__(rt.RiverForcing)
            return river

        # river.ds is built during construction, so this reflects the same
        # "no rivers in domain" condition save() used to report via an empty
        # paths list -- checking it here lets the YAML sidecar be written
        # before save() runs instead of depending on save()'s return value.
        if river.ds.sizes["nriver"] == 0:
            if self.roms_marbl_blueprint_elements.forcing is not None:
                self.roms_marbl_blueprint_elements.forcing.river = None
            return river

        try:
            river.to_yaml(yaml_path)
        except Exception as e:
            warnings.warn(
                f"Failed to save river forcing YAML to {yaml_path}: {e}",
                UserWarning,
                stacklevel=2,
            )

        if existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = list(existing_paths)
            warnings.warn(
                f"River forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                "constructing RiverForcing once to write YAML (this may be slow).",
                UserWarning,
                stacklevel=2,
            )
        else:
            with mem_log("RiverForcing.save", enabled=self.verbose):
                paths = self._pio_finalize(river.save(self._pio_mangle(output_path)))
        # Append Resources directly to roms_marbl_blueprint_elements.forcing[subkey]
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                    resource
                )
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.roms_marbl_blueprint_elements.forcing, subkey).data.append(
                resource
            )

        # updates settings_dict
        if "river_frc" not in self._settings_run_time:
            self._settings_run_time["river_frc"] = {}

        self._settings_run_time["river_frc"]["river_source"] = True
        self._settings_run_time["river_frc"]["analytical"] = False
        self._settings_run_time["river_frc"]["nriv"] = river.ds.sizes["nriver"]

        # check to make sure river_volume and river_tracer are in the dataset
        if "river_volume" not in river.ds.variables:
            raise ValueError("river_volume is not in the dataset")
        if "river_tracer" not in river.ds.variables:
            raise ValueError("river_tracer is not in the dataset")

        self._settings_run_time["river_frc"]["rvol_vname"] = "river_volume"
        self._settings_run_time["river_frc"]["rvol_tname"] = "river_time"
        self._settings_run_time["river_frc"]["rtrc_vname"] = "river_tracer"
        self._settings_run_time["river_frc"]["rtrc_tname"] = "river_time"

        if "forcing" not in self._settings_run_time:
            self._settings_run_time["forcing"] = {}
        self._settings_run_time["forcing"]["river_path"] = (
            paths[0] if isinstance(paths, (list, tuple)) else paths
        )

    @register_input(name="cdr_forcing", order=80, label="Generating CDR forcing")
    def _generate_cdr_forcing(
        self, key: str = "cdr_forcing", cdr_kwargs=None, **kwargs
    ):
        """Generate CDR forcing input files."""
        cdr_kwargs = cdr_kwargs or {}
        if not cdr_kwargs:
            return

        yaml_path = self._yaml_filename(key)

        input_args = self._build_input_args(key, base_kwargs=cdr_kwargs)

        with mem_log("CDRForcing()", enabled=self.verbose):
            cdr = rt.CDRForcing(**input_args)
        output_path = self._forcing_filename(CDR_FORCING_NETCDF_STEM)

        cdr.to_yaml(yaml_path)

        if self._should_reuse_existing_output(output_path):
            print(f"   ↪ Reusing existing file: {output_path}")
            paths = [str(output_path)]
        else:
            with mem_log("CDRForcing.save", enabled=self.verbose):
                paths = self._pio_finalize(cdr.save(self._pio_mangle(output_path)))

        # Normalize output paths to absolute strings so downstream template
        # settings can reliably embed full file locations.
        normalized_paths: list[str] = []
        if isinstance(paths, (list, tuple)):
            raw_paths = list(paths)
        else:
            raw_paths = [paths]
        for raw_path in raw_paths:
            path_obj = Path(str(raw_path))
            if not path_obj.is_absolute():
                path_obj = output_path.parent / path_obj
            normalized_paths.append(str(path_obj.resolve()))
        paths = normalized_paths

        # Append Resources directly to roms_marbl_blueprint_elements.cdr_forcing
        for path in paths:
            resource = Resource(location=path, partitioned=False)
            self.roms_marbl_blueprint_elements.cdr_forcing.data.append(resource)

        if "cppdefs" not in self._settings_compile_time:
            self._settings_compile_time["cppdefs"] = {}
        self._settings_compile_time["cppdefs"]["cdr_forcing"] = True
        # always set this to cdr.nc per conventions; c-star will symlink to the real path in the blueprint
        if "cdr_frc" not in self._settings_run_time:
            self._settings_run_time["cdr_frc"] = {}
        self._settings_run_time["cdr_frc"]["cdr_file"] = "cdr.nc"
        self._settings_run_time["cdr_frc"]["cdr_source"] = True
        self._settings_run_time["cdr_frc"]["ncdr_parm"] = len(cdr.releases)
        self._settings_run_time["cdr_frc"]["forcing_parameterized"] = True
        self._settings_run_time["cdr_frc"]["cdr_volume"] = (
            cdr.releases.release_type == "volume"
        )
        # enable cdr output
        if "cdr_output" not in self._settings_run_time:
            self._settings_run_time["cdr_output"] = {}
        self._settings_run_time["cdr_output"]["do_cdr"] = True

    @register_input(
        name="forcing.corrections", order=90, label="Generating corrections forcing"
    )
    def _generate_corrections(self, key: str = "corrections", **kwargs):
        """Generate corrections forcing (not implemented)."""
        raise NotImplementedError(
            "Corrections forcing generation is not yet implemented."
        )

    def _partition_files(self, **kwargs):
        """
        Partition whole input files across tiles using roms_tools.partition_netcdf.

        Uses the paths stored in `roms_marbl_blueprint_elements` to build the list of whole-field files,
        and records the partitioned paths in the Resource objects.

        Always writes NETCDF4 (roms-tools' default) regardless of ``use_pio``: the
        executor never passes ``partition_files=True`` alongside PIO today, so the
        two don't co-occur. If that changes, tile outputs would need the same
        ``_pio_mangle``/``_pio_finalize`` nccopy treatment as the whole-field saves.
        """
        input_args = dict(
            np_eta=self.partitioning.n_procs_y,
            np_xi=self.partitioning.n_procs_x,
            output_dir=self.input_data_dir,
            include_coarse_dims=self.include_coarse_dims,
        )

        for function_key, _ in self.input_list:
            name = function_key
            dataset = None

            # Get the appropriate dataset from roms_marbl_blueprint_elements
            if name == "grid":
                dataset = self.roms_marbl_blueprint_elements.grid
            elif name == "initial_conditions":
                dataset = self.roms_marbl_blueprint_elements.initial_conditions
            elif name.startswith("forcing."):
                # Extract subkey from "forcing.surface" -> "surface"
                subkey = name.split(".", 1)[1]
                if self.roms_marbl_blueprint_elements.forcing is not None:
                    dataset = getattr(
                        self.roms_marbl_blueprint_elements.forcing, subkey, None
                    )
            elif name == "cdr_forcing":
                dataset = self.roms_marbl_blueprint_elements.cdr_forcing

            if dataset is None or not dataset.data:
                print(f"⚠️  Skipping {name} because it is empty")
                continue

            # Partition each Resource in the dataset
            # We need to collect new resources because partitioning creates multiple files
            new_resources = []
            for resource in dataset.data:
                if resource.location is None:
                    new_resources.append(resource)
                    continue
                partitioned_paths = rt.partition_netcdf(resource.location, **input_args)
                # partition_netcdf returns a list of paths (one per partition)
                # Create a Resource for each partitioned file
                if isinstance(partitioned_paths, list):
                    for partitioned_path in partitioned_paths:
                        resource_dict = resource.model_dump()
                        resource_dict["location"] = partitioned_path
                        resource_dict["partitioned"] = True
                        new_resources.append(Resource(**resource_dict))
                else:
                    # If it returns a single path (shouldn't happen, but handle it)
                    resource_dict = resource.model_dump()
                    resource_dict["location"] = partitioned_paths
                    resource_dict["partitioned"] = True
                    new_resources.append(Resource(**resource_dict))
            # Replace all resources in the dataset with the new partitioned resources
            dataset.data = new_resources
