import typing as t
import warnings
from datetime import datetime

from pydantic import Field, NonNegativeInt, PositiveInt, model_validator

from cstar.applications.ucla_roms.models import (
    CodeRepository,
    ForcingConfiguration,
    ParameterSet,
    RuntimeParameterSet,
)
from cstar.orchestration.models import (
    Blueprint,
    ComputeNeeds,
    ConfiguredBaseModel,
    Dataset,
)
from cstar.roms.gpu_layout import GpuRankLayout
from cstar.roms.namelist import _parse_gpu_release, gpu_namelist_schema_for_ref
from cstar.roms.precheck import check_gpu_features

if t.TYPE_CHECKING:
    from cstar.system.toolchain import ToolchainName

APP_NAME: t.Literal["ucla_roms_gpu"] = "ucla_roms_gpu"

TOOLCHAIN: t.Final["ToolchainName"] = "gpu"
"""The system toolchain profile the application builds and runs with: the
application declares it, and its simulation loads it before building or
launching the model."""


class GpuCodeRepositories(ConfiguredBaseModel):
    """The repositories a GPU run builds and configures the model from."""

    roms: CodeRepository
    """The gpu-ucla-roms repository. It is cloned with its MARBL submodule, which
    the GPU build compiles into the model."""

    run_time: CodeRepository
    """The namelist and MARBL settings (`marbl_in`) the model reads at run time.

    The GPU model reads the namelist vocabulary of one ucla-roms release (0.11.0
    for every gpu-ucla-roms release so far), so the namelist file may only use
    that release's groups and keys."""


class TilingParameterSet(ParameterSet):
    """How the domain is split over GPUs, and how the MPI ranks sit on the nodes.

    Each of the `n_procs_x * n_procs_y` tiles runs on its own GPU. The GPU model
    makes the last `writers_per_node` ranks on every node writer ranks, which
    write the output files, so a run takes `tiles + writers_per_node * nodes` MPI
    tasks, the same number on every node, and the tiles must divide evenly over
    the nodes.

    Namelist overrides of the processor grid (`np_xi`/`np_eta` in
    `param_settings`) must agree with `n_procs_x`/`n_procs_y`.
    """

    n_procs_x: PositiveInt
    """Number of tiles along the x-axis."""

    n_procs_y: PositiveInt
    """Number of tiles along the y-axis."""

    nodes: PositiveInt = Field(default=1)
    """Number of nodes the run spans; each holds `n_procs_x * n_procs_y / nodes`
    tiles, so it needs at least that many GPUs."""

    writers_per_node: NonNegativeInt = Field(default=1)
    """Writer ranks on each node, which write the output files while the GPUs
    compute. One per node suits most runs; a large domain writing big history
    records may want more (C-Star warns when it judges the count too small)."""

    @property
    def layout(self) -> GpuRankLayout:
        """The MPI rank layout of the run."""
        return GpuRankLayout(
            tiles=self.n_procs_x * self.n_procs_y,
            nodes=self.nodes,
            writers_per_node=self.writers_per_node,
        )

    @model_validator(mode="after")
    def _validate_tiling(self) -> "TilingParameterSet":
        """Reject a layout the GPU model would stop on.

        Named distinctly from `ParameterSet._model_validator` so the inherited
        locked/hash check is not shadowed and both validators run.
        """
        _ = self.layout
        return self


class UclaRomsGpuBlueprint(Blueprint):
    """Blueprint schema for running a UCLA-ROMS simulation on GPUs with gpu-ucla-roms."""

    schema_version: str = Field(default="1.0.0")
    """The blueprint schema version."""

    application: str = Field(default=APP_NAME)
    """The process type to be executed by the blueprint."""

    valid_start_date: datetime
    """Beginning of the time range for the available data."""

    valid_end_date: datetime
    """End of the time range for the available data."""

    code: GpuCodeRepositories
    """Code repositories used to build, configure, and execute the simulation."""

    initial_conditions: Dataset = Field(min_length=1, max_length=1)
    """File containing the starting conditions of the simulation."""

    grid: Dataset = Field(min_length=1, max_length=1)
    """File defining the grid geometry."""

    forcing: ForcingConfiguration
    """Forcing configuration."""

    tiling: TilingParameterSet
    """How the domain is split over GPUs and the MPI ranks over nodes."""

    runtime_params: RuntimeParameterSet
    """User-defined runtime parameters."""

    cdr_forcing: Dataset | None = Field(default=None)
    """Location of CDR input file for this run. Optional."""

    nesting_info: Dataset | None = Field(default=None)
    """Location of nesting info input file for this run. Optional."""

    namelist_overrides: dict[str, dict[str, t.Any]] = Field(default_factory=dict)
    """Mapping of ROMS namelist group to key/value overrides.

    These overrides are layered onto the runtime namelist LAST, after C-Star's
    own derived settings, so they take precedence over anything C-Star would
    otherwise compute; list values replace the existing list wholesale.

    Group and key names are checked against the namelist vocabulary the pinned
    gpu-ucla-roms release reads: unknown names are an error when `code.roms`
    pins a release, and a warning otherwise. Switches for features the GPU
    model does not have are refused. See :class:`TilingParameterSet` for the
    processor grid.
    """

    @model_validator(mode="after")
    def _model_validator(self) -> "UclaRomsGpuBlueprint":
        """Perform validation on the model after field-level validation is complete."""
        if self.valid_end_date <= self.valid_start_date:
            msg = "valid_start_date must precede valid_end_date"
            raise ValueError(msg)

        if self.runtime_params.end_date > self.valid_end_date:
            msg = "end_date is outside the valid range"
            raise ValueError(msg)

        if self.runtime_params.start_date < self.valid_start_date:
            msg = "start_date is outside the valid range"
            raise ValueError(msg)

        if partitioned := self._partitioned_datasets():
            msg = (
                "the GPU model reads whole input files; these datasets are "
                f"partitioned: {', '.join(partitioned)}"
            )
            raise ValueError(msg)

        self._validate_namelist_overrides()

        return self

    def _partitioned_datasets(self) -> list[str]:
        """Name every input dataset that lists a partitioned file."""
        datasets: dict[str, Dataset | None] = {
            "grid": self.grid,
            "initial_conditions": self.initial_conditions,
            "cdr_forcing": self.cdr_forcing,
            "nesting_info": self.nesting_info,
            **{
                f"forcing.{name}": getattr(self.forcing, name)
                for name in ForcingConfiguration.model_fields
            },
        }
        return [
            name
            for name, dataset in datasets.items()
            if dataset is not None and any(r.partitioned for r in dataset.data)
        ]

    def _validate_namelist_overrides(self) -> None:
        """Check `namelist_overrides` names, tiling consistency and GPU features early.

        Unknown group/key names are an error when `code.roms` pins a
        gpu-ucla-roms release and a warning otherwise. `np_xi`/`np_eta`
        conflicts with `tiling` and switches for features the GPU model does
        not have (Rule E in :mod:`cstar.roms.precheck`) are always an error.
        Values are re-validated when the overrides are merged onto the real
        namelist before launch.
        """
        if not self.namelist_overrides:
            return

        param_overrides = self.namelist_overrides.get("param_settings", {})
        violations = [
            f"namelist override {key!r} ({value!r}) conflicts with the "
            f"blueprint tiling ({expected!r})"
            for key, expected in (
                ("np_xi", self.tiling.n_procs_x),
                ("np_eta", self.tiling.n_procs_y),
            )
            if (value := param_overrides.get(key)) is not None and value != expected
        ]

        ref = self.code.roms.checkout_target
        if _parse_gpu_release(ref) is not None:
            schema = gpu_namelist_schema_for_ref(ref)
            violations += schema.unknown_override_keys(self.namelist_overrides)
        else:
            with warnings.catch_warnings():
                # suppress the unpinned-ref fallback warning; unknown names are
                # reported below only if any are found
                warnings.simplefilter("ignore", UserWarning)
                schema = gpu_namelist_schema_for_ref(ref)
            if unknown := schema.unknown_override_keys(self.namelist_overrides):
                warnings.warn(
                    f"namelist_overrides contain names not in {schema.__name__} "
                    f"(assumed for unpinned gpu-ucla-roms ref {ref!r}): "
                    + "; ".join(unknown),
                    UserWarning,
                    stacklevel=2,
                )

        if violations:
            raise ValueError("invalid namelist_overrides: " + "; ".join(violations))

        check_gpu_features(self.namelist_overrides)

    @property
    def compute_needs(self) -> ComputeNeeds:
        """The MPI tasks, GPUs and nodes the run needs (from the tiling)."""
        layout = self.tiling.layout
        return ComputeNeeds(
            tasks=layout.tasks, gpu_tasks=layout.tiles, nodes=layout.nodes
        )

    @property
    def processor_grid(self) -> tuple[int | None, int | None]:
        """The domain decomposition (tiles along x, y)."""
        return (self.tiling.n_procs_x, self.tiling.n_procs_y)
