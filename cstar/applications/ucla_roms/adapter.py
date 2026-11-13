import typing as t

from cstar.applications.ucla_roms.models import (
    CodeRepository,
    ForcingConfiguration,
    UclaRomsBlueprint,
)
from cstar.base.adapter import ModelAdapter
from cstar.base.additional_code import AdditionalCode
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.orchestration import models
from cstar.pio.external_codebase import PIOExternalCodeBase
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSCarbonateSensitivity,
    ROMSCdrForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSNestingInfo,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)


class RomsInputsBlueprint(t.Protocol):
    """The parts of a ROMS-family blueprint (ucla_roms, ucla_roms_gpu) the input
    adapters read.
    """

    @property
    def code(self) -> object:
        """The code repositories, read by name (see `AddtlCodeAdapter`)."""
        ...

    @property
    def grid(self) -> models.Dataset:
        """File defining the grid geometry."""
        ...

    @property
    def initial_conditions(self) -> models.Dataset:
        """File containing the starting conditions of the simulation."""
        ...

    @property
    def forcing(self) -> ForcingConfiguration:
        """Forcing configuration."""
        ...

    @property
    def cdr_forcing(self) -> models.Dataset | None:
        """Location of the CDR input file, if any."""
        ...

    @property
    def nesting_info(self) -> models.Dataset | None:
        """Location of the nesting info input file, if any."""
        ...

    @property
    def processor_grid(self) -> tuple[int | None, int | None]:
        """The domain decomposition (processes along x, y); partitioned input
        datasets were split for it.
        """
        ...


class _SourcePartitioning(t.TypedDict):
    """The keyword arguments naming the processor grid an input dataset was split for."""

    source_np_xi: int | None
    source_np_eta: int | None


def _source_np(model: RomsInputsBlueprint, partitioned: bool) -> _SourcePartitioning:
    """Return the `source_np_xi`/`source_np_eta` keyword arguments of an input dataset.

    A dataset marked `partitioned` was split for the model's processor grid;
    any other has no source partitioning.
    """
    np_xi, np_eta = model.processor_grid if partitioned else (None, None)
    return {"source_np_xi": np_xi, "source_np_eta": np_eta}


class DiscretizationAdapter(ModelAdapter[UclaRomsBlueprint, ROMSDiscretization]):
    """Create a ROMSDiscretization from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSDiscretization:
        return ROMSDiscretization(
            n_procs_x=self.model.partitioning.n_procs_x,
            n_procs_y=self.model.partitioning.n_procs_y,
            n_cores=self.model.partitioning.n_cores,
        )


class AddtlCodeAdapter(ModelAdapter[RomsInputsBlueprint, AdditionalCode]):
    """Create a AdditionalCode from a blueprint model."""

    def __init__(self, model: RomsInputsBlueprint, key: str) -> None:
        super().__init__(model)
        self.key = key

    @t.override
    def adapt(self) -> AdditionalCode:
        code_attr: CodeRepository = getattr(self.model.code, self.key)

        return AdditionalCode(
            location=str(code_attr.location),
            subdir=(str(code_attr.filter.directory) if code_attr.filter else ""),
            checkout_target=code_attr.checkout_target,
            files=(code_attr.filter.files if code_attr.filter else []),
        )


class CodebaseAdapter(ModelAdapter[UclaRomsBlueprint, ROMSExternalCodeBase]):
    """Create a ROMSExternalCodeBase from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSExternalCodeBase:
        return ROMSExternalCodeBase(
            source_repo=str(self.model.code.roms.location),
            checkout_target=self.model.code.roms.checkout_target,
        )


class MARBLAdapter(ModelAdapter[UclaRomsBlueprint, MARBLExternalCodeBase]):
    """Create a MARBLExternalCodeBase from a blueprint model."""

    @t.override
    def adapt(self) -> MARBLExternalCodeBase:
        if self.model.code.marbl is None:
            msg = "MARBL codebase not found"
            raise RuntimeError(msg)

        return MARBLExternalCodeBase(
            source_repo=str(self.model.code.marbl.location),
            checkout_target=self.model.code.marbl.checkout_target
            or self.model.code.marbl.branch,
        )


class PIOAdapter(ModelAdapter[UclaRomsBlueprint, PIOExternalCodeBase]):
    """Create a PIOExternalCodeBase from a blueprint model.

    Unlike MARBL, a missing `code.pio` section is not an error: the default
    ParallelIO source repository and checkout target are used instead.
    """

    @t.override
    def adapt(self) -> PIOExternalCodeBase:
        if self.model.code.pio is None:
            return PIOExternalCodeBase()

        return PIOExternalCodeBase(
            source_repo=str(self.model.code.pio.location),
            checkout_target=self.model.code.pio.checkout_target
            or self.model.code.pio.branch,
        )


class GridAdapter(ModelAdapter[RomsInputsBlueprint, ROMSModelGrid]):
    """Create a ROMSModelGrid from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSModelGrid:
        return ROMSModelGrid(
            location=str(self.model.grid.data[0].location),
            file_hash=(
                self.model.grid.data[0].hash
                if isinstance(self.model.grid.data[0], models.VersionedResource)
                else None
            ),
            start_date=None,
            end_date=None,
            **_source_np(self.model, self.model.grid.data[0].partitioned),
        )


class InitialConditionAdapter(ModelAdapter[RomsInputsBlueprint, ROMSInitialConditions]):
    """Create a ROMSInitialCondition from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSInitialConditions:
        return ROMSInitialConditions(
            location=str(self.model.initial_conditions.data[0].location),
            file_hash=(
                self.model.initial_conditions.data[0].hash
                if isinstance(
                    self.model.initial_conditions.data[0], models.VersionedResource
                )
                else None
            ),
            start_date=None,
            end_date=None,
            **_source_np(self.model, self.model.initial_conditions.data[0].partitioned),
        )


class TidalForcingAdapter(ModelAdapter[RomsInputsBlueprint, ROMSTidalForcing]):
    """Create a ROMSTidalForcing from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSTidalForcing | None:
        if self.model.forcing.tidal is None:
            return None
        return ROMSTidalForcing(
            location=str(self.model.forcing.tidal.data[0].location),
            file_hash=(
                self.model.forcing.tidal.data[0].hash
                if isinstance(
                    self.model.forcing.tidal.data[0], models.VersionedResource
                )
                else None
            ),
            start_date=None,
            end_date=None,
            **_source_np(self.model, self.model.forcing.tidal.data[0].partitioned),
        )


class RiverForcingAdapter(ModelAdapter[RomsInputsBlueprint, ROMSRiverForcing]):
    """Create a ROMSRiverForcing from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSRiverForcing | None:
        if self.model.forcing.river is None:
            return None

        return ROMSRiverForcing(
            location=str(self.model.forcing.river.data[0].location),
            file_hash=(
                self.model.forcing.river.data[0].hash
                if isinstance(
                    self.model.forcing.river.data[0], models.VersionedResource
                )
                else None
            ),
            start_date=None,
            end_date=None,
            **_source_np(self.model, self.model.forcing.river.data[0].partitioned),
        )


class BoundaryForcingAdapter(
    ModelAdapter[RomsInputsBlueprint, list[ROMSBoundaryForcing]]
):
    """Create a ROMSBoundaryForcing from a blueprint model."""

    @t.override
    def adapt(self) -> list[ROMSBoundaryForcing]:
        return [
            ROMSBoundaryForcing(
                location=str(f.location),
                file_hash=(f.hash if isinstance(f, models.VersionedResource) else None),
                start_date=None,
                end_date=None,
                **_source_np(self.model, f.partitioned),
            )
            for f in self.model.forcing.boundary.data
        ]


class SurfaceForcingAdapter(
    ModelAdapter[RomsInputsBlueprint, list[ROMSSurfaceForcing]]
):
    """Create a ROMSSurfaceForcing from a blueprint model."""

    @t.override
    def adapt(self) -> list[ROMSSurfaceForcing]:
        return [
            ROMSSurfaceForcing(
                location=str(f.location),
                file_hash=(f.hash if isinstance(f, models.VersionedResource) else None),
                start_date=None,
                end_date=None,
                **_source_np(self.model, f.partitioned),
            )
            for f in self.model.forcing.surface.data
        ]


class CdrForcingAdapter(ModelAdapter[RomsInputsBlueprint, ROMSCdrForcing]):
    """Create a ROMSCdrForcing from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSCdrForcing | None:
        if self.model.cdr_forcing is None:
            return None

        return ROMSCdrForcing(
            location=str(self.model.cdr_forcing.data[0].location),
            file_hash=(
                self.model.cdr_forcing.data[0].hash
                if isinstance(self.model.cdr_forcing.data[0], models.VersionedResource)
                else None
            ),
            start_date=None,
            end_date=None,
        )


class NestingInfoAdapter(ModelAdapter[RomsInputsBlueprint, ROMSNestingInfo]):
    """Create a ROMSNestingInfo from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSNestingInfo | None:
        if self.model.nesting_info is None:
            return None

        return ROMSNestingInfo(
            location=str(self.model.nesting_info.data[0].location),
            file_hash=(
                self.model.nesting_info.data[0].hash
                if isinstance(self.model.nesting_info.data[0], models.VersionedResource)
                else None
            ),
            start_date=None,
            end_date=None,
        )


class ForcingCorrectionAdapter(
    ModelAdapter[RomsInputsBlueprint, list[ROMSForcingCorrections]]
):
    """Create a ROMSForcingCorrections from a blueprint model."""

    @t.override
    def adapt(self) -> list[ROMSForcingCorrections] | None:
        if self.model.forcing.corrections is None:
            return None
        return [
            ROMSForcingCorrections(
                location=str(f.location),
                file_hash=(f.hash if isinstance(f, models.VersionedResource) else None),
                start_date=None,
                end_date=None,
                **_source_np(self.model, f.partitioned),
            )
            for f in self.model.forcing.corrections.data
        ]


class CarbonateSensitivityAdapter(
    ModelAdapter[RomsInputsBlueprint, list[ROMSCarbonateSensitivity]]
):
    """Create a ROMSCarbonateSensitivity from a blueprint model."""

    @t.override
    def adapt(self) -> list[ROMSCarbonateSensitivity] | None:
        if self.model.forcing.carbonate_sensitivity is None:
            return None
        return [
            ROMSCarbonateSensitivity(
                location=str(f.location),
                file_hash=(f.hash if isinstance(f, models.VersionedResource) else None),
                start_date=None,
                end_date=None,
                **_source_np(self.model, f.partitioned),
            )
            for f in self.model.forcing.carbonate_sensitivity.data
        ]
