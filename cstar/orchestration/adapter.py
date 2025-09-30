import typing as t

from pydantic import BaseModel

from cstar.base.additional_code import AdditionalCode
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.orchestration import models
from cstar.roms import ROMSSimulation
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSCdrForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)

_Tin = t.TypeVar("_Tin", bound=BaseModel)
_Tout_co = t.TypeVar("_Tout_co", covariant=True)


class ModelAdapter(t.Generic[_Tin, _Tout_co], t.Protocol):
    """Contract exposing a mechanism to adapt a source model to a target type."""

    model: _Tin

    def __init__(self, model: _Tin) -> None:
        self.model = model

    def adapt(self) -> _Tout_co | None:
        """Adapt the source model to the target output type.

        Returns
        -------
        _Tout
            The instance converted from the source model
        """
        ...


class DiscretizationAdapter(
    ModelAdapter[models.RomsMarblBlueprint, ROMSDiscretization]
):
    """Create a ROMSDiscretization from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSDiscretization:
        return ROMSDiscretization(
            time_step=self.model.model_params.time_step,
            n_procs_x=self.model.partitioning.n_procs_x,
            n_procs_y=self.model.partitioning.n_procs_y,
        )


class AddtlCodeAdapter(ModelAdapter[models.RomsMarblBlueprint, AdditionalCode]):
    """Create a AdditionalCode from a blueprint model."""

    def __init__(self, model: models.RomsMarblBlueprint, key: str) -> None:
        super().__init__(model)
        self.key = key

    @t.override
    def adapt(self) -> AdditionalCode:
        code_attr: models.CodeRepository = getattr(self.model.code, self.key)

        return AdditionalCode(
            location=str(code_attr.location),
            subdir=(str(code_attr.filter.directory) if code_attr.filter else ""),
            checkout_target=code_attr.checkout_target,
            files=(code_attr.filter.files if code_attr.filter else []),
        )


class CodebaseAdapter(ModelAdapter[models.RomsMarblBlueprint, ROMSExternalCodeBase]):
    """Create a ROMSExternalCodeBase from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSExternalCodeBase:
        return ROMSExternalCodeBase(
            source_repo=str(self.model.code.roms.location),
            checkout_target=self.model.code.roms.checkout_target,
        )


class MARBLAdapter(ModelAdapter[models.RomsMarblBlueprint, MARBLExternalCodeBase]):
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


class GridAdapter(ModelAdapter[models.RomsMarblBlueprint, ROMSModelGrid]):
    """Create a ROMSModelGrid from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSModelGrid:
        return ROMSModelGrid(
            location=str(self.model.grid.data.location),
            file_hash=(
                self.model.grid.data.hash
                if isinstance(self.model.grid.data, models.VersionedResource)
                else None
            ),
            start_date=self.model.valid_start_date,
            end_date=self.model.valid_end_date,
            source_np_xi=self.model.partitioning.n_procs_x
            if self.model.grid.data.partitioned
            else None,
            source_np_eta=self.model.partitioning.n_procs_y
            if self.model.grid.data.partitioned
            else None,
        )


class InitialConditionAdapter(
    ModelAdapter[models.RomsMarblBlueprint, ROMSInitialConditions]
):
    """Create a ROMSInitialCondition from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSInitialConditions:
        return ROMSInitialConditions(
            location=str(self.model.initial_conditions.data.location),
            file_hash=(
                self.model.initial_conditions.data.hash
                if isinstance(
                    self.model.initial_conditions.data, models.VersionedResource
                )
                else None
            ),
            start_date=self.model.valid_start_date,
            end_date=self.model.valid_end_date,
            source_np_xi=self.model.partitioning.n_procs_x
            if self.model.initial_conditions.data.partitioned
            else None,
            source_np_eta=self.model.partitioning.n_procs_y
            if self.model.initial_conditions.data.partitioned
            else None,
        )


class TidalForcingAdapter(ModelAdapter[models.RomsMarblBlueprint, ROMSTidalForcing]):
    """Create a ROMSTidalForcing from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSTidalForcing | None:
        if self.model.forcing.tidal is None:
            return None
        return ROMSTidalForcing(
            location=str(self.model.forcing.tidal.data.location),
            file_hash=(
                self.model.forcing.tidal.data.hash
                if isinstance(self.model.forcing.tidal.data, models.VersionedResource)
                else None
            ),
            start_date=self.model.valid_start_date,
            end_date=self.model.valid_end_date,
            source_np_xi=self.model.partitioning.n_procs_x
            if self.model.forcing.tidal.data.partitioned
            else None,
            source_np_eta=self.model.partitioning.n_procs_y
            if self.model.forcing.tidal.data.partitioned
            else None,
        )


class RiverForcingAdapter(ModelAdapter[models.RomsMarblBlueprint, ROMSRiverForcing]):
    """Create a ROMSRiverForcing from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSRiverForcing | None:
        if self.model.forcing.river is None:
            return None

        return ROMSRiverForcing(
            location=str(self.model.forcing.river.data.location),
            file_hash=(
                self.model.forcing.river.data.hash
                if isinstance(self.model.forcing.river.data, models.VersionedResource)
                else None
            ),
            start_date=self.model.valid_start_date,
            end_date=self.model.valid_end_date,
            source_np_xi=self.model.partitioning.n_procs_x
            if self.model.forcing.river.data.partitioned
            else None,
            source_np_eta=self.model.partitioning.n_procs_y
            if self.model.forcing.river.data.partitioned
            else None,
        )


class BoundaryForcingAdapter(
    ModelAdapter[models.RomsMarblBlueprint, list[ROMSBoundaryForcing]]
):
    """Create a ROMSBoundaryForcing from a blueprint model."""

    @t.override
    def adapt(self) -> list[ROMSBoundaryForcing]:
        return [
            ROMSBoundaryForcing(
                location=str(f.location),
                file_hash=(f.hash if isinstance(f, models.VersionedResource) else None),
                start_date=self.model.valid_start_date,
                end_date=self.model.valid_end_date,
                source_np_xi=self.model.partitioning.n_procs_x
                if f.partitioned
                else None,
                source_np_eta=self.model.partitioning.n_procs_y
                if f.partitioned
                else None,
            )
            for f in self.model.forcing.boundary.data
        ]


class SurfaceForcingAdapter(
    ModelAdapter[models.RomsMarblBlueprint, list[ROMSSurfaceForcing]]
):
    """Create a ROMSSurfaceForcing from a blueprint model."""

    @t.override
    def adapt(self) -> list[ROMSSurfaceForcing]:
        return [
            ROMSSurfaceForcing(
                location=str(f.location),
                file_hash=(f.hash if isinstance(f, models.VersionedResource) else None),
                start_date=self.model.valid_start_date,
                end_date=self.model.valid_end_date,
                source_np_xi=self.model.partitioning.n_procs_x
                if f.partitioned
                else None,
                source_np_eta=self.model.partitioning.n_procs_y
                if f.partitioned
                else None,
            )
            for f in self.model.forcing.surface.data
        ]


class CdrForcingAdapter(ModelAdapter[models.RomsMarblBlueprint, ROMSCdrForcing]):
    @t.override
    def adapt(self) -> ROMSCdrForcing | None:
        if self.model.cdr_forcing is None:
            return None

        return ROMSCdrForcing(
            location=str(self.model.cdr_forcing.data.location),
            file_hash=self.model.cdr_forcing.data.hash
            if isinstance(self.model.cdr_forcing.data, models.VersionedResource)
            else None,
            start_date=self.model.valid_start_date,
            end_date=self.model.valid_end_date,
        )


class ForcingCorrectionAdapter(
    ModelAdapter[models.RomsMarblBlueprint, list[ROMSForcingCorrections]]
):
    @t.override
    def adapt(self) -> list[ROMSForcingCorrections] | None:
        if self.model.forcing.corrections is None:
            return None
        return [
            ROMSForcingCorrections(
                location=str(f.location),
                file_hash=(f.hash if isinstance(f, models.VersionedResource) else None),
                start_date=self.model.valid_start_date,
                end_date=self.model.valid_end_date,
                source_np_xi=self.model.partitioning.n_procs_x
                if f.partitioned
                else None,
                source_np_eta=self.model.partitioning.n_procs_y
                if f.partitioned
                else None,
            )
            for f in self.model.forcing.corrections.data
        ]


class BlueprintAdapter(ModelAdapter[models.RomsMarblBlueprint, ROMSSimulation]):
    """Create a ROMSSimulation from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSSimulation:
        return ROMSSimulation(
            name=self.model.name,
            directory=self.model.runtime_params.output_dir,
            discretization=DiscretizationAdapter(self.model).adapt(),
            runtime_code=AddtlCodeAdapter(self.model, "run_time").adapt(),
            compile_time_code=AddtlCodeAdapter(self.model, "compile_time").adapt(),
            codebase=CodebaseAdapter(self.model).adapt(),
            start_date=self.model.runtime_params.start_date,
            end_date=self.model.runtime_params.end_date,
            valid_start_date=self.model.valid_start_date,
            valid_end_date=self.model.valid_end_date,
            marbl_codebase=(
                MARBLAdapter(self.model).adapt() if self.model.code.marbl else None
            ),
            model_grid=GridAdapter(self.model).adapt(),
            initial_conditions=InitialConditionAdapter(self.model).adapt(),
            tidal_forcing=TidalForcingAdapter(self.model).adapt(),
            river_forcing=RiverForcingAdapter(self.model).adapt(),
            forcing_corrections=ForcingCorrectionAdapter(self.model).adapt(),
            boundary_forcing=BoundaryForcingAdapter(self.model).adapt(),
            surface_forcing=SurfaceForcingAdapter(self.model).adapt(),
            cdr_forcing=CdrForcingAdapter(self.model).adapt(),
        )
