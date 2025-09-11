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
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)

_Tin = t.TypeVar("_Tin", bound=BaseModel)
_Tout_co = t.TypeVar("_Tout_co", covariant=True)

# TODO: consider doing a straight up JSON conversion of the model to a format
# that would deserialize into the target "native" object instead of this?


class ModelAdapter(t.Generic[_Tin, _Tout_co], t.Protocol):
    """Contract exposing a mechanism to adapt a source model to a target type."""

    model: _Tin

    def __init__(self, model: _Tin) -> None:
        self.model = model

    def adapt(self) -> _Tout_co:
        """Adapt the source model to the target output type.

        Returns
        -------
        _Tout
            The instance converted from the source model
        """
        ...


class DiscretizationAdapter(ModelAdapter[models.Blueprint, ROMSDiscretization]):
    """Create a ROMSDiscretization from a blueprint model."""

    # def __init__(self, model: models.Blueprint) -> None:
    #     self.model = model

    @t.override
    def adapt(self) -> ROMSDiscretization:
        return ROMSDiscretization(
            time_step=1,
            n_procs_x=self.model.partitioning.n_procs_x,
            n_procs_y=self.model.partitioning.n_procs_y,
        )


class AddtlCodeAdapter(ModelAdapter[models.Blueprint, AdditionalCode]):
    """Create a AdditionalCode from a blueprint model."""

    def __init__(self, model: models.Blueprint, key: str) -> None:
        super().__init__(model)
        self.key = key

    @t.override
    def adapt(self) -> AdditionalCode:
        code_attr: models.CodeRepository = getattr(self.model.code, self.key)

        return AdditionalCode(
            location=(self.model.runtime_params.output_dir / "runtime").as_posix(),
            subdir=(str(code_attr.filter.directory) if code_attr.filter else ""),
            checkout_target=code_attr.commit or code_attr.branch,
            files=(code_attr.filter.files if code_attr.filter else []),
        )


class CodebaseAdapter(ModelAdapter[models.Blueprint, ROMSExternalCodeBase]):
    """Create a ROMSExternalCodeBase from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSExternalCodeBase:
        return ROMSExternalCodeBase(
            source_repo=str(self.model.code.roms.url),
            checkout_target=self.model.code.roms.commit or self.model.code.roms.branch,
        )


class MARBLAdapter(ModelAdapter[models.Blueprint, MARBLExternalCodeBase]):
    """Create a MARBLExternalCodeBase from a blueprint model."""

    @t.override
    def adapt(self) -> MARBLExternalCodeBase:
        if self.model.code.marbl is None:
            msg = "MARBL codebase not found"
            raise RuntimeError(msg)

        return MARBLExternalCodeBase(
            source_repo=str(self.model.code.marbl.url),
            checkout_target=self.model.code.marbl.commit
            or self.model.code.marbl.branch,
        )


class GridAdapter(ModelAdapter[models.Blueprint, ROMSModelGrid]):
    """Create a ROMSModelGrid from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSModelGrid:
        return ROMSModelGrid(
            location=str(self.model.runtime_params.output_dir / "model_grid"),
            # WARNING - path is not valid...
            file_hash="",
            start_date=self.model.runtime_params.start_date,
            end_date=self.model.runtime_params.start_date,
        )


class ConditionAdapter(ModelAdapter[models.Blueprint, ROMSInitialConditions]):
    """Create a ROMSInitialCondition from a blueprint model."""

    @t.override
    def adapt(self) -> ROMSInitialConditions:
        return ROMSInitialConditions(
            location=str(
                self.model.runtime_params.output_dir / "initial_conditions",
            ),
            # WARNING - path is not valid...
            start_date=self.model.runtime_params.start_date,
            end_date=self.model.runtime_params.start_date,
        )


class TidalForcingAdapter(ModelAdapter[models.Blueprint, ROMSTidalForcing]):
    """Create a ROMSTidalForcing from a blueprint model."""

    # def __init__(self, model: models.Blueprint, key: str) -> None:
    #     super().__init__(model)
    #     self.key = key

    @t.override
    def adapt(self) -> ROMSTidalForcing:
        # code_attr: models.Forcing = getattr(self.model.code, self.key)

        return ROMSTidalForcing(
            location=str(
                self.model.runtime_params.output_dir / "forcing/tidal",
            ),
            # WARNING - path is not valid...
            start_date=self.model.runtime_params.start_date,
            end_date=self.model.runtime_params.start_date,
        )


class RiverForcingAdapter(ModelAdapter[models.Blueprint, ROMSRiverForcing]):
    """Create a ROMSRiverForcing from a blueprint model."""

    # def __init__(self, model: models.Blueprint, key: str) -> None:
    #     super().__init__(model)
    #     self.key = key

    @t.override
    def adapt(self) -> ROMSRiverForcing:
        # code_attr: models.Forcing = getattr(self.model.code, self.key)

        return ROMSRiverForcing(
            location=str(
                self.model.runtime_params.output_dir / "forcing/river",
            ),
            # WARNING - path is not valid...
            start_date=self.model.runtime_params.start_date,
            end_date=self.model.runtime_params.start_date,
        )


class BoundaryForcingAdapter(ModelAdapter[models.Blueprint, ROMSBoundaryForcing]):
    """Create a ROMSBoundaryForcing from a blueprint model."""

    # def __init__(self, model: models.Blueprint, key: str) -> None:
    #     super().__init__(model)
    #     self.key = key

    @t.override
    def adapt(self) -> ROMSBoundaryForcing:
        # code_attr: models.Forcing = getattr(self.model.code, self.key)

        return ROMSBoundaryForcing(
            location=str(
                self.model.runtime_params.output_dir / "forcing/boundary",
            ),
            # WARNING - path is not valid...
            start_date=self.model.runtime_params.start_date,
            end_date=self.model.runtime_params.start_date,
        )


class SurfaceForcingAdapter(ModelAdapter[models.Blueprint, ROMSSurfaceForcing]):
    """Create a ROMSSurfaceForcing from a blueprint model."""

    # def __init__(self, model: models.Blueprint, key: str) -> None:
    #     super().__init__(model)
    #     self.key = key

    @t.override
    def adapt(self) -> ROMSSurfaceForcing:
        # code_attr: models.Forcing = getattr(self.model.code, self.key)

        return ROMSSurfaceForcing(
            location=str(
                self.model.runtime_params.output_dir / "forcing/surface",
            ),
            # WARNING - path is not valid...
            start_date=self.model.runtime_params.start_date,
            end_date=self.model.runtime_params.start_date,
        )


class BlueprintAdapter(ModelAdapter[models.Blueprint, ROMSSimulation]):
    """Create a ROMSSimulation from a blueprint model."""

    def __init__(self, model: models.Blueprint) -> None:
        self.model = model

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
            initial_conditions=ConditionAdapter(self.model).adapt(),
            tidal_forcing=TidalForcingAdapter(self.model).adapt(),
            river_forcing=RiverForcingAdapter(self.model).adapt(),
            boundary_forcing=[
                # WARNING - current schema does not take into account a forcing LIST
                BoundaryForcingAdapter(self.model).adapt(),
            ],
            surface_forcing=[
                # WARNING - current schema does not take into account a forcing LIST
                SurfaceForcingAdapter(self.model).adapt(),
            ],
        )
