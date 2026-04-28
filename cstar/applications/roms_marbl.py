import typing as t
from dataclasses import dataclass

from cstar.applications.utils import register_application
from cstar.entrypoint.worker.worker import SimulationRunner
from cstar.orchestration.models import (
    ApplicationDefinition,
    RomsMarblBlueprint,
    Transform,
)
from cstar.orchestration.transforms import OverrideTransform, RomsMarblTimeSplitter

_APP_NAME: t.Literal["roms_marbl"] = "roms_marbl"


class RomsMarblRunner(SimulationRunner):
    pass


@register_application
@dataclass
class RomsMarblApplication(ApplicationDefinition[RomsMarblBlueprint, RomsMarblRunner]):
    name: str = _APP_NAME
    blueprint: type[RomsMarblBlueprint] = RomsMarblBlueprint
    runner: type[RomsMarblRunner] = RomsMarblRunner
    applicable_transforms: tuple[type[Transform]] = (
        RomsMarblTimeSplitter,
        OverrideTransform,
    )
    resources_needed: t.Any = ()
    long_name: str = "ROMS-MARBL simulation runner"
