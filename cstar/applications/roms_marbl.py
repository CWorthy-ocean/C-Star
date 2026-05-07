import typing as t

from cstar.applications.utils import register_application
from cstar.entrypoint.worker.worker import SimulationRunner
from cstar.orchestration.models import (
    ApplicationDefinition,
    RomsMarblBlueprint,
)
from cstar.orchestration.transforms import OverrideTransform, RomsMarblTimeSplitter

_APP_NAME: t.Literal["roms_marbl"] = "roms_marbl"
_APP_NAME_LONG: t.Literal["ROMS-MARBL simulation runner"] = (
    "ROMS-MARBL simulation runner"
)


class RomsMarblRunner(SimulationRunner):
    pass


@register_application
class RomsMarblApplication(ApplicationDefinition[RomsMarblBlueprint, RomsMarblRunner]):
    name = _APP_NAME
    long_name = _APP_NAME_LONG
    runner = RomsMarblRunner
    blueprint = RomsMarblBlueprint
    applicable_transforms = (RomsMarblTimeSplitter, OverrideTransform)
