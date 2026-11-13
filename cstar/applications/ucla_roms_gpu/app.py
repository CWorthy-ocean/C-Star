import typing as t
from pathlib import Path
from typing import TYPE_CHECKING, override

from cstar.applications.core import ApplicationDefinition, register_application
from cstar.applications.ucla_roms.app import (
    RomsSimulationRunner,
    roms_artifacts,
    roms_protected_variables,
)
from cstar.applications.ucla_roms.transforms import (
    CarbonateSensitivityDirective,
    ContinuanceDirective,
    NestingDirective,
)
from cstar.applications.ucla_roms_gpu.models import (
    APP_NAME,
    TOOLCHAIN,
    UclaRomsGpuBlueprint,
)
from cstar.applications.ucla_roms_gpu.resume import prepare_resume_blueprint
from cstar.roms.file_system import RomsFileSystemManager
from cstar.roms.gpu_external_codebase import CODEBASE_KEY

if TYPE_CHECKING:
    from collections.abc import Sequence

    from cstar.applications.core import ArtifactSpec
    from cstar.roms.simulation import ROMSSimulation

_APP_NAME_LONG: t.Literal["UCLA-ROMS GPU simulation runner"] = (
    "UCLA-ROMS GPU simulation runner"
)


class UclaRomsGpuRunner(RomsSimulationRunner[UclaRomsGpuBlueprint]):
    """Worker class to run c-star simulations on GPUs."""

    @classmethod
    @override
    def _simulation_class(cls) -> "type[ROMSSimulation]":
        from cstar.roms.gpu_simulation import ROMSGpuSimulation

        return ROMSGpuSimulation

    @classmethod
    @override
    def _resume_blueprint(cls, blueprint_uri: str) -> str:
        return prepare_resume_blueprint(blueprint_uri)


@register_application
class UclaRomsGpuApplication(
    ApplicationDefinition[UclaRomsGpuBlueprint, UclaRomsGpuRunner]
):
    name: str = APP_NAME
    long_name: str = _APP_NAME_LONG
    runner = UclaRomsGpuRunner
    blueprint = UclaRomsGpuBlueprint
    # `RomsTimeSplitter` rebuilds a ucla_roms blueprint, so it is not listed
    applicable_transforms = ()
    directives = (
        ContinuanceDirective,
        NestingDirective,
        CarbonateSensitivityDirective,
    )
    resumable = True
    pre_runnable = True
    toolchain = TOOLCHAIN

    @override
    def artifacts(self, blueprint: UclaRomsGpuBlueprint) -> "Sequence[ArtifactSpec]":
        """Declare the files a GPU UCLA-ROMS run leaves in its step root.

        Parameters
        ----------
        blueprint : UclaRomsGpuBlueprint
            The blueprint of the GPU UCLA-ROMS step.

        Returns
        -------
        Sequence[ArtifactSpec]
            The joined output files by stream (the streams other steps consume
            under their own kinds), the namelist and MARBL configuration the
            model read, the GPU executable and the run log. The model has no
            `cppdefs.opt`, so none is declared.
        """
        # resolved when asked, not at import, because `cstar.roms.gpu_simulation`
        # imports this package
        from cstar.roms.gpu_simulation import GPU_BUILD_DIR_NAME, GPU_EXECUTABLE_NAME

        # a manager on a placeholder root only spells the layout; it touches no disk
        fs = RomsFileSystemManager(Path("/"))
        return roms_artifacts(
            fs,
            executable=fs.codebase_subdir(CODEBASE_KEY)
            / GPU_BUILD_DIR_NAME
            / GPU_EXECUTABLE_NAME,
        )

    @override
    def protected_variables(self) -> "Sequence[str]":
        """Name the variables compression keeps exact: the same as for CPU UCLA-ROMS
        output, which the GPU model writes in the same streams.

        Returns
        -------
        Sequence[str]
            Variable names and glob patterns (`roms_protected_variables`).
        """
        return roms_protected_variables()
