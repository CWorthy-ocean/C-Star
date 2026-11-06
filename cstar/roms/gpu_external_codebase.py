from pathlib import Path
from typing import Final

from cstar.base.external_codebase import ExternalCodeBase
from cstar.base.utils import _run_cmd

MARBL_SOURCE_SENTINEL: Final = "MARBL/src/marbl_interface.F90"
"""A file of the MARBL submodule's source that the GPU build compiles
(`marbl_interface` in the Makefile's `MARBL_NAMES`), relative to the clone's root.
A clone that has not fetched the submodule has the empty directory only."""

CODEBASE_KEY: Final = "roms"
"""The codebase's directory under a simulation's `input/codebases` (its `key`, as
`ExternalCodeBase` derives it from `ROMS_GPU_ROOT`). Spelled once here so the
application can declare the executable's path without building a codebase."""


class ROMSGpuExternalCodeBase(ExternalCodeBase):
    """An implementation of the ExternalCodeBase class for gpu-ucla-roms, the
    CUDA-Fortran port of UCLA ROMS.

    The repository holds MARBL as a git submodule, which the model's build
    compiles into the executable, so configuring this codebase fetches the
    submodule. Nothing is compiled here: `ROMSGpuSimulation.build` compiles
    the model, once the namelist has fixed the build's constants.

    Methods:
    --------
    get()
        inherited from ExternalCodeBase.get() to clone the gpu-ucla-roms repository
    configure()
        overrides ExternalCodeBase.configure() to set the environment and fetch the MARBL submodule
    """

    @property
    def _default_source_repo(self) -> str:
        return "https://github.com/CWorthy-ocean/gpu-ucla-roms.git"

    @property
    def _default_checkout_target(self) -> str:
        return "v0.1.2-beta"

    @property
    def root_env_var(self) -> str:
        return "ROMS_GPU_ROOT"

    @property
    def key(self) -> str:
        """The codebase's directory under a simulation's `input/codebases`."""
        return CODEBASE_KEY

    def _configure(self) -> None:
        """Fetch the MARBL submodule the GPU build compiles.

        A plain clone of the repository leaves the submodule's directory empty.
        """
        self._export_env()
        assert self.working_copy is not None  # verified by ExternalCodeBase.configure()

        _run_cmd(
            "git submodule update --init --recursive",
            cwd=self.working_copy.path,
            msg_pre="Fetching the MARBL submodule of gpu-ucla-roms...",
            msg_post="Fetched the MARBL submodule",
            msg_err="Error when fetching the MARBL submodule of gpu-ucla-roms.",
            raise_on_error=True,
        )

    def _is_built_at(self, root: Path) -> bool:
        # Nothing is compiled by this codebase; what it must hold is the
        # submodule's source (the build itself is checked by the simulation)
        return (root / MARBL_SOURCE_SENTINEL).is_file()
