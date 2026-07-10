import os
import shutil
from pathlib import Path

from cstar.base.external_codebase import ExternalCodeBase
from cstar.base.gitutils import _check_local_repo_changed_from_remote
from cstar.base.utils import _run_cmd
from cstar.system.manager import cstar_sysmgr


class PIOExternalCodeBase(ExternalCodeBase):
    """An implementation of the ExternalCodeBase class for the NCAR Parallel IO library.

    This subclass sets unique values for ExternalCodeBase properties specific to
    ParallelIO (PIO), and configures the library by building it with CMake.

    PIO is built in-tree in a subdirectory literally named `build`, without an
    install step, because the ROMS Makedefs.inc links directly against
    `$(PIO_ROOT)/build/src/clib` and `$(PIO_ROOT)/build/src/flib`. Timing support
    (GPTL) is disabled as ROMS links only `-lpiof -lpioc`.

    Note: unlike MARBL, the built libraries carry no compiler suffix, so switching
    compilers on a system requires a fresh build (e.g. `CSTAR_FRESH_CODEBASES=1`).
    """

    @property
    def _default_source_repo(self) -> str:
        return "https://github.com/NCAR/ParallelIO.git"

    @property
    def _default_checkout_target(self) -> str:
        return "pio2_7_0"

    @property
    def root_env_var(self) -> str:
        return "PIO_ROOT"

    def _get_dependency_root(self, env_var: str) -> str | None:
        """Look up a dependency location from the C-Star environment or the process
        environment.
        """
        return cstar_sysmgr.environment.environment_variables.get(
            env_var
        ) or os.environ.get(env_var)

    def _configure(self) -> None:
        """Configure the PIO codebase on the local machine.

        This method builds the PIO C and Fortran libraries with CMake and adds
        necessary variables to the environment.

        Raises
        ------
        EnvironmentError
            If NETCDFHOME or PNETCDFHOME are not set for the current system.
            PNETCDFHOME is required (rather than optional, as for PIO in general)
            because the ROMS Makedefs.inc links against PnetCDF whenever PIO is
            enabled.
        """
        assert self.working_copy is not None  # Has been verified by `configure()``
        pio_root = self.working_copy.path
        # Set env var:
        cstar_sysmgr.environment.set_env_var(self.root_env_var, str(pio_root))

        netcdf_home = self._get_dependency_root("NETCDFHOME")
        pnetcdf_home = self._get_dependency_root("PNETCDFHOME")
        missing = [
            var
            for var, val in (("NETCDFHOME", netcdf_home), ("PNETCDFHOME", pnetcdf_home))
            if not val
        ]
        if missing:
            raise OSError(
                f"Cannot build ParallelIO: {' and '.join(missing)} not set. "
                "These should be defined in the environment file for your system "
                f"({cstar_sysmgr.environment.system_env_path}). Note that ROMS "
                "requires PnetCDF whenever ParallelIO is enabled."
            )

        mpi_home = self._get_dependency_root("MPIHOME")
        mpicc, mpifc = "mpicc", "mpif90"
        if mpi_home and (Path(mpi_home) / "bin/mpicc").exists():
            mpicc = str(Path(mpi_home) / "bin/mpicc")
        if mpi_home and (Path(mpi_home) / "bin/mpif90").exists():
            mpifc = str(Path(mpi_home) / "bin/mpif90")

        # On macOS, CMake finishes static archives with Apple-style ranlib flags
        # (`-c`) but pairs conda's clang with llvm-ranlib, which rejects them.
        # Pin CMAKE_RANLIB to the `ranlib` on PATH (cctools in a conda env,
        # Apple's outside one), which accepts those flags.
        ranlib_clause = ""
        if cstar_sysmgr.name.startswith("darwin"):
            ranlib = shutil.which("ranlib")
            if ranlib:
                ranlib_clause = f"-DCMAKE_RANLIB:FILEPATH={ranlib} "

        # Configure. The build directory must be named `build` and the library must
        # not be installed elsewhere: ROMS' Makedefs.inc hardcodes both.
        _run_cmd(
            "cmake -S . -B build "
            f"-DCMAKE_C_COMPILER={mpicc} "
            f"-DCMAKE_Fortran_COMPILER={mpifc} "
            f"-DNetCDF_C_PATH={netcdf_home} "
            f"-DNetCDF_Fortran_PATH={netcdf_home} "
            f"-DPnetCDF_PATH={pnetcdf_home} "
            f"{ranlib_clause}"
            "-DPIO_ENABLE_TIMING=OFF "
            "-DPIO_ENABLE_TESTS=OFF "
            "-DPIO_ENABLE_EXAMPLES=OFF "
            "-DPIO_ENABLE_DOC=OFF "
            "-DBUILD_SHARED_LIBS=OFF",
            cwd=pio_root,
            msg_pre="Configuring ParallelIO...",
            msg_err="Error when configuring ParallelIO.",
            raise_on_error=True,
        )

        # Compile
        _run_cmd(
            "cmake --build build --parallel 4",
            cwd=pio_root,
            msg_pre="Compiling ParallelIO...",
            msg_post=f"ParallelIO successfully installed at {pio_root}",
            msg_err="Error when compiling ParallelIO.",
            raise_on_error=True,
        )

    @property
    def is_configured(self) -> bool:
        """Determine whether PIO is configured locally.

        This method confirms that:
        - Necessary environment variables are set
        - The repository has not changed from that described by `source`.
        - PIO's C and Fortran libraries exist in the in-tree build directory
        """
        # Check PIO_ROOT env var is set:
        pio_root = cstar_sysmgr.environment.environment_variables.get(self.root_env_var)
        if not pio_root:
            return False
        # Check PIO repo hasn't changed:
        assert self.source.checkout_target is not None  # cannot be for ExternalCodeBase
        # NOTE can't use self.working_copy.changed_from_source here as ExternalCodeBase uses this property to set `working_copy`
        if _check_local_repo_changed_from_remote(
            remote_repo=self.source.location,
            local_repo=pio_root,
            checkout_target=self.source.checkout_target,
        ):
            return False
        # Check library files exist:
        for lib in ("build/src/clib/libpioc.a", "build/src/flib/libpiof.a"):
            if not (Path(pio_root) / lib).exists():
                return False

        return True
