import os
import pathlib
import subprocess
import unittest.mock as mock

import pytest

from cstar.pio.external_codebase import PIOExternalCodeBase


class TestPIOExternalCodeBaseInit:
    """Test initialization of PIOExternalCodeBase"""

    @mock.patch(
        "cstar.pio.external_codebase.PIOExternalCodeBase.is_configured",
        new_callable=mock.PropertyMock,
        return_value=False,
    )
    @mock.patch("cstar.base.external_codebase.SourceData.__init__")
    def test_init_with_args(self, mock_source_init, mock_is_configured):
        """Test Initialization with provided source_repo and checkout_target args

        Asserts SourceData is instantiated with the correct parameters.
        """
        mock_source_init.return_value = None  # __init__ should return None

        source_repo = "https://github.com/dafyddstephenson/ParallelIO.git"
        checkout_target = "main"

        PIOExternalCodeBase(source_repo=source_repo, checkout_target=checkout_target)

        mock_source_init.assert_called_once_with(
            location=source_repo, identifier=checkout_target
        )

    @mock.patch(
        "cstar.pio.external_codebase.PIOExternalCodeBase.is_configured",
        new_callable=mock.PropertyMock,
        return_value=False,
    )
    @mock.patch("cstar.base.external_codebase.SourceData.__init__")
    def test_init_without_args(self, mock_source_init, mock_is_configured):
        """Test that the defaults are set correctly if PIOExternalCodeBase initialized
        without args.
        """
        mock_source_init.return_value = None
        pio_codebase = PIOExternalCodeBase()

        mock_source_init.assert_called_once_with(
            location=pio_codebase._default_source_repo,
            identifier=pio_codebase._default_checkout_target,
        )
        assert (
            pio_codebase._default_source_repo
            == "https://github.com/NCAR/ParallelIO.git"
        )

        assert pio_codebase._default_checkout_target == "pio2_7_0"
        assert pio_codebase.root_env_var == "PIO_ROOT"
        assert pio_codebase.key == "pio"


class TestPIOExternalCodeBaseConfigure:
    def test_configure_success(
        self,
        pioexternalcodebase_staged,
        pio_path: pathlib.Path,
    ):
        """Test that the _configure method succeeds when subprocess calls succeed."""
        pecb = pioexternalcodebase_staged
        env_vars = {
            "NETCDFHOME": "/netcdf/home/",
            "PNETCDFHOME": "/pnetcdf/home/",
        }
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            mock.patch("cstar.pio.external_codebase._run_cmd") as mock_run_cmd,
        ):
            mock_run_cmd.return_value.returncode = 0
            pecb._configure()

        ## Check that environment was updated correctly
        actual_value = os.getenv(pecb.root_env_var)
        assert actual_value == str(pecb.working_copy.path)

        assert mock_run_cmd.call_count == 2
        configure_call, build_call = mock_run_cmd.call_args_list

        configure_cmd = configure_call.args[0]
        assert configure_cmd.startswith(
            'PKG_CONFIG_PATH="/netcdf/home/lib/pkgconfig:/pnetcdf/home/lib/pkgconfig'
            ':${PKG_CONFIG_PATH:-}" cmake -S . -B build '
        )
        assert "-DCMAKE_PREFIX_PATH='/netcdf/home/;/pnetcdf/home/'" in configure_cmd
        assert "-DHAVE_PAR_FILTERS=FALSE" in configure_cmd
        assert "-DPIO_ENABLE_TIMING=OFF" in configure_cmd
        assert "-DBUILD_SHARED_LIBS=OFF" in configure_cmd
        assert configure_call.kwargs["cwd"] == pio_path
        assert configure_call.kwargs["raise_on_error"] is True

        assert build_call.args[0] == "cmake --build build --parallel 4"
        assert build_call.kwargs["cwd"] == pio_path
        assert build_call.kwargs["raise_on_error"] is True

    def test_configure_uses_mpihome_compiler_wrappers(
        self,
        pioexternalcodebase_staged,
        tmp_path: pathlib.Path,
    ):
        """Test that MPI compiler wrappers under MPIHOME are used when present."""
        mpi_home = tmp_path / "mpi"
        (mpi_home / "bin").mkdir(parents=True)
        (mpi_home / "bin" / "mpicc").touch()
        (mpi_home / "bin" / "mpif90").touch()

        env_vars = {
            "NETCDFHOME": "/netcdf/home/",
            "PNETCDFHOME": "/pnetcdf/home/",
            "MPIHOME": str(mpi_home),
        }
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            mock.patch("cstar.pio.external_codebase._run_cmd") as mock_run_cmd,
        ):
            pioexternalcodebase_staged._configure()

        configure_cmd = mock_run_cmd.call_args_list[0].args[0]
        assert f"-DCMAKE_C_COMPILER={mpi_home}/bin/mpicc" in configure_cmd
        assert f"-DCMAKE_Fortran_COMPILER={mpi_home}/bin/mpif90" in configure_cmd

    @pytest.mark.parametrize(
        "system_name, expected_clause",
        [
            # On macOS, CMake pairs conda clang with llvm-ranlib, which rejects
            # the Apple-style flags CMake passes; the PATH ranlib is pinned instead
            ("darwin_arm64", "-DCMAKE_RANLIB:FILEPATH=/fake/bin/ranlib"),
            ("linux_x86_64", None),
            ("perlmutter", None),
        ],
    )
    def test_configure_pins_ranlib_on_darwin(
        self,
        pioexternalcodebase_staged,
        system_name: str,
        expected_clause: str | None,
    ):
        """Test that the cmake configure command pins CMAKE_RANLIB to the PATH
        ranlib on Darwin systems only.
        """
        env_vars = {
            "NETCDFHOME": "/netcdf/home/",
            "PNETCDFHOME": "/pnetcdf/home/",
        }
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            mock.patch(
                "cstar.system.manager.CStarSystemManager.name",
                new_callable=mock.PropertyMock,
                return_value=system_name,
            ),
            mock.patch(
                "cstar.pio.external_codebase.shutil.which",
                return_value="/fake/bin/ranlib",
            ),
            mock.patch("cstar.pio.external_codebase._run_cmd") as mock_run_cmd,
        ):
            pioexternalcodebase_staged._configure()

        configure_cmd = mock_run_cmd.call_args_list[0].args[0]
        if expected_clause:
            assert expected_clause in configure_cmd
        else:
            assert "CMAKE_RANLIB" not in configure_cmd

    @pytest.mark.parametrize(
        "env_vars, missing",
        [
            ({"NETCDFHOME": "/netcdf/home/"}, "PNETCDFHOME"),
            ({"PNETCDFHOME": "/pnetcdf/home/"}, "NETCDFHOME"),
            ({}, "NETCDFHOME and PNETCDFHOME"),
        ],
    )
    def test_configure_missing_dependency_raises(
        self,
        pioexternalcodebase_staged,
        env_vars: dict[str, str],
        missing: str,
    ):
        """Test that _configure raises when NETCDFHOME or PNETCDFHOME are unset."""
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            mock.patch.dict(os.environ, env_vars, clear=True),
            mock.patch("cstar.pio.external_codebase._run_cmd") as mock_run_cmd,
            pytest.raises(
                EnvironmentError,
                match=f"Cannot build ParallelIO: {missing} not set",
            ),
        ):
            pioexternalcodebase_staged._configure()

        mock_run_cmd.assert_not_called()

    @mock.patch("cstar.base.utils.subprocess.run")
    def test_cmake_failure(
        self,
        mock_subprocess,
        pioexternalcodebase_staged,
    ):
        """Test that the _configure method raises an error when cmake fails."""
        mock_subprocess.side_effect = [
            subprocess.CompletedProcess(
                args=["cmake"],
                returncode=1,
                stdout="",
                stderr="Mocked ParallelIO Configuration Failure",
            )
        ]
        env_vars = {
            "NETCDFHOME": "/netcdf/home/",
            "PNETCDFHOME": "/pnetcdf/home/",
        }

        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            pytest.raises(
                RuntimeError,
                match=(
                    "Error when configuring ParallelIO. Return Code: `1`. STDERR:\n"
                    "Mocked ParallelIO Configuration Failure"
                ),
            ),
        ):
            pioexternalcodebase_staged._configure()

    @pytest.mark.parametrize(
        "env_var_defined, repo_changed, missing_libs, expected",
        [
            # 1. Missing PIO_ROOT
            (False, False, [], False),
            # 2. Repo has changed
            (True, True, [], False),
            # 3. C library file missing
            (True, False, ["build/src/clib/libpioc.a"], False),
            # 4. Fortran library file missing
            (True, False, ["build/src/flib/libpiof.a"], False),
            # 5. Fully configured
            (True, False, [], True),
        ],
    )
    def test_is_configured_variants(
        self,
        pioexternalcodebase_staged: PIOExternalCodeBase,
        tmp_path: pathlib.Path,
        env_var_defined: bool,
        repo_changed: bool,
        missing_libs: list[str],
        expected: bool,
    ):
        """Tests all possible combinations of conditions for `PIOExternalCodeBase.is_configured`.

        Parameters
        ----------
        env_var_defined (bool):
            Whether the PIO_ROOT environment variable is defined
        repo_changed (bool):
            Whether the local repository clone has changed from the remote
        missing_libs (list[str]):
            Library files (relative to PIO_ROOT) that do NOT exist locally
        expected (bool):
            The expected outcome of the property with the other parameters.
        """
        env_vars = {"PIO_ROOT": str(tmp_path)} if env_var_defined else {}

        for lib in ("build/src/clib/libpioc.a", "build/src/flib/libpiof.a"):
            if lib not in missing_libs:
                lib_path = tmp_path / lib
                lib_path.parent.mkdir(parents=True, exist_ok=True)
                lib_path.touch()

        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            mock.patch(
                "cstar.pio.external_codebase._check_local_repo_changed_from_remote",
                return_value=repo_changed,
            ),
        ):
            assert pioexternalcodebase_staged.is_configured is expected
