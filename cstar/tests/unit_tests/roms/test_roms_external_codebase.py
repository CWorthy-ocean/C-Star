import os
import pathlib
import subprocess
import unittest.mock as mock

import dotenv
import pytest

import cstar
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.system.manager import cstar_sysmgr


class TestROMSExternalCodeBaseInit:
    """Test initialization of ROMSExternalCodeBase"""

    @mock.patch(
        "cstar.roms.external_codebase.ROMSExternalCodeBase.is_configured",
        new_callable=mock.PropertyMock,
        return_value=False,
    )
    @mock.patch("cstar.base.external_codebase.SourceData.__init__")
    def test_init_with_args(self, mock_source_init, mock_is_configured):
        """Test Initialization with provided source_repo and checkout_target args

        Asserts SourceData is instantiated with the correct parameters.
        """
        mock_source_init.return_value = None

        source_repo = "https://www.github.com/CESR-lab/ucla-roms.git"
        checkout_target = "246c11fa537145ba5868f2256dfb4964aeb09a25"

        roms_codebase = ROMSExternalCodeBase(
            source_repo=source_repo, checkout_target=checkout_target
        )

        mock_source_init.assert_called_once_with(
            location=source_repo, identifier=checkout_target
        )

        assert (
            roms_codebase._default_source_repo
            == "https://github.com/CWorthy-ocean/ucla-roms.git"
        )
        assert roms_codebase._default_checkout_target == "main"
        assert roms_codebase.root_env_var == "ROMS_ROOT"

    @mock.patch(
        "cstar.roms.external_codebase.ROMSExternalCodeBase.is_configured",
        new_callable=mock.PropertyMock,
        return_value=False,
    )
    @mock.patch("cstar.base.external_codebase.SourceData.__init__")
    def test_init_without_args(self, mock_source_init, mock_is_configured):
        """Test that the defaults are set correctly if ROMSExternalCodeBase initialized
        without args.
        """
        mock_source_init.return_value = None
        roms_codebase = ROMSExternalCodeBase()

        mock_source_init.assert_called_once_with(
            location=roms_codebase._default_source_repo,
            identifier=roms_codebase._default_checkout_target,
        )


class TestROMSExternalCodeBaseConfigure:
    def test_configure_success(
        self,
        romsexternalcodebase_staged,
        dotenv_path: pathlib.Path,
        roms_path: pathlib.Path,
    ):
        """Test that the _configure method succeeds when subprocess calls succeed."""
        recb = romsexternalcodebase_staged
        with mock.patch.object(
            cstar.roms.external_codebase, "_run_cmd"
        ) as mock_run_cmd:
            mock_run_cmd.side_effect = [
                mock.Mock(returncode=0),  # first call
                mock.Mock(returncode=0),  # second call
            ]
            recb._configure()

            # Assertions:
            ## Check environment variables
            dotenv.load_dotenv(dotenv_path, override=True)
            assert os.environ[recb.root_env_var] == str(recb.working_copy.path)
            dotenv_var = dotenv.get_key(dotenv_path, recb.root_env_var)
            assert dotenv_var == str(recb.working_copy.path)

        mock_run_cmd.assert_any_call(
            f"make nhmg COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=roms_path / "Work",
            msg_pre="Compiling NHMG library...",
            msg_err="Error when compiling ROMS' NHMG library.",
            raise_on_error=True,
        )

    @mock.patch("cstar.base.utils.subprocess.run")
    def test_make_nhmg_failure(
        self,
        mock_subprocess,
        romsexternalcodebase_staged,
        dotenv_path,
    ):
        """Test that the _configure method raises an error when 'NHMG/make' fails."""
        mock_subprocess.side_effect = [
            subprocess.CompletedProcess(
                args=["make nhmg"],
                returncode=1,
                stdout="",
                stderr="Mocked ROMS Compilation Failure",
            ),
        ]
        recb = romsexternalcodebase_staged

        # Test
        with (
            pytest.raises(
                RuntimeError,
                match=(
                    "Error when compiling ROMS' NHMG library. Return Code: `1`. STDERR:\n"
                    "Mocked ROMS Compilation Failure"
                ),
            ),
        ):
            recb._configure()
        assert mock_subprocess.call_count == 1

    @mock.patch("cstar.base.utils.subprocess.run")
    def test_make_tools_failure(
        self,
        mock_subprocess,
        romsexternalcodebase_staged,
        dotenv_path,
    ):
        """Test that the _configure method raises an error when 'Tools-Roms/make' fails."""
        mock_subprocess.side_effect = [
            subprocess.CompletedProcess(
                args=["make nhmg"], returncode=0, stdout="", stderr=""
            ),
            subprocess.CompletedProcess(
                args=["make Tools-Roms"],
                returncode=1,
                stdout="",
                stderr="Mocked ROMS Compilation Failure",
            ),
        ]
        recb = romsexternalcodebase_staged

        # Test
        with pytest.raises(
            RuntimeError,
            match=(
                "Error when compiling Tools-Roms. Return Code: `1`. STDERR:\n"
                "Mocked ROMS Compilation Failure"
            ),
        ):
            recb._configure()
        assert mock_subprocess.call_count == 2

    def test_is_configured_when_configured(
        self,
        romsexternalcodebase_staged,
        roms_path,
        dotenv_path,
    ):
        """Tests that the `is_configured` property returns True when all conditions met:

        Conditions:
        - environment variable is defined
        - local repository clone is clean
        - Tools-Roms/mpc program exists

        """
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value={"ROMS_ROOT": roms_path},
            ),
            mock.patch(
                "cstar.roms.external_codebase._check_local_repo_changed_from_remote",
                return_value=False,
            ),
            mock.patch("pathlib.Path.exists", return_value=True),
            mock.patch(
                "pathlib.Path.iterdir", return_value=["fake", "files", "in", "dir"]
            ),
        ):
            assert romsexternalcodebase_staged.is_configured

    @pytest.mark.parametrize(
        "env_var_defined, repo_changed, mpc_exists, expected",
        [
            # 1. Missing ROMS_ROOT
            (False, False, True, False),
            # 2. Repo has changed
            (True, True, True, False),
            # 3. mpc program missing
            (True, False, False, False),
            # 5. Fully configured
            (True, False, True, True),
        ],
    )
    def test_is_configured_variants(
        self,
        romsexternalcodebase_staged,
        roms_path,
        env_var_defined,
        repo_changed,
        mpc_exists,
        expected,
    ):
        """Tests all possible combinations of conditions for `ROMSExternalCodeBase.is_configured`.

        Parameters
        ----------
        env_var_defined (bool):
            Whether the ROMS environment variable is defined
        repo_changed (bool):
            Whether the local repository clone is clean
        mpc_exists (bool):
            Whether the Tools-Roms/mpc program exists
        expected (bool):
            The expected outcome of the property with the other parameters.
        """
        env_vars = {"ROMS_ROOT": str(roms_path)} if env_var_defined else {}

        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            mock.patch(
                "cstar.roms.external_codebase._check_local_repo_changed_from_remote",
                return_value=repo_changed,
            ),
            mock.patch("pathlib.Path.exists", return_value=mpc_exists),
        ):
            assert romsexternalcodebase_staged.is_configured is expected
