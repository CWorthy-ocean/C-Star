import os
import pathlib
from typing import Any
from unittest import mock

import dotenv
import pytest

from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.system.manager import cstar_sysmgr


@pytest.fixture
def codebase():
    """Fixture providing a configured instance of `ROMSExternalCodeBase` for testing."""
    source_repo = "https://github.com/CESR-lab/ucla-roms.git"
    checkout_target = "246c11fa537145ba5868f2256dfb4964aeb09a25"
    return ROMSExternalCodeBase(
        source_repo=source_repo, checkout_target=checkout_target
    )


def test_default_source_repo(codebase):
    """Test if the default source repo is set correctly."""
    assert codebase.default_source_repo == "https://github.com/CESR-lab/ucla-roms.git"


def test_default_checkout_target(codebase):
    """Test if the default checkout target is set correctly."""
    assert codebase.default_checkout_target == "main"


def test_expected_env_var(codebase):
    """Test if the expected environment variable is set correctly."""
    assert codebase.expected_env_var == "ROMS_ROOT"


def test_defaults_are_set():
    """Test that the defaults are set correctly."""

    roms_codebase = ROMSExternalCodeBase()
    assert roms_codebase.source_repo == "https://github.com/CESR-lab/ucla-roms.git"
    assert roms_codebase.checkout_target == "main"


class TestROMSExternalCodeBaseGet:
    """Test cases for the `get` method of `ROMSExternalCodeBase`.

    The original method:
        1. clones ROMS from `ROMSExternalCodeBase.source_repo`
        2. checks out the correct commit from `ROMSExternalCodeBase.checkout_target`
        3. Sets environment variable ROMS_ROOT and appends $ROMS_ROOT/Tools-Roms to PATH
        4. Replaces ROMS Makefiles for machine-agnostic compilation [_codebase_adjustments()]
        5. Compiles the NHMG library
        6. Compiles the Tools-Roms package

    Tests
    -----
    test_get_success
        Verifies that `get` completes successfully, setting environment variables and
        calling necessary subprocesses when all commands succeed.
    test_make_nhmg_failure
        Ensures that `get` raises an error with a descriptive message when the `make nhmg`
        command fails during installation.
    test_make_tools_roms_failure
        Confirms that `get` raises an error with an appropriate message if `make Tools-Roms`
        fails after `make nhmg` succeeds.

    Fixtures
    --------
    roms_codebase : ROMSExternalCodeBase
        A configured instance of `ROMSExternalCodeBase` for testing purposes, with predefined
        source repository and checkout target.
    tmp_path : pathlib.Path
        A temporary directory for isolating filesystem operations.

    Mocks
    -----
    mock_subprocess_run : MagicMock
        Mocks `subprocess.run` to simulate `make` commands and other shell commands.
    mock_clone_and_checkout : MagicMock
        Mocks `_clone_and_checkout` to simulate repository cloning and checkout processes.
    mock_write_to_config_file : MagicMock
        Mocks `_write_to_config_file` to simulate writing configuration data to files.
    mock_copytree : MagicMock
        Mocks `shutil.copytree` to avoid actual filesystem interactions during Makefile adjustments.
    env_patch : MagicMock
        Mocks `os.environ` to control and simulate environment variable modifications.
    """

    def setup_method(self):
        """Common setup before each test method."""
        # Mock subprocess, _clone_and_checkout, and _write_to_config_file
        self.mock_subprocess_run = mock.patch("subprocess.run").start()
        self.mock_clone_and_checkout = mock.patch(
            "cstar.roms.external_codebase._clone_and_checkout"
        ).start()

        self.mock_copytree = mock.patch("shutil.copytree").start()

        # Clear environment variables
        self.env_patch = mock.patch.dict(os.environ, {}, clear=True)
        self.env_patch.start()

    def teardown_method(self):
        """Common teardown after each test method."""
        mock.patch.stopall()

    def test_get_success(
        self,
        dotenv_path: pathlib.Path,
        roms_path: pathlib.Path,
        codebase: ROMSExternalCodeBase,
    ):
        """Test that the get method succeeds when subprocess calls succeed."""
        # Setup:
        with mock.patch(
            "cstar.system.environment.CStarEnvironment.user_env_path",
            new_callable=mock.PropertyMock,
            return_value=dotenv_path,
        ):
            ## Mock success of calls to subprocess.run:
            self.mock_subprocess_run.return_value.returncode = 0

            # Test
            ## Call the get method
            codebase.get(target=roms_path)

            # Assertions:
            ## Check environment variables
            dotenv.load_dotenv(dotenv_path, override=True)

            exp_roms_value = str(roms_path)
            exp_roms_tools_value = f":{roms_path / 'Tools-Roms'}"

            assert os.environ[codebase.expected_env_var] == exp_roms_value
            assert exp_roms_tools_value in os.environ["PATH"]

            ## Check that _clone_and_checkout was (mock) called correctly
            self.mock_clone_and_checkout.assert_called_once_with(
                source_repo=codebase.source_repo,
                local_path=roms_path,
                checkout_target=codebase.checkout_target,
            )

            k0, v0 = codebase.expected_env_var, str(roms_path)
            k1, v1 = "PATH", f"${{PATH}}{exp_roms_tools_value}"

            cfg = dotenv.dotenv_values(dotenv_path)

            # confirm user environment file was updated
            actual_value = cfg[k0]
            assert v0 == actual_value

            actual_value = cfg[k1]
            assert actual_value is not None
            assert v1.split(":")[1] in actual_value

            self.mock_subprocess_run.assert_any_call(
                f"make nhmg COMPILER={cstar_sysmgr.environment.compiler}",
                cwd=roms_path / "Work",
                capture_output=True,
                text=True,
                shell=True,
            )

            self.mock_subprocess_run.assert_any_call(
                f"make COMPILER={cstar_sysmgr.environment.compiler}",
                cwd=roms_path / "Tools-Roms",
                capture_output=True,
                text=True,
                shell=True,
            )

    def test_get_prebuilt(
        self,
        dotenv_path: pathlib.Path,
        roms_path: pathlib.Path,
        codebase: ROMSExternalCodeBase,
    ):
        """Test that the get method succeeds when a prebuilt ROMS codebase is
        configured."""

        env_copy = os.environ.copy()
        env_copy[codebase.expected_env_var] = str(roms_path)
        env_copy[codebase.prebuilt_env_var] = "1"

        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.user_env_path",
                new_callable=mock.PropertyMock,
                return_value=dotenv_path,
            ),
            mock.patch.dict(os.environ, env_copy),
        ):
            ## Mock subprocess.run calls to appear successful
            self.mock_subprocess_run.return_value.returncode = 0

            # Test
            codebase.get(target=roms_path)

            ## Assertions:
            # Confirm that the codebase ignores the prebuilt environment variable
            self.mock_clone_and_checkout.assert_called()

    def test_local_config_status_with_prebuilt_and_root(
        self,
        dotenv_path: pathlib.Path,
        codebase: ROMSExternalCodeBase,
        custom_user_env: Any,
    ):
        """Test that the `local_config_status` method return an appropriate return code
        when a prebuilt ROMS codebase is configured."""

        mock_path = "/any/path/it/is/not/verified"
        custom_user_env(
            {
                codebase.prebuilt_env_var: "1",
                codebase.expected_env_var: mock_path,
            }
        )
        assert os.environ[codebase.prebuilt_env_var] == "1"
        assert os.environ[codebase.expected_env_var] == mock_path

        # Test
        status_code = codebase.local_config_status

        ## Assertions:
        # Confirm that the status is 0 when prebuilt & expected var are set
        assert status_code == 0

    def test_local_config_status_with_prebuilt_and_no_root(
        self,
        codebase: ROMSExternalCodeBase,
        custom_user_env: Any,
    ):
        """Test that the `local_config_status` method returns an appropriate return code
        when a prebuilt ROMS codebase is configured but the expected environment
        variable is not set."""

        # Do NOT set `xxx_codebase.expected_env_var`, only prebuilt_env_var
        custom_user_env({codebase.prebuilt_env_var: "1"})
        assert os.environ[codebase.prebuilt_env_var] == "1"
        assert codebase.expected_env_var not in os.environ

        # Test
        status_code = codebase.local_config_status

        ## Assertions:
        # Confirm that the status is 4 due to missing XXX_ROOT
        assert status_code == 4

    def test_make_nhmg_failure(self, codebase, tmp_path):
        """Test that the get method raises an error when 'make nhmg' fails."""

        ## There are two subprocess calls, we'd like one fail, one pass:
        self.mock_subprocess_run.side_effect = [
            mock.Mock(
                returncode=1, stderr="Compiling NHMG library failed successfully"
            ),  # Fail nhmg
            mock.Mock(returncode=0),  # Success for Tools-Roms (won't be reached)
        ]
        dotenv_path = tmp_path / ".cstar.env"

        # Test
        with (
            pytest.raises(
                RuntimeError,
                match="Error when compiling ROMS' NHMG library. Return Code: `1`. STDERR:\nCompiling NHMG library failed successfully",
            ),
            mock.patch(
                "cstar.system.environment.CStarEnvironment.user_env_path",
                new_callable=mock.PropertyMock,
                return_value=dotenv_path,
            ),
        ):
            codebase.get(target=tmp_path)

        # Assertions:
        ## Check that subprocess.run was called only once due to failure
        assert self.mock_subprocess_run.call_count == 1

    def test_make_tools_roms_failure(self, codebase, tmp_path):
        """Test that the get method raises an error when 'make Tools-Roms' fails."""

        # Simulate success for `make nhmg` and failure for `make Tools-Roms`
        self.mock_subprocess_run.side_effect = [
            mock.Mock(returncode=0),  # Success for nhmg
            mock.Mock(
                returncode=1,
                stderr="Error when compiling Tools-Roms. Return Code: `1`. STDERR:\nCompiling Tools-Roms failed successfully",
            ),  # Fail Tools-Roms
        ]

        with pytest.raises(
            RuntimeError, match="Compiling Tools-Roms failed successfully"
        ):
            codebase.get(target=tmp_path)

        # Check that subprocess.run was called twice
        assert self.mock_subprocess_run.call_count == 2

    def test_prebuilt_env_var(self):
        """Verify that the value of the prebuilt_env_var is set correctly."""
        roms_codebase = ROMSExternalCodeBase()
        assert roms_codebase.prebuilt_env_var == "CSTAR_ROMS_PREBUILT"
