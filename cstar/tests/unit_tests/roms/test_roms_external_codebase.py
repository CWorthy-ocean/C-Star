import os
import pathlib
from unittest import mock

import dotenv
import pytest

from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.system.manager import cstar_sysmgr


class TestROMSExternalCodeBaseInit:
    def test_init_with_args(self):
        """Test ROMSExternalCodeBase initializes correctly with arguments."""
        source_repo = "https://github.com/ucla-roms/ucla-roms.git"
        checkout_target = "246c11fa537145ba5868f2256dfb4964aeb09a25"
        roms_codebase = ROMSExternalCodeBase(
            source_repo=source_repo, checkout_target=checkout_target
        )
        assert roms_codebase.source_repo == source_repo
        assert roms_codebase.checkout_target == checkout_target
        assert (
            roms_codebase.default_source_repo
            == "https://github.com/CWorthy-ocean/ucla-roms.git"
        )
        assert roms_codebase.default_checkout_target == "main"
        assert roms_codebase.expected_env_var == "ROMS_ROOT"

    def test_init_without_args(self):
        """Test ROMSExternalCodeBase uses defaults when no args provided."""
        roms_codebase = ROMSExternalCodeBase()
        assert roms_codebase.checkout_target == roms_codebase.default_checkout_target
        assert roms_codebase.source_repo == roms_codebase.default_source_repo


class TestROMSExternalCodeBaseGet:
    """Test cases for the `get` method of `ROMSExternalCodeBase`.

    The original method:
        1. clones ROMS from `ROMSExternalCodeBase.source_repo`
        2. checks out the correct commit from `ROMSExternalCodeBase.checkout_target`
        3. Sets environment variable ROMS_ROOT and appends $ROMS_ROOT/Tools-Roms to PATH
        4. Compiles the NHMG library
        5. Compiles the Tools-Roms package

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
        self.roms_codebase = ROMSExternalCodeBase()
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
            self.roms_codebase.get(target=roms_path)

            # Assertions:
            ## Check environment variables
            dotenv.load_dotenv(dotenv_path, override=True)

            exp_roms_value = str(roms_path)
            exp_roms_tools_value = f":{roms_path / 'Tools-Roms'}"

            assert os.environ[self.roms_codebase.expected_env_var] == exp_roms_value
            assert exp_roms_tools_value in os.environ["PATH"]

            ## Check that _clone_and_checkout was (mock) called correctly
            self.mock_clone_and_checkout.assert_called_once_with(
                source_repo=self.roms_codebase.source_repo,
                local_path=roms_path,
                checkout_target=self.roms_codebase.checkout_target,
            )

            k0, v0 = self.roms_codebase.expected_env_var, str(roms_path)
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

    def test_make_nhmg_failure(self, tmp_path):
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
            self.roms_codebase.get(target=tmp_path)

        # Assertions:
        ## Check that subprocess.run was called only once due to failure
        assert self.mock_subprocess_run.call_count == 1

    def test_make_tools_roms_failure(self, tmp_path):
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
            self.roms_codebase.get(target=tmp_path)

        # Check that subprocess.run was called twice
        assert self.mock_subprocess_run.call_count == 2
