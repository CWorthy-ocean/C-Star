import os
import pytest
from unittest import mock
from cstar.roms.base_model import ROMSBaseModel
from cstar.base.system import cstar_system


@pytest.fixture
def roms_base_model():
    """Fixture providing a configured instance of `ROMSBaseModel` for testing."""
    source_repo = "https://github.com/CESR-lab/ucla-roms.git"
    checkout_target = "246c11fa537145ba5868f2256dfb4964aeb09a25"
    return ROMSBaseModel(source_repo=source_repo, checkout_target=checkout_target)


def test_default_source_repo(roms_base_model):
    """Test if the default source repo is set correctly."""
    assert (
        roms_base_model.default_source_repo
        == "https://github.com/CESR-lab/ucla-roms.git"
    )


def test_default_checkout_target(roms_base_model):
    """Test if the default checkout target is set correctly."""
    assert roms_base_model.default_checkout_target == "main"


def test_expected_env_var(roms_base_model):
    """Test if the expected environment variable is set correctly."""
    assert roms_base_model.expected_env_var == "ROMS_ROOT"


def test_defaults_are_set():
    """Test that the defaults are set correctly."""

    roms_base_model = ROMSBaseModel()
    assert roms_base_model.source_repo == "https://github.com/CESR-lab/ucla-roms.git"
    assert roms_base_model.checkout_target == "main"


class TestROMSBaseModelGet:
    """Test cases for the `get` method of `ROMSBaseModel`.

    The original method:
        1. clones ROMS from `ROMSBaseModel.source_repo`
        2. checks out the correct commit from `ROMSBaseModel.checkout_target`
        3. Sets environment variable ROMS_ROOT and appends $ROMS_ROOT/Tools-Roms to PATH
        4. Replaces ROMS Makefiles for machine-agnostic compilation [_base_model_adjustments()]
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
    roms_base_model : ROMSBaseModel
        A configured instance of `ROMSBaseModel` for testing purposes, with predefined
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
            "cstar.roms.base_model._clone_and_checkout"
        ).start()
        self.mock_update_user_dotenv = mock.patch(
            "cstar.roms.base_model._update_user_dotenv"
        ).start()

        self.mock_copytree = mock.patch("shutil.copytree").start()

        # Clear environment variables
        self.env_patch = mock.patch.dict(os.environ, {}, clear=True)
        self.env_patch.start()

    def teardown_method(self):
        """Common teardown after each test method."""
        mock.patch.stopall()

    def test_get_success(self, roms_base_model, tmp_path):
        """Test that the get method succeeds when subprocess calls succeed."""
        # Setup:
        ## Make temporary target dir
        roms_dir = tmp_path / "roms"

        ## Mock success of calls to subprocess.run:
        self.mock_subprocess_run.return_value.returncode = 0

        # Test
        ## Call the get method
        roms_base_model.get(target=roms_dir)

        # Assertions:
        ## Check environment variables

        assert os.environ["ROMS_ROOT"] == str(roms_dir)
        assert f":{roms_dir}/Tools-Roms/" in os.environ["PATH"]

        ## Check that _clone_and_checkout was (mock) called correctly
        self.mock_clone_and_checkout.assert_called_once_with(
            source_repo=roms_base_model.source_repo,
            local_path=roms_dir,
            checkout_target=roms_base_model.checkout_target,
        )

        ## Check that _update_user_dotenv was (mock) called correctly
        env_file_str = (
            f"ROMS_ROOT={roms_dir}" + "\nPATH=${PATH}:" + f"{roms_dir}/Tools-Roms\n"
        )

        self.mock_update_user_dotenv.assert_called_once_with(env_file_str)

        ## Check that subprocess.run was (mock) called twice for `make nhmg` and `make Tools-Roms`
        print(self.mock_subprocess_run.call_args_list)
        assert self.mock_subprocess_run.call_count == 2

        self.mock_subprocess_run.assert_any_call(
            f"make nhmg COMPILER={cstar_system.environment.compiler}",
            cwd=f"{roms_dir}/Work",
            capture_output=True,
            text=True,
            shell=True,
        )

        self.mock_subprocess_run.assert_any_call(
            f"make COMPILER={cstar_system.environment.compiler}",
            cwd=f"{roms_dir}/Tools-Roms",
            capture_output=True,
            text=True,
            shell=True,
        )

    def test_make_nhmg_failure(self, roms_base_model, tmp_path):
        """Test that the get method raises an error when 'make nhmg' fails."""

        ## There are two subprocess calls, we'd like one fail, one pass:
        self.mock_subprocess_run.side_effect = [
            mock.Mock(
                returncode=1, stderr="Compiling NHMG library failed successfully"
            ),  # Fail nhmg
            mock.Mock(returncode=0),  # Success for Tools-Roms (won't be reached)
        ]

        # Test
        with pytest.raises(
            RuntimeError,
            match="Error 1 when compiling ROMS' NHMG library. STDERR stream: \n Compiling NHMG library failed successfully",
        ):
            roms_base_model.get(target=tmp_path)

        # Assertions:
        ## Check that subprocess.run was called only once due to failure
        assert self.mock_subprocess_run.call_count == 1

    def test_make_tools_roms_failure(self, roms_base_model, tmp_path):
        """Test that the get method raises an error when 'make Tools-Roms' fails."""

        # Simulate success for `make nhmg` and failure for `make Tools-Roms`
        self.mock_subprocess_run.side_effect = [
            mock.Mock(returncode=0),  # Success for nhmg
            mock.Mock(
                returncode=1,
                stderr="Error 1 when compiling Tools-Roms. STDERR stream: \n Compiling Tools-Roms failed successfully",
            ),  # Fail Tools-Roms
        ]

        with pytest.raises(
            RuntimeError, match=" Compiling Tools-Roms failed successfully"
        ):
            roms_base_model.get(target=tmp_path)

        # Check that subprocess.run was called twice
        assert self.mock_subprocess_run.call_count == 2
