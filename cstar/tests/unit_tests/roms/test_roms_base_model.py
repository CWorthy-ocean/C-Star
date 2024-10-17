import os
import pytest
from unittest import mock
from cstar.roms.base_model import ROMSBaseModel
from cstar.base.environment import _CSTAR_COMPILER


@pytest.fixture
def roms_base_model():
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
    """Test cases for ROMSBaseModel.get method."""

    def setup_method(self):
        """Common setup before each test method."""
        # Mock subprocess, _clone_and_checkout, and _write_to_config_file
        self.mock_subprocess_run = mock.patch("subprocess.run").start()
        self.mock_clone_and_checkout = mock.patch(
            "cstar.roms.base_model._clone_and_checkout"
        ).start()
        self.mock_write_to_config_file = mock.patch(
            "cstar.roms.base_model._write_to_config_file"
        ).start()

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

        ## Check that _write_to_config_file was (mock) called correctly
        config_file_str = (
            f'    _CSTAR_ENVIRONMENT_VARIABLES["ROMS_ROOT"]="{roms_dir}"'
            + '\n    _CSTAR_ENVIRONMENT_VARIABLES.setdefault("PATH",os.environ.get("PATH",default=""))'
            + '\n    _CSTAR_ENVIRONMENT_VARIABLES["PATH"]+=":'
            + f'{roms_dir}/Tools-Roms"\n'
        )
        self.mock_write_to_config_file.assert_called_once_with(config_file_str)

        ## Check that subprocess.run was (mock) called twice for `make nhmg` and `make Tools-Roms`
        print(self.mock_subprocess_run.call_args_list)
        assert self.mock_subprocess_run.call_count == 2

        self.mock_subprocess_run.assert_any_call(
            f"make nhmg COMPILER={_CSTAR_COMPILER}",
            cwd=f"{roms_dir}/Work",
            capture_output=True,
            text=True,
            shell=True,
        )

        self.mock_subprocess_run.assert_any_call(
            f"make COMPILER={_CSTAR_COMPILER}",
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
