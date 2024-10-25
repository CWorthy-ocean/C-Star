import os
import pytest
from unittest import mock
from cstar.marbl.base_model import MARBLBaseModel
from cstar.base.environment import _CSTAR_COMPILER


@pytest.fixture
def marbl_base_model():
    source_repo = "https://github.com/marbl-ecosys/MARBL.git"
    checkout_target = "v0.45.0"
    return MARBLBaseModel(source_repo=source_repo, checkout_target=checkout_target)


def test_default_source_repo(marbl_base_model):
    """Test if the default source repo is set correctly."""
    assert (
        marbl_base_model.default_source_repo
        == "https://github.com/marbl-ecosys/MARBL.git"
    )


def test_default_checkout_target(marbl_base_model):
    """Test if the default checkout target is set correctly."""
    assert marbl_base_model.default_checkout_target == "v0.45.0"


def test_expected_env_var(marbl_base_model):
    """Test if the expected environment variable is set correctly."""
    assert marbl_base_model.expected_env_var == "MARBL_ROOT"


def test_defaults_are_set():
    """Test that the defaults are set correctly."""

    marbl_base_model = MARBLBaseModel()
    assert marbl_base_model.source_repo == "https://github.com/marbl-ecosys/MARBL.git"
    assert marbl_base_model.checkout_target == "v0.45.0"


class TestMARBLBaseModelGet:
    """Test cases for MARBLBaseModel.get method."""

    def setup_method(self):
        """Common setup before each test method."""
        # Mock subprocess, _clone_and_checkout, and _write_to_config_file
        self.mock_subprocess_run = mock.patch("subprocess.run").start()
        self.mock_clone_and_checkout = mock.patch(
            "cstar.marbl.base_model._clone_and_checkout"
        ).start()
        self.mock_write_to_config_file = mock.patch(
            "cstar.marbl.base_model._write_to_config_file"
        ).start()

        # Clear environment variables
        self.env_patch = mock.patch.dict(os.environ, {}, clear=True)
        self.env_patch.start()

    def teardown_method(self):
        """Common teardown after each test method."""
        mock.patch.stopall()

    def test_get_success(self, marbl_base_model, tmp_path):
        """Test that the get method succeeds when subprocess calls succeed."""
        # Setup:
        ## Make temporary target dir
        marbl_dir = tmp_path / "marbl"

        ## Mock success of calls to subprocess.run:
        self.mock_subprocess_run.return_value.returncode = 0

        # Test
        ## Call the get method
        marbl_base_model.get(target=marbl_dir)

        # Assertions:
        ## Check environment variables
        assert os.environ["MARBL_ROOT"] == str(marbl_dir)

        ## Check that _clone_and_checkout was (mock) called correctly
        self.mock_clone_and_checkout.assert_called_once_with(
            source_repo=marbl_base_model.source_repo,
            local_path=marbl_dir,
            checkout_target=marbl_base_model.checkout_target,
        )

        ## Check that _write_to_config_file was (mock) called correctly
        config_file_str = (
            f'\n    _CSTAR_ENVIRONMENT_VARIABLES["MARBL_ROOT"]="{marbl_dir}"\n'
        )
        self.mock_write_to_config_file.assert_called_once_with(config_file_str)

        self.mock_subprocess_run.assert_called_once_with(
            f"make {_CSTAR_COMPILER} USEMPI=TRUE",
            cwd=f"{marbl_dir}/src",
            capture_output=True,
            text=True,
            shell=True,
        )

    def test_make_failure(self, marbl_base_model, tmp_path):
        """Test that the get method raises an error when 'make' fails."""

        ## There are two subprocess calls, we'd like one fail, one pass:
        self.mock_subprocess_run.side_effect = [
            mock.Mock(returncode=1, stderr="Compiling MARBL failed successfully"),
        ]

        # Test
        with pytest.raises(
            RuntimeError,
            match="Error 1 when compiling MARBL. STDERR stream: \n Compiling MARBL failed successfully",
        ):
            marbl_base_model.get(target=tmp_path)
