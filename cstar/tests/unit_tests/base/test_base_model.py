import os
import pytest
from pathlib import Path
from unittest import mock
from cstar.base.environment import _CSTAR_ROOT
from cstar.base.base_model import BaseModel

################################################################################


# Define a mock subclass to implement the abstract methods
class MockBaseModel(BaseModel):
    @property
    def expected_env_var(self):
        return "TEST_ROOT"

    @property
    def checkout_hash(self):
        """Simulate the checkout_hash property of BaseModel.

        This is usually determined dynamically from BaseModel.checkout_target
        """
        return "test123"

    @property
    def default_source_repo(self):
        return "https://github.com/test/repo.git"

    @property
    def default_checkout_target(self):
        return "test_target"

    # Abstract methods that aren't properties:
    def _base_model_adjustments(self):
        pass

    def get(self, target: str | Path):
        print(f"mock installing BaseModel at {target}")
        pass


@pytest.fixture
def generic_base_model():
    return MockBaseModel()


def test_base_model_str(generic_base_model):
    result_str = str(generic_base_model)

    # Define the expected output
    expected_str = (
        "MockBaseModel\n"
        "-------------\n"
        "source_repo : https://github.com/test/repo.git (default)\n"
        "checkout_target : test_target (corresponding to hash test123) (default)\n"
        "local_config_status: 3 (Environment variable TEST_ROOT is not present and it is assumed the base model is not installed locally)"
    )

    # Compare the actual result with the expected result
    assert result_str == expected_str


def test_base_model_repr(generic_base_model):
    result_repr = repr(generic_base_model)
    expected_repr = (
        "MockBaseModel("
        + "\nsource_repo = 'https://github.com/test/repo.git',"
        + "\ncheckout_target = 'test_target'"
        + "\n)"
        + "\nState: <local_config_status = 3>"
    )

    assert result_repr == expected_repr
    pass


class TestBaseModelConfig:
    def setup_method(self):
        self.patch_get_repo_remote = mock.patch(
            "cstar.base.base_model._get_repo_remote"
        )
        self.mock_get_repo_remote = self.patch_get_repo_remote.start()

        self.patch_get_repo_head_hash = mock.patch(
            "cstar.base.base_model._get_repo_head_hash"
        )
        self.mock_get_repo_head_hash = self.patch_get_repo_head_hash.start()

    def teardown_method(self):
        self.patch_get_repo_remote.stop()
        self.patch_get_repo_head_hash.stop()

    @mock.patch.dict(os.environ, {"TEST_ROOT": "/path/to/repo"}, clear=True)
    def test_local_config_status_valid(self, generic_base_model):
        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "test123"

        # Assert the status is 0 when everything is correct
        assert generic_base_model.local_config_status == 0
        assert generic_base_model.is_setup

    @mock.patch.dict(os.environ, {"TEST_ROOT": "/path/to/repo"}, clear=True)
    def test_local_config_status_wrong_remote(self, generic_base_model):
        self.mock_get_repo_remote.return_value = (
            "https://github.com/test/wrong_repo.git"
        )

        assert generic_base_model.local_config_status == 1

    @mock.patch.dict(os.environ, {"TEST_ROOT": "/path/to/repo"}, clear=True)
    def test_local_config_status_wrong_checkout(self, generic_base_model):
        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "wrong123"

        assert generic_base_model.local_config_status == 2

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_local_config_status_no_env_var(self, generic_base_model):
        assert generic_base_model.local_config_status == 3


class TestBaseModelConfigHandling:
    def setup_method(self):
        self.patch_get_repo_remote = mock.patch(
            "cstar.base.base_model._get_repo_remote"
        )
        self.mock_get_repo_remote = self.patch_get_repo_remote.start()

        self.patch_get_repo_head_hash = mock.patch(
            "cstar.base.base_model._get_repo_head_hash"
        )
        self.mock_get_repo_head_hash = self.patch_get_repo_head_hash.start()

        self.patch_local_config_status = mock.patch.object(
            BaseModel, "local_config_status", new_callable=mock.PropertyMock
        )
        self.mock_local_config_status = self.patch_local_config_status.start()

        self.patch_subprocess_run = mock.patch("subprocess.run")
        self.mock_subprocess_run = self.patch_subprocess_run.start()

    def teardown_method(self):
        self.patch_local_config_status.stop()
        self.patch_subprocess_run.stop()
        self.patch_local_config_status.stop()
        self.patch_get_repo_head_hash.stop()
        self.patch_get_repo_remote.stop()

    def test_handle_config_status_valid(self, generic_base_model, capsys):
        """Test when local_config_status == 0 (correct configuration)"""
        # Mock the config status to be 0 (everything is correct)
        self.mock_local_config_status.return_value = 0

        # Call the method
        generic_base_model.handle_config_status()

        # Capture printed output and check that nothing happens
        captured = capsys.readouterr()
        assert "correctly configured. Nothing to be done" in captured.out

    def test_handle_config_status_wrong_repo(self, generic_base_model):
        """Test when local_config_status == 1 (wrong repository remote)"""
        # Mock the config status to be 1 (wrong repo)
        self.mock_local_config_status.return_value = 1

        # Simulate the wrong repository remote
        self.mock_get_repo_remote.return_value = "https://github.com/wrong/repo.git"

        # Assert that it raises an EnvironmentError
        with pytest.raises(EnvironmentError) as exception_info:
            generic_base_model.handle_config_status()

        # Check error message:
        expected_message = (
            "System environment variable 'TEST_ROOT' points to "
            "a github repository whose remote: \n 'https://github.com/wrong/repo.git' \n"
            "does not match that expected by C-Star: \n"
            "https://github.com/test/repo.git."
            "Your environment may be misconfigured."
        )

        assert str(exception_info.value) == expected_message

    @mock.patch("builtins.input", side_effect=["not y or n"])  # mock_input
    @mock.patch.dict(os.environ, {"TEST_ROOT": "/path/to/repo"}, clear=True)
    def test_handle_config_status_wrong_checkout_user_invalid(
        self, mock_input, generic_base_model, capsys
    ):
        # Assert that it raises an EnvironmentError
        self.mock_local_config_status.return_value = 2
        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "wrong123"

        # Expect StopIteration after the invalid input due to no further inputs
        with pytest.raises(StopIteration):
            generic_base_model.handle_config_status()

        expected_message = "invalid selection; enter 'y' or 'n'"
        captured = capsys.readouterr()
        assert expected_message in str(captured.out)

    @mock.patch("builtins.input", side_effect=["n"])  # mock_input
    @mock.patch.dict(os.environ, {"TEST_ROOT": "/path/to/repo"}, clear=True)
    def test_handle_config_status_wrong_checkout_user_n(
        self, mock_input, generic_base_model, capsys
    ):
        # Assert that it raises an EnvironmentError
        self.mock_local_config_status.return_value = 2
        self.mock_get_repo_head_hash.return_value = "wrong123"

        with pytest.raises(EnvironmentError):
            generic_base_model.handle_config_status()

        # Capture print statements
        capsys.readouterr()

    @mock.patch("builtins.input", side_effect=["y"])  # mock_input
    @mock.patch.dict(os.environ, {"TEST_ROOT": "/path/to/repo"}, clear=True)
    def test_handle_config_status_wrong_checkout_user_y(
        self, mock_input, generic_base_model, capsys
    ):
        """Test handling when local_config_status == 2 (right remote, wrong hash) and
        user agrees to checkout."""

        self.mock_local_config_status.return_value = 2

        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "wrong123"

        # Call the method to trigger the flow
        generic_base_model.handle_config_status()

        ## Assert that subprocess.run was called with the correct git checkout command
        self.mock_subprocess_run.assert_called_with(
            "git -C /path/to/repo checkout test_target", shell=True
        )

        self.mock_subprocess_run.assert_called_once()

        # Check that the prompt for user input was shown
        captured = capsys.readouterr()

        expected_message = (
            "############################################################\n"
            + "C-STAR: TEST_ROOT points to the correct repo "
            + "https://github.com/test/repo.git but HEAD is at: \n"
            + "wrong123, rather than the hash associated with "
            + "checkout_target test_target:\n"
            + "test123"
            + "\n############################################################"
        )
        assert str(captured.out).strip() == expected_message

    @mock.patch("builtins.input", side_effect=["y"])  # mock_input
    def test_handle_config_status_no_env_var_user_y(
        self, mock_input, generic_base_model, capsys
    ):
        self.mock_local_config_status.return_value = 3

        generic_base_model.handle_config_status()

        expected_install_dir = Path(_CSTAR_ROOT) / "externals/repo"

        # Verify that 'get' (defined above)  is called when user inputs 'y':
        captured = capsys.readouterr()
        assert (f"mock installing BaseModel at {expected_install_dir}") in str(
            captured.out
        )

    @mock.patch("builtins.input", side_effect=["n"])  # mock_input
    def test_handle_config_status_no_env_var_user_n(
        self, mock_input, generic_base_model, capsys
    ):
        self.mock_local_config_status.return_value = 3

        with pytest.raises(EnvironmentError):
            generic_base_model.handle_config_status()

    @mock.patch("builtins.input", side_effect=["not y or n"])  # mock_input
    def test_handle_config_status_no_env_var_user_invalid(
        self, mock_input, generic_base_model, capsys
    ):
        self.mock_local_config_status.return_value = 3

        # Expect StopIteration after the invalid input due to no further inputs
        with pytest.raises(StopIteration):
            generic_base_model.handle_config_status()

        expected_message = "invalid selection; enter 'y','n',or 'custom'"
        captured = capsys.readouterr()
        assert expected_message in str(captured.out)
