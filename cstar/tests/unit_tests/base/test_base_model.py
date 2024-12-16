import pytest
from pathlib import Path
from unittest import mock
from cstar.base.system import cstar_system
from cstar.base.base_model import BaseModel

################################################################################


class MockBaseModel(BaseModel):
    """A mock subclass of the `BaseModel` abstract base class used for testing
    purposes."""

    @property
    def expected_env_var(self):
        return "TEST_ROOT"

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
    """Yields a generic base model (instance of MockBaseModel defined above) for use in
    testing."""
    # Correctly patch the imported _get_hash_from_checkout_target in the BaseModel's module
    with mock.patch(
        "cstar.base.base_model._get_hash_from_checkout_target", return_value="test123"
    ):
        yield MockBaseModel()


def test_base_model_str(generic_base_model):
    """Test the string representation of the `BaseModel` class.

    Fixtures
    --------
    generic_base_model : MockBaseModel
        A mock instance of `BaseModel` with a predefined environment and repository configuration.

    Mocks
    -----
    local_config_status : PropertyMock
        Mocked to test different states of the local configuration, such as valid,
        wrong repo, right repo/wrong hash, and repo not found.

    Asserts
    -------
    str
        Verifies that the expected string output matches the actual string representation
        under various configurations of `local_config_status`.
    """
    # Define the expected output
    expected_str = (
        "MockBaseModel\n"
        "-------------\n"
        "source_repo : https://github.com/test/repo.git (default)\n"
        "checkout_target : test_target (corresponding to hash test123) (default)\n"
    )

    # Compare the actual result with the expected result
    assert expected_str in str(generic_base_model)

    # Assuming generic_base_model is an instance of a class
    with mock.patch.object(
        type(generic_base_model), "local_config_status", new_callable=mock.PropertyMock
    ) as mock_local_config_status:
        mock_local_config_status.return_value = 0
        assert (
            "(Environment variable TEST_ROOT is present, points to the correct repository remote, and is checked out at the correct hash)"
            in str(generic_base_model)
        )

        mock_local_config_status.return_value = 1
        assert (
            "(Environment variable TEST_ROOT is present but does not point to the correct repository remote [unresolvable])"
            in str(generic_base_model)
        )

        # Change the return value again
        mock_local_config_status.return_value = 2
        assert (
            "(Environment variable TEST_ROOT is present, points to the correct repository remote, but is checked out at the wrong hash)"
            in str(generic_base_model)
        )

        # Final test with return value 3
        mock_local_config_status.return_value = 3
        assert (
            "(Environment variable TEST_ROOT is not present and it is assumed the base model is not installed locally)"
            in str(generic_base_model)
        )


def test_base_model_repr(generic_base_model):
    """Test the repr representation of the `BaseModel` class."""

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
    """Unit tests for calculating the configuration status of BaseModel.

    This test suite evaluates the `local_config_status` property of `BaseModel` (mocked by
    `MockBaseModel`) to confirm correct repository and environment variable configurations.

    Tests
    -----
    test_local_config_status_valid
        Verifies that `local_config_status` is 0 when the configuration is valid.
    test_local_config_status_wrong_remote
        Checks that `local_config_status` is 1 when the repository remote URL is incorrect.
    test_local_config_status_wrong_checkout
        Confirms that `local_config_status` is 2 when the repository checkout hash is incorrect.
    test_local_config_status_no_env_var
        Ensures that `local_config_status` is 3 when the required environment variable is missing.

    Fixtures
    --------
    generic_base_model : MockBaseModel
        Provides a mock instance of `BaseModel` for use in the tests.

    Mocks
    -----
    patch_get_repo_remote : MagicMock
        Mocks `cstar.utils._get_repo_remote` function to simulate different repository remotes.
    patch_get_repo_head_hash : MagicMock
        Mocks `cstar.utils._get_repo_head_hash` function to simulate different repository head hashes.
    patch_environment_variables : MagicMock
        Mocks `cstar_system.environment.environment_variables` to control the environment variables.
    """

    def setup_method(self):
        self.patch_environment = mock.patch(
            "cstar.base.system.CStarSystem.environment",
            new_callable=mock.PropertyMock,
            return_value=mock.Mock(
                environment_variables={"TEST_ROOT": "/path/to/repo"}
            ),
        )
        self.mock_environment = self.patch_environment.start()

        self.patch_get_repo_remote = mock.patch(
            "cstar.base.base_model._get_repo_remote"
        )
        self.mock_get_repo_remote = self.patch_get_repo_remote.start()

        self.patch_get_repo_head_hash = mock.patch(
            "cstar.base.base_model._get_repo_head_hash"
        )
        self.mock_get_repo_head_hash = self.patch_get_repo_head_hash.start()

    def teardown_method(self):
        self.patch_environment.stop()
        self.patch_get_repo_remote.stop()
        self.patch_get_repo_head_hash.stop()

    def test_local_config_status_valid(self, generic_base_model):
        # Set return values for other mocks
        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "test123"

        # Assert local_config_status logic
        assert generic_base_model.local_config_status == 0
        assert generic_base_model.is_setup

    def test_local_config_status_wrong_remote(self, generic_base_model):
        self.mock_get_repo_remote.return_value = (
            "https://github.com/test/wrong_repo.git"
        )

        assert generic_base_model.local_config_status == 1

    def test_local_config_status_wrong_checkout(self, generic_base_model):
        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "wrong123"

        assert generic_base_model.local_config_status == 2

    def test_local_config_status_no_env_var(self, generic_base_model):
        self.mock_environment.return_value.environment_variables = {}
        assert generic_base_model.local_config_status == 3


class TestBaseModelConfigHandling:
    """Unit tests for handling various configuration statuses in `BaseModel`.

    This suite evaluates `BaseModel`'s `handle_config_status` method by simulating different
    environment and repository states. Each test checks the behavior of `handle_config_status`
    under varying `local_config_status` values, user input responses, and repository setups.

    Tests
    -----
    test_handle_config_status_valid
        Confirms that no action is taken when the configuration is valid (local_config_status == 0).
    test_handle_config_status_wrong_repo
        Ensures an `EnvironmentError` is raised for incorrect repository remote (local_config_status == 1).
    test_handle_config_status_wrong_checkout_user_invalid
        Simulates an invalid user response when prompted to correct the checkout (local_config_status == 2).
    test_handle_config_status_wrong_checkout_user_n
        Confirms that an `EnvironmentError` is raised when user opts not to correct an incorrect checkout (local_config_status == 2).
    test_handle_config_status_wrong_checkout_user_y
        Verifies that the system attempts to correct the checkout when the user agrees (local_config_status == 2).
    test_handle_config_status_no_env_var_user_y
        Checks that `get` method is called to install `BaseModel` when the associated environment variable is missing (local_config_status == 3) and user opts to proceed.
    test_handle_config_status_no_env_var_user_n
        Ensures that an `EnvironmentError` is raised when user declines to set up a missing BaseModel (local_config_status == 3).
    test_handle_config_status_no_env_var_user_invalid
        Simulates an invalid user response when prompted to set up the BaseModel (local_config_status == 3).
    test_handle_config_status_no_env_var_user_custom
        Confirms that `BaseModel` installs to a custom path when user specifies a custom directory (local_config_status == 3).

    Fixtures
    --------
    generic_base_model : MockBaseModel
        Provides a mock instance of `BaseModel` for testing configuration handling.

    Mocks
    -----
    patch_get_repo_remote : MagicMock
        Mocks `cstar.utils._get_repo_remote` to control the repository remote URL.
    patch_get_repo_head_hash : MagicMock
        Mocks `cstar.utils._get_repo_head_hash` to control the repository head hash.
    patch_local_config_status : MagicMock
        Mocks `BaseModel.local_config_status` to simulate different configuration states.
    patch_subprocess_run : MagicMock
        Mocks `subprocess.run` to simulate command-line actions for `git checkout`.
    patch_environment : MagicMock
        Mocks `cstar_system.environment.environment_variables` to control the environment variables.
    """

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

        self.patch_environment = mock.patch(
            "cstar.base.system.CStarSystem.environment",
            new_callable=mock.PropertyMock,
            return_value=mock.Mock(
                environment_variables={"TEST_ROOT": "/path/to/repo"},
                package_root=Path("/mock/package/root"),
            ),
        )
        self.mock_environment = self.patch_environment.start()

    def teardown_method(self):
        self.patch_local_config_status.stop()
        self.patch_subprocess_run.stop()
        self.patch_local_config_status.stop()
        self.patch_get_repo_head_hash.stop()
        self.patch_get_repo_remote.stop()
        self.patch_environment.stop()

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

        expected_install_dir = (
            Path(cstar_system.environment.package_root) / "externals/repo"
        )

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

    @mock.patch(
        "builtins.input", side_effect=["custom", "some/install/path"]
    )  # mock_input
    def test_handle_config_status_no_env_var_user_custom(
        self, mock_input, generic_base_model, capsys
    ):
        self.mock_local_config_status.return_value = 3
        generic_base_model.handle_config_status()
        expected_install_dir = Path("some/install/path").resolve()

        captured = capsys.readouterr()
        assert (f"mock installing BaseModel at {expected_install_dir}") in str(
            captured.out
        )
