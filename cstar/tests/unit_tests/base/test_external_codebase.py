import logging
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.external_codebase import ExternalCodeBase

################################################################################


class MockExternalCodeBase(ExternalCodeBase):
    """A mock subclass of the `ExternalCodeBase` abstract base class used for testing
    purposes.
    """

    def __init__(self, log: logging.Logger):
        super().__init__(None, None)
        self._log = log

    @property
    def expected_env_var(self):
        return "TEST_ROOT"

    @property
    def _default_source_repo(self):
        return "https://github.com/test/repo.git"

    @property
    def _default_checkout_target(self):
        return "test_target"

    def get(self, target_dir: Path | None = Path("mock_default_target_dir")):
        self.log.info(f"mock installing ExternalCodeBase at {target_dir}")
        pass

    def _configure(self):
        pass

    @property
    def is_configured(self):
        pass


@pytest.fixture
def generic_codebase(log: logging.Logger):
    """Yields a generic codebase (instance of MockExternalCodeBase defined above) for
    use in testing.
    """
    # Correctly patch the imported _get_hash_from_checkout_target in the ExternalCodeBase's module
    with mock.patch(
        "cstar.base.external_codebase._get_hash_from_checkout_target",
        return_value="test123",
    ):
        yield MockExternalCodeBase(log=log)


def test_codebase_str(generic_codebase):
    """Test the string representation of the `ExternalCodeBase` class.

    Fixtures
    --------
    generic_codebase : MockExternalCodeBase
        A mock instance of `ExternalCodeBase` with a predefined environment and repository configuration.

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
        "MockExternalCodeBase\n"
        "--------------------\n"
        "source_repo : https://github.com/test/repo.git (default)\n"
        "checkout_target : test_target (corresponding to hash test123) (default)\n"
    )

    # Compare the actual result with the expected result
    assert expected_str in str(generic_codebase)

    # Assuming generic_codebase is an instance of a class
    with mock.patch.object(
        type(generic_codebase), "local_config_status", new_callable=mock.PropertyMock
    ) as mock_local_config_status:
        mock_local_config_status.return_value = 0
        assert (
            "(Environment variable TEST_ROOT is present, points to the correct repository remote, and is checked out at the correct hash)"
            in str(generic_codebase)
        )

        mock_local_config_status.return_value = 1
        assert (
            "(Environment variable TEST_ROOT is present but does not point to the correct repository remote [unresolvable])"
            in str(generic_codebase)
        )

        # Change the return value again
        mock_local_config_status.return_value = 2
        assert (
            "(Environment variable TEST_ROOT is present, points to the correct repository remote, but is checked out at the wrong hash)"
            in str(generic_codebase)
        )

        # Final test with return value 3
        mock_local_config_status.return_value = 3
        assert (
            "(Environment variable TEST_ROOT is not present and it is assumed the external codebase is not installed locally)"
            in str(generic_codebase)
        )


def test_codebase_repr(generic_codebase):
    """Test the repr representation of the `ExternalCodeBase` class."""
    result_repr = repr(generic_codebase)
    expected_repr = (
        "MockExternalCodeBase("
        + "\nsource_repo = 'https://github.com/test/repo.git',"
        + "\ncheckout_target = 'test_target'"
        + "\n)"
        + "\nState: <local_config_status = 3>"
    )

    assert result_repr == expected_repr
    pass


class TestExternalCodeBaseConfig:
    """Unit tests for calculating the configuration status of ExternalCodeBase.

    This test suite evaluates the `local_config_status` property of `ExternalCodeBase` (mocked by
    `MockExternalCodeBase`) to confirm correct repository and environment variable configurations.

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
    generic_codebase : MockExternalCodeBase
        Provides a mock instance of `ExternalCodeBase` for use in the tests.

    Mocks
    -----
    patch_get_repo_remote : MagicMock
        Mocks `cstar.utils._get_repo_remote` function to simulate different repository remotes.
    patch_get_repo_head_hash : MagicMock
        Mocks `cstar.utils._get_repo_head_hash` function to simulate different repository head hashes.
    patch_environment_variables : MagicMock
        Mocks `cstar_sysmgr.environment.environment_variables` to control the environment variables.
    """

    def setup_method(self):
        self.patch_environment = mock.patch(
            "cstar.system.manager.CStarSystemManager.environment",
            new_callable=mock.PropertyMock,
            return_value=mock.Mock(
                environment_variables={"TEST_ROOT": "/path/to/repo"}
            ),
        )
        self.mock_environment = self.patch_environment.start()

        self.patch_get_repo_remote = mock.patch(
            "cstar.base.external_codebase._get_repo_remote"
        )
        self.mock_get_repo_remote = self.patch_get_repo_remote.start()

        self.patch_get_repo_head_hash = mock.patch(
            "cstar.base.external_codebase._get_repo_head_hash"
        )
        self.mock_get_repo_head_hash = self.patch_get_repo_head_hash.start()

    def teardown_method(self):
        self.patch_environment.stop()
        self.patch_get_repo_remote.stop()
        self.patch_get_repo_head_hash.stop()

    def test_local_config_status_valid(self, generic_codebase):
        # Set return values for other mocks
        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "test123"

        # Assert local_config_status logic
        assert generic_codebase.local_config_status == 0
        assert generic_codebase.is_setup

    def test_local_config_status_wrong_remote(self, generic_codebase):
        self.mock_get_repo_remote.return_value = (
            "https://github.com/test/wrong_repo.git"
        )

        assert generic_codebase.local_config_status == 1

    def test_local_config_status_wrong_checkout(self, generic_codebase):
        self.mock_get_repo_remote.return_value = "https://github.com/test/repo.git"
        self.mock_get_repo_head_hash.return_value = "wrong123"

        assert generic_codebase.local_config_status == 2

    def test_local_config_status_no_env_var(self, generic_codebase):
        self.mock_environment.return_value.environment_variables = {}
        assert generic_codebase.local_config_status == 3
