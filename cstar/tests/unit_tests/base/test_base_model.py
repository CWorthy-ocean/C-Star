import os
import pytest
from unittest import mock
from cstar.base.base_model import BaseModel

################################################################################


# Define a mock subclass to implement the abstract methods
# Define a mock subclass to implement the abstract methods and required attributes
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

    def get(self):
        pass


@pytest.fixture
def generic_base_model():
    return MockBaseModel()


def test_base_model_str():
    pass


def test_base_model_repr():
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


def test_is_setup():
    pass


def test_handle_local_config_status():
    pass
