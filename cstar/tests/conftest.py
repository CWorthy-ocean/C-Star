from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch

import pytest


@pytest.fixture(scope="session", autouse=True)
def skip_html_checks_for_performance():
    """
    Global auto-use that skips the head checks used to see if HTTP input datasets are HTML.
    For our test cases, they are always files, except for when we explicitly test that they,
    aren't, and those tests apply patches on top of this one.

    This is purely to speed up tests.
    """
    fake_response = Mock()
    fake_response.headers = {"Content-Type": "application/octet-stream"}
    with patch("cstar.io.source_data.requests.head", return_value=fake_response):
        yield


@pytest.fixture(scope="session")
def mock_user_env_name() -> str:
    """Return a unique name for a temporary user .env config file.

    Returns
    -------
    str
        The name of the .env file
    """
    return ".mock.env"


@pytest.fixture(autouse=True)
def never_touch_user_env(tmp_path: Path, mock_user_env_name: str):
    """Autouse fixture to always replace user env path with a temp path"""
    with patch(
        "cstar.system.environment.CStarEnvironment.user_env_path",
        new_callable=PropertyMock,
        return_value=tmp_path / mock_user_env_name,
    ):
        yield
