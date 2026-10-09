from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


@pytest.fixture(scope="session")
def package_path() -> Path:
    """Fixture that returns the path to the repository root directory.

    Returns
    -------
    Path
    """
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="session")
def tests_path(package_path: Path) -> Path:
    """Fixture that returns the path to the directory containing C-Star tests.

    Returns
    -------
    Path
    """
    return package_path / "cstar" / "tests"


@pytest.fixture(scope="session", autouse=True)
def skip_html_checks_for_performance() -> Generator[None, None, None]:
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


@pytest.fixture
def mock_lmod_filename() -> str:
    """Provide a unique .lmod filename for tests.

    Returns
    -------
    str
        The filename
    """
    return "mock.lmod"


@pytest.fixture
def mock_lmod_path(tmp_path: Path, mock_lmod_filename: str) -> Path:
    """Create an empty, temporary .lmod file and return the path.

    Parameters
    ----------
    tmp_path : Path
        The path to a temporary location to write the lmod file
    mock_lmod_filename : str
        The filename to use for the .lmod file

    Returns
    -------
    str
        The complete path to the file
    """
    tmp_path.mkdir(parents=True, exist_ok=True)

    path = tmp_path / mock_lmod_filename
    path.touch()  # CStarEnvironment expects the file to exist & opens it
    return path
