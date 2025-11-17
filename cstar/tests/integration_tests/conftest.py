import logging
from pathlib import Path

import pytest

from cstar.base.log import get_logger
from cstar.tests.integration_tests.blueprints.fixtures import (
    modify_template_blueprint,  # noqa : F401  # noqa : F401
)
from cstar.tests.integration_tests.fixtures import (
    fetch_remote_test_case_data,  # noqa: F401  # noqa: F401
)


@pytest.fixture
def log() -> logging.Logger:
    return get_logger("cstar.tests.integration_tests")


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
