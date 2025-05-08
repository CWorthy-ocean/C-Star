import logging
import pathlib

import pytest

from cstar.base.log import get_logger


@pytest.fixture
def log() -> logging.Logger:
    return get_logger("cstar.tests.unit_tests")


@pytest.fixture
def dotenv_path(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary user environment configuration file
    return tmp_path / ".cstar.env"


@pytest.fixture
def marbl_path(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary directory for writing the marbl code
    return tmp_path / "marbl"


@pytest.fixture
def roms_path(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary directory for writing the roms code
    return tmp_path / "roms"


@pytest.fixture
def system_dotenv_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    # A path to a temporary directory for writing system-level
    # environment configuration file
    return tmp_path / "additional_files" / "env_files"


@pytest.fixture
def mock_system_name() -> str:
    # A name for the mock system/platform executing the tests.
    return "mock_system"


@pytest.fixture
def system_dotenv_path(
    mock_system_name: str, system_dotenv_dir: pathlib.Path
) -> pathlib.Path:
    # A path to a temporary, system-level environment configuration file
    if not system_dotenv_dir.exists():
        system_dotenv_dir.mkdir(parents=True)

    return system_dotenv_dir / f"{mock_system_name}.env"
