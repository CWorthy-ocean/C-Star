import logging
import pathlib
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest

from cstar.base.log import get_logger


@pytest.fixture
def log() -> logging.Logger:
    """Return the logger for logging during tests."""
    return get_logger("cstar.tests.unit_tests")


@pytest.fixture
def dotenv_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path to a temporary user environment configuration file."""
    return tmp_path / ".cstar.env"


@pytest.fixture
def marbl_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path to a temporary directory for writing the marbl code."""
    return tmp_path / "marbl"


@pytest.fixture
def roms_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path to a temporary directory for writing the roms code."""
    return tmp_path / "roms"


@pytest.fixture
def system_dotenv_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a path for writing the system-level environment configuration file."""
    return tmp_path / "additional_files" / "env_files"


@pytest.fixture
def mock_system_name() -> str:
    """Return a name for the mock system/platform executing the tests."""
    return "mock_system"


@pytest.fixture
def system_dotenv_path(
    mock_system_name: str, system_dotenv_dir: pathlib.Path
) -> pathlib.Path:
    """Return a path to a temporary, system-level environment configuration file."""
    if not system_dotenv_dir.exists():
        system_dotenv_dir.mkdir(parents=True)

    return system_dotenv_dir / f"{mock_system_name}.env"


@pytest.fixture
def mock_input_y() -> Generator[Any, Any, Any]:
    """Return a mock input that returns the value 'y'."""
    with mock.patch("builtins.input", side_effect=["y"]):
        yield


@pytest.fixture
def mock_input_n() -> Generator[Any, Any, Any]:
    """Return a mock input that returns the value 'n'."""
    with mock.patch("builtins.input", side_effect=["n"]):
        yield


@pytest.fixture
def mock_input_invalid() -> Generator[Any, Any, Any]:
    """Return a value that does not work as a valid confirm or deny."""
    with mock.patch("builtins.input", side_effect=["not y or n"]):
        yield


@pytest.fixture
def mock_input_with_custom_path() -> Generator[Any, Any, Any]:
    """Return a mock input that returns a custom sub-path."""
    with mock.patch("builtins.input", side_effect=["custom", "some/install/path"]):
        yield
