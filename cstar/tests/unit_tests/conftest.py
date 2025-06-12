import logging
import pathlib
from typing import Callable, Generator
from unittest import mock

import pytest

from cstar.base.log import get_logger
from cstar.system.manager import cstar_sysmgr


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
def default_xxx_root_var() -> str:
    """Fixture to create the default key for the TEST_ROOT environment variable."""
    return "TEST_ROOT"


@pytest.fixture
def default_xxx_root_value() -> str:
    """Fixture to create the default value for the TEST_ROOT environment variable."""
    return "/path/to/repo"


@pytest.fixture
def system_dotenv_path(
    mock_system_name: str, system_dotenv_dir: pathlib.Path
) -> pathlib.Path:
    # A path to a temporary, system-level environment configuration file
    if not system_dotenv_dir.exists():
        system_dotenv_dir.mkdir(parents=True)

    return system_dotenv_dir / f"{mock_system_name}.env"


@pytest.fixture
def mock_lmod_filename() -> str:
    """Fixture to provide a default .lmod filename for tests."""
    return "mock.lmod"


@pytest.fixture
def mock_lmod_path(tmp_path: pathlib.Path, mock_lmod_filename: str) -> pathlib.Path:
    """Fixture to mock the existence of an Lmod configuration file."""
    path = tmp_path / mock_lmod_filename
    path.touch()  # CStarEnvironment expects the file to exist & opens it
    return path


@pytest.fixture
def default_user_env_vars(
    default_xxx_root_var: str, default_xxx_root_value: str
) -> dict[str, str]:
    """Fixture to create the default, minimum set of env vars for the user's .cstar.env
    file, which includes an XXX_ROOT variable and its value."""
    return {
        default_xxx_root_var: default_xxx_root_value,
    }


@pytest.fixture
def default_user_env(
    default_user_env_vars: dict[str, str],
) -> Generator[mock.Mock, None, None]:
    """Fixture to create and populate the mock user environment file without additional
    action in a test case."""
    # Write default environment variables so the test can add this fixture
    # parameter and do nothing else to the user environment file.
    with mock.patch.dict("os.environ", {}) as mock_def_user_env:
        for key, value in default_user_env_vars.items():
            cstar_sysmgr.environment.set_env_var(key, value)
        yield mock_def_user_env


@pytest.fixture
def custom_system_env(
    system_dotenv_path: pathlib.Path,
) -> Generator[Callable[[dict[str, str]], None], None, None]:
    def _inner(
        variables: dict[str, str],
    ) -> None:
        """Callback to parameterize the fixture and set custom system env vars."""
        for key, value in variables.items():
            cstar_sysmgr.environment.set_env_var(key, value)

    with mock.patch(
        "cstar.system.environment.CStarEnvironment.system_env_path",
        new_callable=mock.PropertyMock,
        return_value=system_dotenv_path,
    ):
        yield _inner


@pytest.fixture
def custom_user_env(
    dotenv_path: pathlib.Path,
) -> Generator[Callable[[dict[str, str]], None], None, None]:
    def _inner(
        variables: dict[str, str],
    ) -> None:
        """Callback to parameterize the fixture and set custom user env vars."""
        for key, value in variables.items():
            cstar_sysmgr.environment.set_env_var(key, value)

    with mock.patch(
        "cstar.system.environment.CStarEnvironment.user_env_path",
        new_callable=mock.PropertyMock,
        return_value=dotenv_path,
    ):
        yield _inner


@pytest.fixture
def empty_user_env() -> Generator[mock.Mock, None, None]:
    """Fixture to create and populate the mock user environment file without additional
    action in a test case."""
    # Write default environment variables so the test can add as a parameter
    # and do nothing else to the user environment file.

    with mock.patch.dict("os.environ", {}) as mock_empty_env:
        yield mock_empty_env
