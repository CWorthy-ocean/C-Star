import os
from collections.abc import Generator
from unittest import mock
from unittest.mock import patch

import pytest

from cstar.system.manager import HostNameEvaluator

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"


@pytest.fixture
def env_full_lmod() -> Generator[mock._patch, None, None]:
    """Configure environment variables to have both lmod sys host and sys name."""
    lmod_syshost = "sys-host"
    lmod_sysname = "sys-name"
    with patch.dict(
        os.environ,
        {
            HostNameEvaluator.ENV_LMOD_SYSHOST: lmod_syshost,
            HostNameEvaluator.ENV_LMOD_SYSNAME: lmod_sysname,
        },
    ) as _:
        yield _


@pytest.fixture
def env_no_lmod() -> Generator[mock._patch, None, None]:
    """Configure environment variables to omit both lmod sys host and sys name."""
    with patch.dict(
        os.environ,
        {},
        clear=True,
    ) as _:
        yield _


@patch("platform.machine", return_value=DEFAULT_MOCK_MACHINE_NAME)
@patch("platform.system", return_value=DEFAULT_MOCK_HOST_NAME)
@patch.dict(os.environ, {}, clear=True)
def test_no_lmod_in_env(
    mock_system: mock.MagicMock, mock_machine: mock.MagicMock
) -> None:
    """Verify that an environment without lmod environment variables returns a platform-
    based name.
    """
    namer = HostNameEvaluator()

    mock_system.assert_called_once()
    mock_machine.assert_called_once()

    assert namer.lmod_syshost == ""
    assert namer.lmod_sysname == ""
    assert namer.platform_name == DEFAULT_MOCK_HOST_NAME
    assert namer.machine_name == DEFAULT_MOCK_MACHINE_NAME
    assert namer.lmod_hostname == ""
    assert namer.platform_hostname == f"{namer.platform_name}_{namer.machine_name}"
    assert namer.name == namer.platform_hostname


@patch("platform.machine", return_value="x86_64")
@patch("platform.system", return_value="Linux")
@patch.dict(os.environ, {}, clear=True)
def test_known_linux_platform(
    mock_system: mock.MagicMock, mock_machine: mock.MagicMock
) -> None:
    """Verify that known values for a linux platform result in the correct name.

    NOTE: this test also confirms that the platform name is case-folded.
    """
    namer = HostNameEvaluator()

    mock_system.assert_called_once()
    mock_machine.assert_called_once()

    expected_name = f"{namer.platform_name}_{namer.machine_name}".casefold()

    assert namer.lmod_syshost == ""
    assert namer.lmod_sysname == ""
    assert namer.platform_name == "Linux"
    assert namer.machine_name == "x86_64"
    assert namer.lmod_hostname == ""
    assert namer.platform_hostname == expected_name
    assert namer.name == namer.platform_hostname


@pytest.mark.parametrize(
    ("lmod_syshost", "lmod_sysname"),
    [
        pytest.param("", "sys-name", id="No syshost"),
        pytest.param("sys-host", "", id="No sysname"),
        pytest.param("Expanse", "", id="Expanse (actual)"),
        pytest.param("", "Perlmutter", id="Perlmutter (actual)"),
    ],
)
@patch("platform.machine", return_value=DEFAULT_MOCK_MACHINE_NAME)
@patch("platform.system", return_value=DEFAULT_MOCK_HOST_NAME)
def test_partial_lmod_results_in_lmod_name(
    mock_system: mock.MagicMock,
    mock_machine: mock.MagicMock,
    lmod_syshost: str,
    lmod_sysname: str,
) -> None:
    """Verify that an environment specifying incomplete lmod variables returns the value
    that is set.
    """
    with patch.dict(
        os.environ,
        {
            HostNameEvaluator.ENV_LMOD_SYSHOST: lmod_syshost,
            HostNameEvaluator.ENV_LMOD_SYSNAME: lmod_sysname,
        },
    ):
        namer = HostNameEvaluator()

    mock_system.assert_called_once()
    mock_machine.assert_called_once()

    assert namer.lmod_syshost == lmod_syshost
    assert namer.lmod_sysname == lmod_sysname
    if lmod_syshost:
        expected_name = namer.lmod_syshost.casefold()
        assert namer.lmod_hostname == expected_name
    elif lmod_sysname:
        expected_name = namer.lmod_sysname.casefold()
        assert namer.lmod_hostname == expected_name
    assert namer.name == namer.lmod_hostname


@pytest.mark.usefixtures("env_full_lmod")
@patch("platform.machine", return_value=DEFAULT_MOCK_MACHINE_NAME)
@patch("platform.system", return_value=DEFAULT_MOCK_HOST_NAME)
def test_lmod_prioritizes_syshost(
    mock_system: mock.MagicMock,
    mock_machine: mock.MagicMock,
) -> None:
    """Verify that the an when both LMOD env vars are set, the value from sys host is
    prioritized as the lmod name.
    """
    namer = HostNameEvaluator()

    mock_system.assert_called_once()
    mock_machine.assert_called_once()

    assert namer.lmod_hostname == namer.lmod_syshost
    assert namer.name == namer.lmod_hostname


@pytest.mark.usefixtures("env_full_lmod")
@pytest.mark.parametrize(
    ("system_name", "machine_name"),
    [
        pytest.param("", "system-name", id="No system name"),
        pytest.param("machine-name", "", id="No machine name"),
    ],
)
def test_partial_platform_naming(
    system_name: str,
    machine_name: str,
) -> None:
    """Verify that when the system name and machine name are not both found, the
    platform name is empty.
    """
    with (
        patch("platform.system", return_value=system_name),
        patch("platform.machine", return_value=machine_name),
    ):
        namer = HostNameEvaluator()

    assert namer.platform_name == system_name
    assert namer.machine_name == machine_name
    assert namer.platform_hostname == ""

    # partial platform info shouldn't affect overall name when lmod is available
    assert namer.name


@pytest.mark.usefixtures("env_no_lmod")
@pytest.mark.parametrize(
    ("system_name", "machine_name"),
    [
        pytest.param("", "system-name", id="No system name"),
        pytest.param("machine-name", "", id="No machine name"),
    ],
)
def test_partial_platform_fallback(
    system_name: str,
    machine_name: str,
) -> None:
    """Verify that when there is no lmod information and the system name and machine
    name are not both found, accessing the name fails.
    """
    with (
        patch("platform.system", return_value=system_name),
        patch("platform.machine", return_value=machine_name),
    ):
        namer = HostNameEvaluator()

    assert namer.platform_name == system_name
    assert namer.machine_name == machine_name
    assert namer.platform_hostname == ""

    # without lmod env vars, falling back on partial platform name should fail.
    with pytest.raises(EnvironmentError):
        _ = namer.name
