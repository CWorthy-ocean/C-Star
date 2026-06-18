import os
from unittest import mock
from unittest.mock import patch

import pytest

from cstar.system.manager import HostNameEvaluator

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"


@patch("platform.machine", return_value=DEFAULT_MOCK_MACHINE_NAME)
@patch("platform.system", return_value=DEFAULT_MOCK_HOST_NAME)
@patch.dict(os.environ, {}, clear=True)
def test_no_lmod_in_env(
    mock_system: mock.MagicMock,
    mock_machine: mock.MagicMock,
) -> None:
    """Verify that an environment without lmod environment variables returns a platform-
    based name.
    """
    namer = HostNameEvaluator()

    # confirm the platform values were retrieved during init
    mock_system.assert_called_once()
    mock_machine.assert_called_once()

    actual = namer.platform_hostname

    assert namer.platform_name == DEFAULT_MOCK_HOST_NAME
    assert namer.machine_name == DEFAULT_MOCK_MACHINE_NAME

    expected = f"{namer.platform_name}_{namer.machine_name}"

    assert actual == expected
    assert namer.name == namer.platform_hostname


@patch("platform.machine", mock.Mock(return_value="x86_64"))
@patch("platform.system", mock.Mock(return_value="Linux"))
@patch.dict(os.environ, {}, clear=True)
def test_known_linux_platform() -> None:
    """Verify that known values for a linux platform result in the correct name.

    NOTE: this test also confirms that the platform name is case-folded.
    """
    namer = HostNameEvaluator()

    expected_name = f"{namer.platform_name}_{namer.machine_name}".casefold()

    assert namer.platform_name == "Linux"
    assert namer.machine_name == "x86_64"

    assert namer.platform_hostname == expected_name
    assert namer.name == namer.platform_hostname


@pytest.mark.usefixtures("env_full_lmod")
@patch("platform.machine", mock.Mock(return_value=DEFAULT_MOCK_MACHINE_NAME))
@patch("platform.system", mock.Mock(return_value=DEFAULT_MOCK_HOST_NAME))
def test_lmod_prioritizes_syshost() -> None:
    """Verify that the name resolver prioritizes the lmod-hostname over the
    platform machine name and system name.
    """
    namer = HostNameEvaluator()

    # sanity check platform mocking is correct
    assert namer.machine_name == DEFAULT_MOCK_MACHINE_NAME
    assert namer.platform_name == DEFAULT_MOCK_HOST_NAME

    assert namer.name == namer.lmod_settings.lmod_hostname


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


@pytest.mark.usefixtures("env_clear_lmod")
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
