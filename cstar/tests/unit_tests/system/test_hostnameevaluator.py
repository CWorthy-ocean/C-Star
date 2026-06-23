import os
from unittest import mock
from unittest.mock import patch

import pytest

from cstar.system.environment import LmodEnvSettings
from cstar.system.manager import HostNameEvaluator

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"


@patch(
    "platform.machine",
    new_callable=lambda: mock.MagicMock(return_value=DEFAULT_MOCK_MACHINE_NAME),
)
@patch(
    "platform.system",
    new_callable=lambda: mock.MagicMock(return_value=DEFAULT_MOCK_HOST_NAME),
)
@patch.dict(os.environ, {}, clear=True)
def test_no_lmod_in_env(
    mock_system: mock.MagicMock,
    mock_machine: mock.MagicMock,
) -> None:
    """Verify that an environment without lmod environment variables returns a platform-
    based name.
    """
    namer = HostNameEvaluator()

    # confirm the platform values are retrieved when attributes are accessed.
    namer.platform_name
    mock_system.assert_called()

    namer.machine_name
    mock_machine.assert_called()

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
@pytest.mark.parametrize(
    ("host_value", "name_value", "exp_hostname"),
    [
        pytest.param("", "", "", id="empty system and host"),
        pytest.param("host-value", "", "host-value", id="no system name"),
        pytest.param("", "system-value", "system-value", id="no host name"),
        pytest.param("host-value", "system-value", "host-value", id="prioritize host"),
        pytest.param("HoSt-value", "", "host-value", id="casefold host"),
        pytest.param("", "SyStEm-VaLuE", "system-value", id="casefold system"),
        pytest.param(
            "HoSt-value", "SyStEm-VaLuE", "host-value", id="casefold prioritized host"
        ),
    ],
)
def test_lmod_prioritizes_syshost(
    host_value: str | None, name_value: str | None, exp_hostname: str
) -> None:
    """Verify LMOD hostname computatons follows expected priority order."""
    with (
        mock.patch.dict(
            os.environ,
            {
                LmodEnvSettings.variable("SYSHOST"): host_value,
                LmodEnvSettings.variable("SYSTEM_NAME"): name_value,
            },
        ),
        mock.patch("platform.machine", lambda: name_value),
        mock.patch("platform.system", lambda: host_value),
    ):
        namer = HostNameEvaluator()
        assert namer.lmod_hostname == exp_hostname


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
