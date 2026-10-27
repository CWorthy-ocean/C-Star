from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, ENV_CSTAR_SYSTEM
from cstar.cli.environment.show import app
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
)
from cstar.system.manager import MacOSSystemContext, SystemSelection, SystemSource


@pytest.mark.usefixtures("mock_env")
@pytest.mark.asyncio
async def test_cli_env_show() -> None:
    """Verify that CLI env show command produces output and omits the internal group."""
    runner = CliRunner()

    result = runner.invoke(
        app,
        [],
        color=False,
    )
    assert "CSTAR_LOG_LEVEL:" in result.stdout
    assert "CSTAR_RUNID:" not in result.stdout
    assert "CSTAR_:" not in result.stdout
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_internal_group() -> None:
    """Verify that requesting the internal group by name displays its variables."""
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--group", "internal"],
        color=False,
    )

    assert "CSTAR_RUNID:" in result.stdout
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_export_omits_internal(
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify that export statements leave out the internal group but still
    include a setting that is not persistable.

    Parameters
    ----------
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--display", "export"],
        color=False,
    )

    assert "CSTAR_RUNID" not in result.stdout
    assert "export CSTAR_CONFIG_HOME=" in flatten_cli_output(result.stdout)
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_internal_export_is_empty(
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify that exporting the internal group says it is never exported.

    Parameters
    ----------
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--group", "internal", "--display", "export"],
        color=False,
    )

    assert "CSTAR_RUNID" not in result.stdout
    assert "The internal group is never exported" in flatten_cli_output(result.stdout)
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_marks_each_value_with_its_source(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify each value is marked as coming from the configuration file, the
    shell, or a default, and the header names the file.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    monkeypatch.delenv(ENV_CSTAR_SLURM_MAX_WALLTIME, raising=False)
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    result = CliRunner().invoke(app, [], color=False)

    output = flatten_cli_output(result.stdout)
    assert "Configuration file:" in output
    assert str(isolate_user_config) in output.replace(" ", "")
    assert "(not found)" not in output
    assert f"{ENV_CSTAR_SLURM_ACCOUNT}: m1234 [config]" in output
    assert f"{ENV_CSTAR_LOG_LEVEL}: DEBUG [env]" in output
    assert f"{ENV_CSTAR_SLURM_MAX_WALLTIME}: 48:00:00 [default]" in output
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_says_when_there_is_no_configuration_file(
    isolate_user_config: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify the header flags a configuration file that does not exist.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    assert not isolate_user_config.exists()

    result = CliRunner().invoke(app, [], color=False)

    output = flatten_cli_output(result.stdout)
    assert str(isolate_user_config) in output.replace(" ", "")
    assert "(not found)" in output
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_export_has_no_source_markers(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify the export display carries no source markers.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    result = CliRunner().invoke(app, ["--display", "export"], color=False)

    output = flatten_cli_output(result.stdout)
    assert f"export {ENV_CSTAR_LOG_LEVEL}=" in output
    for marker in ("[config]", "[env]", "[default]"):
        assert marker not in output
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_names_the_system_set_in_the_file(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify the header names the system in use, and the setting that chose it is
    listed with the configuration file as its source.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    monkeypatch.delenv(ENV_CSTAR_SYSTEM, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SYSTEM}=linux_x86_64\n")

    result = CliRunner().invoke(app, [], color=False)

    output = flatten_cli_output(result.stdout)
    assert result.exit_code == 0, result.output
    assert "System: linux_x86_64 (set by CSTAR_SYSTEM)" in output
    assert f"{ENV_CSTAR_SYSTEM}: linux_x86_64 [config]" in output
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
@pytest.mark.parametrize(
    ("selection", "label"),
    [
        (SystemSelection(MacOSSystemContext, SystemSource.DETECTED), "darwin_arm64"),
        (
            SystemSelection(MacOSSystemContext, SystemSource.FALLBACK, "mycluster"),
            "darwin_arm64 (generic; 'mycluster' is not a registered system)",
        ),
    ],
    ids=["detected", "fallback"],
)
def test_cli_env_show_names_the_detected_system(
    selection: SystemSelection,
    label: str,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify the header says when the system was not recognised and the generic
    context is in use.

    Parameters
    ----------
    selection : SystemSelection
        What detection finds.
    label : str
        The system as the header shows it.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    monkeypatch.delenv(ENV_CSTAR_SYSTEM, raising=False)
    monkeypatch.setattr("cstar.system.manager.detect_system", lambda: selection)

    result = CliRunner().invoke(app, [], color=False)

    assert f"System: {label}" in flatten_cli_output(result.stdout)
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_survives_a_system_setting_that_names_nothing(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a `CSTAR_SYSTEM` that names no registered system does not stop the
    display: the header says why the system is not determined, and the setting is
    still listed so it can be fixed.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    monkeypatch.delenv(ENV_CSTAR_SYSTEM, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SYSTEM}=nowhere\n")

    result = CliRunner().invoke(app, [], color=False)

    output = flatten_cli_output(result.stdout)
    assert result.exit_code == 0, result.output
    assert "System: not determined: 'nowhere' is not a registered system" in output
    assert f"{ENV_CSTAR_SYSTEM}: nowhere [config]" in output
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_export_omits_the_system_line(
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify the export display stays a list of export statements.

    Parameters
    ----------
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    result = CliRunner().invoke(app, ["--display", "export"], color=False)

    assert "System:" not in flatten_cli_output(result.stdout)
    assert not result.stderr
