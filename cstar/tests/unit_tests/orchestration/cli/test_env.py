import os
import re
import shlex
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.base.env import (
    ENV_CSTAR_CACHE_HOME,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_STATE_HOME,
    ENV_CSTAR_SYSTEM,
    env_var_groups,
)
from cstar.cli.environment.show import app, effective_value
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
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
def test_cli_env_show_export_omits_internal() -> None:
    """Verify that export statements leave out the internal group but still
    include a setting that is not persistable.
    """
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--display", "export"],
        color=False,
    )

    assert "CSTAR_RUNID" not in result.stdout
    assert "export CSTAR_CONFIG_HOME=" in result.stdout
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
) -> None:
    """Verify the export display carries no source markers.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    result = CliRunner().invoke(app, ["--display", "export"], color=False)

    assert f"export {ENV_CSTAR_LOG_LEVEL}=DEBUG" in result.stdout
    for marker in ("[config]", "[env]", "[default]"):
        assert marker not in result.stdout
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
def test_cli_env_show_export_omits_the_system_line() -> None:
    """Verify the export display stays a list of export statements."""
    result = CliRunner().invoke(app, ["--display", "export"], color=False)

    assert "System:" not in result.stdout
    assert not result.stderr


EXPORT_LINE = re.compile(r"^(?P<comment># )?export \w+=")
"""A line that assigns a setting, commented out or not (descriptions also start
with ``#``)."""


def parse_exports(output: str) -> dict[str, tuple[bool, str]]:
    """Read the export statements out of ``cstar env show --display export``.

    Each statement is parsed as a shell would, so a value that is quoted
    wrongly, or split over two lines, fails here rather than in a user's shell.

    Parameters
    ----------
    output : str
        The command's output, exactly as printed.

    Returns
    -------
    dict[str, tuple[bool, str]]
        Each setting's name mapped to whether its statement is active (not
        commented out) and the value the statement assigns.
    """
    parsed: dict[str, tuple[bool, str]] = {}

    for line in output.splitlines():
        if match := EXPORT_LINE.match(line):
            command, assignment = shlex.split(line.removeprefix("# "))
            assert command == "export"
            name, _, value = assignment.partition("=")
            parsed[name] = (not match["comment"], value)

    return parsed


def export_output(args: list[str] | None = None) -> str:
    """Run ``cstar env show --display export`` and return its stdout."""
    result = CliRunner().invoke(
        app, [*(args or []), "--display", "export"], color=False
    )
    assert result.exit_code == 0, result.output
    assert not result.stderr
    return result.stdout


def test_cli_env_show_export_is_a_shell_script(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify every line of the export display is a comment or one export
    statement a shell reads as a single assignment.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    """
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")
    isolate_user_config.write_text(f'{ENV_CSTAR_SLURM_ACCOUNT}="my account"\n')

    output = export_output()

    lines = [line for line in output.splitlines() if line.strip()]
    for line in lines:
        assert line.startswith("#") or (line.startswith("export ") and "=" in line)
    parsed = parse_exports(output)
    assert parsed[ENV_CSTAR_SLURM_ACCOUNT] == (True, "my account")
    assert len(parsed) == sum(len(items) for items in env_var_groups().values())


def test_cli_env_show_export_comments_out_what_the_user_did_not_set(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify only a setting that was exported or came from the configuration
    file is an active statement, so sourcing the output cannot freeze defaults.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    """
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    monkeypatch.delenv(ENV_CSTAR_SLURM_MAX_WALLTIME, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    output = export_output()

    parsed = parse_exports(output)
    assert f"\nexport {ENV_CSTAR_LOG_LEVEL}=DEBUG\n" in output
    assert parsed[ENV_CSTAR_LOG_LEVEL] == (True, "DEBUG")
    assert parsed[ENV_CSTAR_SLURM_ACCOUNT] == (True, "m1234")
    assert parsed[ENV_CSTAR_SLURM_MAX_WALLTIME] == (False, "48:00:00")


def test_cli_env_show_export_never_exports_an_empty_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a setting that is empty is commented out, since exporting it would
    turn off the fallback the setting has when it is not set.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    """
    monkeypatch.setenv(ENV_CSTAR_SLURM_QUEUE, "")

    parsed = parse_exports(export_output())

    assert parsed[ENV_CSTAR_SLURM_QUEUE] == (False, "")
    assert not [
        name for name, (active, value) in parsed.items() if active and not value
    ]


@pytest.mark.parametrize(
    ("setting", "xdg_variable", "home"),
    [
        (ENV_CSTAR_CACHE_HOME, "XDG_CACHE_HOME", "cache_home"),
        (ENV_CSTAR_CONFIG_HOME, "XDG_CONFIG_HOME", "config_home"),
        (ENV_CSTAR_DATA_HOME, "XDG_DATA_HOME", "data_home"),
        (ENV_CSTAR_STATE_HOME, "XDG_STATE_HOME", "state_home"),
    ],
)
def test_cli_env_show_reports_the_directory_xdg_settings_resolve_to(
    setting: str,
    xdg_variable: str,
    home: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify a home taken from an ``XDG_*_HOME`` variable is shown as the
    ``cstar`` directory under it, which is where C-Star writes, in both displays.

    Parameters
    ----------
    setting : str
        The ``CSTAR_*_HOME`` setting under test.
    xdg_variable : str
        The standard variable the setting falls back to.
    home : str
        The `DirectoryManager` method that returns the directory C-Star uses.
    tmp_path : Path
        Temporary directory standing in for the XDG base directory.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    squash_cli_output : Callable[[str], str]
        Fixture that removes the whitespace rich wrapped paths at.
    """
    base = tmp_path.resolve() / "xdg-base"
    monkeypatch.delenv(setting, raising=False)
    monkeypatch.setenv(xdg_variable, str(base))
    # a scratch file system would take precedence over the XDG variable
    monkeypatch.setattr("cstar.base.env.hpc_data_directory", lambda: None)
    expected = getattr(DirectoryManager, home)()
    assert expected == base / "cstar"

    active, value = parse_exports(export_output())[setting]

    assert not active
    assert value == str(expected)
    assert value.endswith("/cstar")
    assert Path(value).is_relative_to(tmp_path.resolve())
    interactive = CliRunner().invoke(app, [], color=False)
    assert f"{setting}:{expected}[default]" in squash_cli_output(interactive.stdout)


def test_cli_env_show_export_matches_what_c_star_uses(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify every active statement assigns the value the shell or the
    configuration file gave the setting, so sourcing the output changes nothing,
    and that a home's value is the directory C-Star uses.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    """
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")
    isolate_user_config.write_text(f'{ENV_CSTAR_SLURM_ACCOUNT}="my account"\n')
    items = {item.name: item for group in env_var_groups().values() for item in group}

    active = {
        name: value
        for name, (is_active, value) in parse_exports(export_output()).items()
        if is_active
    }

    assert {ENV_CSTAR_LOG_LEVEL, ENV_CSTAR_SLURM_ACCOUNT, ENV_CSTAR_DATA_HOME} <= set(
        active
    )
    for name, value in active.items():
        assert value == os.environ[name] == effective_value(items[name])
    assert active[ENV_CSTAR_DATA_HOME] == str(DirectoryManager.data_home())


def test_cli_env_show_export_keeps_a_home_as_the_user_set_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a home the user set is exported as written, not resolved, so
    sourcing the output does not swap a symlink for its target.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory holding the link and its target.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    """
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target)
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(link))

    assert parse_exports(export_output())[ENV_CSTAR_DATA_HOME] == (True, str(link))


@pytest.mark.parametrize("value", ["x" * 150, "[bold]x[/bold]"], ids=["long", "markup"])
def test_cli_env_show_export_writes_each_statement_verbatim(
    value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a statement is neither wrapped at the terminal width nor altered by
    rich markup, either of which would corrupt the script.

    Parameters
    ----------
    value : str
        A value that a wrapping or markup-aware printer would change.
    monkeypatch : pytest.MonkeyPatch
        Fixture to modify the environment.
    """
    monkeypatch.setenv(ENV_CSTAR_SLURM_ACCOUNT, value)

    output = export_output()

    assert (
        f"export {ENV_CSTAR_SLURM_ACCOUNT}={shlex.quote(value)}" in output.splitlines()
    )
    assert parse_exports(output)[ENV_CSTAR_SLURM_ACCOUNT] == (True, value)
