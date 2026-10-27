from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.base.env import (
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_SCRATCH_DIRS,
    REMOVED_SETTINGS,
    USER_CONFIG_HEADER,
    get_env_item,
    read_user_config,
    render_setting_block,
)
from cstar.base.feature import ENV_FF_DEVELOPER_MODE
from cstar.cli.environment.config_file import app_set, app_unset
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT

EXISTING_FILE = f"""\
# my notes
{ENV_CSTAR_SCRATCH_DIRS}=/scratch

# log level
{ENV_CSTAR_LOG_LEVEL}=INFO
# trailing comment
"""
"""A hand-edited file with comments, a blank line, and two settings."""


@pytest.fixture
def runner() -> CliRunner:
    """A runner that keeps stderr apart from stdout."""
    return CliRunner()


def test_set_creates_file_and_parent(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """The first `set` creates a missing config directory and the file, with the
    header and the setting's block.
    """
    path = isolate_user_config
    path.parent.rmdir()

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    assert f"Set {ENV_CSTAR_LOG_LEVEL}=DEBUG in" in result.stdout
    expected = (
        USER_CONFIG_HEADER
        + "\n"
        + render_setting_block(get_env_item(ENV_CSTAR_LOG_LEVEL), "DEBUG")
    )
    assert path.read_text() == expected
    assert read_user_config(path) == {ENV_CSTAR_LOG_LEVEL: "DEBUG"}


def test_set_replaces_active_line_in_place(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """An existing setting changes in place; every other line is untouched."""
    path = isolate_user_config
    path.write_text(EXISTING_FILE)

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    assert path.read_text() == EXISTING_FILE.replace(
        f"{ENV_CSTAR_LOG_LEVEL}=INFO", f"{ENV_CSTAR_LOG_LEVEL}=DEBUG"
    )


def test_set_replaces_export_line_and_drops_duplicates(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """An `export NAME=` line counts as active, and later duplicates are removed."""
    path = isolate_user_config
    path.write_text(
        f"# top\nexport {ENV_CSTAR_LOG_LEVEL}=INFO\n# mid\n{ENV_CSTAR_LOG_LEVEL}=WARNING\n"
    )

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    assert path.read_text() == f"# top\n{ENV_CSTAR_LOG_LEVEL}=DEBUG\n# mid\n"


def test_set_fills_placeholder_in_place(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """A commented-out `# NAME=` line is filled where it stands."""
    path = isolate_user_config
    text = f"# before\n# {ENV_CSTAR_LOG_LEVEL}=\n# after\n"
    path.write_text(text)

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    assert path.read_text() == f"# before\n{ENV_CSTAR_LOG_LEVEL}=DEBUG\n# after\n"


@pytest.mark.parametrize("trailing_newline", [True, False])
def test_set_appends_when_absent(
    runner: CliRunner, isolate_user_config: Path, trailing_newline: bool
) -> None:
    """A setting the file does not mention is appended as a block."""
    path = isolate_user_config
    existing = f"{ENV_CSTAR_SCRATCH_DIRS}=/scratch"
    path.write_text(existing + ("\n" if trailing_newline else ""))

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    block = render_setting_block(get_env_item(ENV_CSTAR_LOG_LEVEL), "DEBUG")
    assert path.read_text() == f"{existing}\n\n{block}"
    assert read_user_config(path) == {
        ENV_CSTAR_SCRATCH_DIRS: "/scratch",
        ENV_CSTAR_LOG_LEVEL: "DEBUG",
    }


def test_set_quotes_value_with_spaces(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """A value with whitespace is quoted and reads back unchanged."""
    path = isolate_user_config

    result = runner.invoke(
        app_set, [ENV_CSTAR_SCRATCH_DIRS, "/a b/scratch # x"], color=False
    )

    assert result.exit_code == 0
    assert f'{ENV_CSTAR_SCRATCH_DIRS}="/a b/scratch # x"' in path.read_text()
    assert read_user_config(path) == {ENV_CSTAR_SCRATCH_DIRS: "/a b/scratch # x"}


def test_set_quotes_value_starting_with_a_quote(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """A value that begins with a quote character is protected so it reads back
    as written.
    """
    result = runner.invoke(app_set, [ENV_CSTAR_SLURM_ACCOUNT, "'acct'"], color=False)

    assert result.exit_code == 0
    assert read_user_config(isolate_user_config) == {ENV_CSTAR_SLURM_ACCOUNT: "'acct'"}
    assert "reads back as" not in result.stdout


def test_set_reports_the_expanded_value(
    runner: CliRunner, isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A value with a ``$VAR`` reference is written as given and the command says
    what it reads back as.
    """
    monkeypatch.setenv("MYVAR", "/my/var")

    result = runner.invoke(app_set, [ENV_CSTAR_SLURM_ACCOUNT, "$MYVAR/x"], color=False)

    assert result.exit_code == 0
    assert f"{ENV_CSTAR_SLURM_ACCOUNT}=$MYVAR/x" in isolate_user_config.read_text()
    assert "reads back as '/my/var/x'" in result.stdout.replace("\n", "")


def test_set_warns_when_shell_exports_other_value(
    runner: CliRunner,
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """A different exported value is reported on stderr, since it wins."""
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "ERROR")

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    assert "takes precedence" in flatten_cli_output(result.stderr)
    assert "takes precedence" not in result.stdout


def test_set_is_quiet_when_the_file_supplied_the_exported_value(
    runner: CliRunner,
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A value the loader exported from the file is not a shell export, so
    replacing it needs no warning.
    """
    monkeypatch.delenv(ENV_CSTAR_LOG_LEVEL, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_LOG_LEVEL}=ERROR\n")

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    assert not result.stderr
    assert f"{ENV_CSTAR_LOG_LEVEL}=DEBUG" in isolate_user_config.read_text()


def test_set_is_quiet_when_shell_exports_same_value(
    runner: CliRunner,
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exported value equal to the new one needs no warning."""
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0
    assert not result.stderr


def test_unset_comments_out_active_line(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """`unset` leaves a placeholder and keeps every other line."""
    path = isolate_user_config
    path.write_text(EXISTING_FILE)

    result = runner.invoke(app_unset, [ENV_CSTAR_LOG_LEVEL], color=False)

    assert result.exit_code == 0
    assert f"Unset {ENV_CSTAR_LOG_LEVEL} in" in result.stdout
    assert path.read_text() == EXISTING_FILE.replace(
        f"{ENV_CSTAR_LOG_LEVEL}=INFO", f"# {ENV_CSTAR_LOG_LEVEL}="
    )
    assert read_user_config(path) == {ENV_CSTAR_SCRATCH_DIRS: "/scratch"}


def test_set_after_unset_reuses_placeholder(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """The placeholder `unset` leaves is where a later `set` writes."""
    path = isolate_user_config
    path.write_text(EXISTING_FILE)

    runner.invoke(app_unset, [ENV_CSTAR_LOG_LEVEL], color=False)
    runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "INFO"], color=False)

    assert path.read_text() == EXISTING_FILE


def test_unset_absent_name_reports_and_succeeds(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """Unsetting a setting the file does not set is not an error."""
    path = isolate_user_config
    path.write_text(f"# {ENV_CSTAR_LOG_LEVEL}=\n")

    result = runner.invoke(app_unset, [ENV_CSTAR_LOG_LEVEL], color=False)

    assert result.exit_code == 0
    assert f"{ENV_CSTAR_LOG_LEVEL} is not set in" in result.stdout
    assert path.read_text() == f"# {ENV_CSTAR_LOG_LEVEL}=\n"


def test_unset_without_file_reports_and_succeeds(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """A missing file is not created by `unset`."""
    path = isolate_user_config

    result = runner.invoke(app_unset, [ENV_CSTAR_LOG_LEVEL], color=False)

    assert result.exit_code == 0
    assert f"{ENV_CSTAR_LOG_LEVEL} is not set in" in result.stdout
    assert not path.exists()


REJECTED_NAMES = [
    "CSTAR_NOT_A_SETTING",
    ENV_FF_DEVELOPER_MODE,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_RUNID,
    next(iter(REMOVED_SETTINGS)),
]


@pytest.mark.parametrize("name", REJECTED_NAMES)
def test_rejected_names(
    runner: CliRunner,
    isolate_user_config: Path,
    flatten_cli_output: Callable[[str], str],
    name: str,
) -> None:
    """`set` refuses names the file may not hold as a usage error, naming the
    setting, with the file left alone: a typo is caught when it is written.
    """
    path = isolate_user_config
    path.write_text(EXISTING_FILE)

    result = runner.invoke(app_set, [name, "x"], color=False)

    assert result.exit_code == 2
    assert name in flatten_cli_output(result.stderr)
    assert path.read_text() == EXISTING_FILE


@pytest.mark.parametrize("name", REJECTED_NAMES)
def test_unset_takes_out_a_line_the_loader_skips(
    runner: CliRunner, isolate_user_config: Path, name: str
) -> None:
    """`unset` accepts any name, so a line the loader skips (a removed setting,
    one from a newer version) can be taken out of the file with it.
    """
    path = isolate_user_config
    path.write_text(f"{EXISTING_FILE}{name}=1\n")

    result = runner.invoke(app_unset, [name], color=False)

    assert result.exit_code == 0, result.output
    assert path.read_text() == f"{EXISTING_FILE}# {name}=\n"


def test_unset_comments_out_a_name_written_without_equals(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """`unset` takes out a line naming a setting with no ``=``."""
    path = isolate_user_config
    path.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}\n{EXISTING_FILE}")

    result = runner.invoke(app_unset, [ENV_CSTAR_SLURM_ACCOUNT], color=False)

    assert result.exit_code == 0, result.output
    assert path.read_text() == f"# {ENV_CSTAR_SLURM_ACCOUNT}=\n{EXISTING_FILE}"


def test_set_fills_in_a_name_written_without_equals(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """`set` gives a value to a line naming the setting with no ``=``, in place."""
    path = isolate_user_config
    path.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}\n{EXISTING_FILE}")

    result = runner.invoke(app_set, [ENV_CSTAR_SLURM_ACCOUNT, "m1234"], color=False)

    assert result.exit_code == 0, result.output
    assert path.read_text() == f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n{EXISTING_FILE}"


def test_set_keeps_lines_the_loader_skips(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """`set` in a file that names a setting from a newer version leaves that line
    alone: another version of C-Star may read it.
    """
    path = isolate_user_config
    path.write_text(f"{EXISTING_FILE}CSTAR_FROM_A_NEWER_VERSION=1\n")

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "DEBUG"], color=False)

    assert result.exit_code == 0, result.output
    assert "CSTAR_FROM_A_NEWER_VERSION=1\n" in path.read_text()
    assert read_user_config(path)[ENV_CSTAR_LOG_LEVEL] == "DEBUG"


@pytest.mark.parametrize("name", REJECTED_NAMES)
def test_rejected_names_create_no_file(
    runner: CliRunner, isolate_user_config: Path, name: str
) -> None:
    """A refused `set` does not create the file."""
    path = isolate_user_config

    result = runner.invoke(app_set, [name, "x"], color=False)

    assert result.exit_code == 2
    assert not path.exists()


def test_set_refuses_a_value_that_is_not_a_log_level(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """A refused value creates no file and names the levels the setting accepts."""
    path = isolate_user_config

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "loud"], color=False)

    assert result.exit_code == 2
    assert "is not a log level" in result.stderr.replace("\n", "")
    assert not path.exists()


def test_set_leaves_an_existing_file_alone_when_refusing_a_value(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """A refused value does not rewrite a file that already holds the setting."""
    path = isolate_user_config
    path.write_text(EXISTING_FILE)

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "loud"], color=False)

    assert result.exit_code == 2
    assert path.read_text() == EXISTING_FILE


def test_set_stores_a_log_level_in_upper_case(
    runner: CliRunner, isolate_user_config: Path
) -> None:
    """A log level typed in lower case is written, reported and read back as its
    upper-case name, with no "reads back as" note.
    """
    path = isolate_user_config

    result = runner.invoke(app_set, [ENV_CSTAR_LOG_LEVEL, "debug"], color=False)

    assert result.exit_code == 0
    assert f"{ENV_CSTAR_LOG_LEVEL}=DEBUG\n" in path.read_text()
    assert f"Set {ENV_CSTAR_LOG_LEVEL}=DEBUG in" in result.stdout
    assert "reads back as" not in result.stdout
    assert read_user_config(path) == {ENV_CSTAR_LOG_LEVEL: "DEBUG"}
