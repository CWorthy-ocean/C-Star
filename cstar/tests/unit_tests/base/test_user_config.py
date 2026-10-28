import os
from pathlib import Path

import pytest

import cstar
from cstar.base.env import (
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_CONFIG_REPORTED,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_RUNID,
    REMOVED_SETTINGS,
    canonical_user_config_value,
    capture_environment,
    get_env_item,
    load_user_config,
    parse_user_config,
    read_user_config,
    user_config_problems,
)
from cstar.base.exceptions import CstarUserConfigError
from cstar.base.feature import ENV_FF_DEVELOPER_MODE
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT, ENV_CSTAR_SLURM_QUEUE
from cstar.system.environment import LmodEnvSettings
from cstar.system.manager import CStarSystemManager


def test_missing_file_is_silent(isolate_user_config: Path) -> None:
    """Verify a missing configuration file loads as empty without complaint."""
    assert not isolate_user_config.exists()

    config = load_user_config()

    assert config.path == isolate_user_config
    assert not config.applied


def test_file_beats_default(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a setting only the file names is exported and takes effect."""
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    config = load_user_config()

    assert get_env_item(ENV_CSTAR_SLURM_ACCOUNT).value == "m1234"
    assert os.environ[ENV_CSTAR_SLURM_ACCOUNT] == "m1234"
    assert config.applied == {ENV_CSTAR_SLURM_ACCOUNT: "m1234"}


def test_env_beats_file(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify an exported value stands over the file and is not recorded as applied."""
    monkeypatch.setenv(ENV_CSTAR_SLURM_ACCOUNT, "shell-acct")
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    config = load_user_config()

    assert os.environ[ENV_CSTAR_SLURM_ACCOUNT] == "shell-acct"
    assert get_env_item(ENV_CSTAR_SLURM_ACCOUNT).value == "shell-acct"
    assert ENV_CSTAR_SLURM_ACCOUNT not in config.applied


def test_file_value_is_used_as_written_for_the_data_home(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify a data home named by the file wins over the default factory and is
    resolved exactly, with no `cstar` subdirectory appended.
    """
    target = tmp_path / "from-file"
    monkeypatch.delenv(ENV_CSTAR_DATA_HOME, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_DATA_HOME}={target}\n")

    load_user_config()

    assert get_env_item(ENV_CSTAR_DATA_HOME).value == str(target)
    assert DirectoryManager.data_home() == target.resolve()


def test_unnamed_data_home_keeps_its_default(
    isolate_user_config: Path, mock_xdg_dirs: dict[str, Path]
) -> None:
    """Verify a file that does not name the data home leaves it to the
    environment the fixtures provide.
    """
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    config = load_user_config()

    assert ENV_CSTAR_DATA_HOME not in config.applied
    assert get_env_item(ENV_CSTAR_DATA_HOME).value == str(
        mock_xdg_dirs[ENV_CSTAR_DATA_HOME]
    )


def test_values_are_expanded(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `~` and `$VAR` / `${VAR}` references in values are expanded."""
    monkeypatch.setenv("MYVAR", "/my/var")
    isolate_user_config.write_text(
        f"{ENV_CSTAR_SLURM_ACCOUNT}=~/x\n"
        f"{ENV_CSTAR_DATA_HOME}=$MYVAR/y\n"
        f"{ENV_CSTAR_SLURM_QUEUE}=${{MYVAR}}/z\n"
    )

    values = read_user_config(isolate_user_config)

    assert values[ENV_CSTAR_SLURM_ACCOUNT] == str(Path.home() / "x")
    assert values[ENV_CSTAR_DATA_HOME] == "/my/var/y"
    assert values[ENV_CSTAR_SLURM_QUEUE] == "/my/var/z"


def test_log_level_is_stored_in_upper_case(isolate_user_config: Path) -> None:
    """Verify a log level in any case is read as its upper-case name."""
    isolate_user_config.write_text(f"{ENV_CSTAR_LOG_LEVEL}=debug\n")

    config, skipped = parse_user_config(isolate_user_config)

    assert config == {ENV_CSTAR_LOG_LEVEL: "DEBUG"}
    assert not skipped


def test_unknown_log_level_is_skipped_with_the_reason(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a value that is not a log level is skipped like a bad name: one
    stderr line giving the reason, and the setting is left to its default.
    """
    monkeypatch.delenv(ENV_CSTAR_LOG_LEVEL, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_LOG_LEVEL}=loud\n")

    values, skipped = parse_user_config(isolate_user_config)

    reason = (
        f"{ENV_CSTAR_LOG_LEVEL}='loud' is not a log level; "
        "use one of TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL"
    )
    assert not values
    assert skipped == {ENV_CSTAR_LOG_LEVEL: reason}

    config = load_user_config()

    assert (
        capsys.readouterr().err
        == f"Ignoring a line in {isolate_user_config}: {reason}\n"
    )
    assert not config.applied
    assert ENV_CSTAR_LOG_LEVEL not in os.environ


def test_canonical_value_leaves_other_settings_alone() -> None:
    """Verify only a setting with a fixed set of spellings is rewritten."""
    assert canonical_user_config_value(ENV_CSTAR_SLURM_ACCOUNT, "m1234") == "m1234"
    assert canonical_user_config_value(ENV_CSTAR_LOG_LEVEL, " warning ") == "WARNING"


def test_key_without_a_value_is_empty(isolate_user_config: Path) -> None:
    """Verify a key written without a value yields an empty string."""
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=\n")

    assert read_user_config(isolate_user_config) == {ENV_CSTAR_SLURM_ACCOUNT: ""}


def test_bad_keys_are_skipped_with_one_line_each(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify every key the file may not set is skipped with one stderr line
    naming the file, and the file's other settings still apply.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    removed = next(iter(REMOVED_SETTINGS))
    bad = [
        "CSTAR_NOT_A_SETTING",
        ENV_FF_DEVELOPER_MODE,
        ENV_CSTAR_CONFIG_HOME,
        ENV_CSTAR_RUNID,
        removed,
    ]
    isolate_user_config.write_text(
        "\n".join([f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234", *(f"{n}=1" for n in bad)])
    )

    config = load_user_config()

    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == len(bad)
    for name, line in zip(bad, lines, strict=True):
        assert line.startswith(f"Ignoring a line in {isolate_user_config}: {name} ")
    assert list(config.skipped) == bad
    assert config.applied == {ENV_CSTAR_SLURM_ACCOUNT: "m1234"}
    assert os.environ[ENV_CSTAR_SLURM_ACCOUNT] == "m1234"


def test_removed_setting_is_skipped_with_its_guidance(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a file written with a setting this version removed (as one written
    by an older C-Star is) loads, skipping it with one line that quotes the
    registry's guidance.
    """
    name = "CSTAR_ORCH_TRX_FREQ"
    monkeypatch.delenv(name, raising=False)
    isolate_user_config.write_text(f"{name}=1D\n")

    config = load_user_config()

    err = capsys.readouterr().err
    assert err == (
        f"Ignoring a line in {isolate_user_config}: {name} has no effect; "
        f"{REMOVED_SETTINGS[name].guidance}\n"
    )
    assert name not in os.environ
    assert config.applied == {}


def test_newer_versions_setting_is_skipped_as_unknown(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a file naming a setting from a newer C-Star loads, skipping it with
    one line saying this version does not know it, and applying the rest.
    """
    name = "CSTAR_FROM_A_NEWER_VERSION"
    monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(f"{name}=1\n{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    config = load_user_config()

    err = capsys.readouterr().err
    assert err == (
        f"Ignoring a line in {isolate_user_config}: {name} is not a setting "
        f"C-Star {cstar.__version__} knows\n"
    )
    assert name not in os.environ
    assert config.applied == {ENV_CSTAR_SLURM_ACCOUNT: "m1234"}


def test_other_names_are_not_c_star_settings() -> None:
    """Verify a name without the C-Star prefix is reported as not a setting at all,
    with no version: no version of C-Star reads it.
    """
    assert user_config_problems(["SLURM_ACCOUNT"]) == [
        "SLURM_ACCOUNT is not a C-Star setting"
    ]


def test_skipped_lines_are_reported_once_per_process_tree(
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the loader reports skipped lines once and marks the file reported,
    so a step subprocess that inherits the marker and reads the same file stays
    quiet, while one reading another file reports it.
    """
    isolate_user_config.write_text("CSTAR_FROM_A_NEWER_VERSION=1\n")

    load_user_config()
    load_user_config()
    assert len(capsys.readouterr().err.splitlines()) == 1
    assert os.environ[ENV_CSTAR_CONFIG_REPORTED] == str(isolate_user_config)

    # a subprocess: a fresh cache, the parent's environment
    load_user_config.cache_clear()
    config = load_user_config()
    assert capsys.readouterr().err == ""
    assert list(config.skipped) == ["CSTAR_FROM_A_NEWER_VERSION"]

    monkeypatch.setenv(ENV_CSTAR_CONFIG_REPORTED, "/another/config.env")
    load_user_config.cache_clear()
    load_user_config()
    assert "CSTAR_FROM_A_NEWER_VERSION" in capsys.readouterr().err


def test_a_clean_file_sets_no_report_marker(isolate_user_config: Path) -> None:
    """Verify a file with nothing to skip leaves the report marker unset."""
    isolate_user_config.write_text(f"{ENV_CSTAR_LOG_LEVEL}=INFO\n")

    load_user_config()

    assert ENV_CSTAR_CONFIG_REPORTED not in os.environ


def test_good_key_has_no_problems() -> None:
    """Verify a registered, persistable setting passes validation."""
    assert user_config_problems([ENV_CSTAR_SLURM_ACCOUNT]) == []


def test_load_is_once_per_process(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a second load returns the same object, does not re-read a changed
    file, and does not re-export a setting removed from the environment.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    first = load_user_config()
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=other\n")
    # not monkeypatch.delenv: it would record the loader's export as the original
    # value and put it back after the fixture's `patch.dict` restored the environment
    del os.environ[ENV_CSTAR_SLURM_ACCOUNT]
    second = load_user_config()

    assert second is first
    assert second.applied == {ENV_CSTAR_SLURM_ACCOUNT: "m1234"}
    assert ENV_CSTAR_SLURM_ACCOUNT not in os.environ


def test_capture_environment_includes_config_settings(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a setting the file supplied is captured for a run record."""
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    load_user_config()

    assert capture_environment()[ENV_CSTAR_SLURM_ACCOUNT] == "m1234"


def test_system_manager_loads_the_file(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify constructing a system manager exports the configuration file."""
    monkeypatch.setenv(LmodEnvSettings.variable("SYSHOST"), "")
    monkeypatch.setenv(LmodEnvSettings.variable("SYSTEM_NAME"), "Perlmutter")
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    CStarSystemManager()

    assert os.environ[ENV_CSTAR_SLURM_ACCOUNT] == "m1234"


def test_bare_name_without_equals_is_skipped(isolate_user_config: Path) -> None:
    """Verify a name written with no ``=`` is skipped rather than exported empty."""
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}\n")

    values, skipped = parse_user_config(isolate_user_config)

    assert values == {}
    assert "has no value" in skipped[ENV_CSTAR_SLURM_ACCOUNT]


def test_unreadable_file_is_reported_cleanly(isolate_user_config: Path) -> None:
    """Verify a file that cannot be decoded raises the configuration error
    naming the file, not a raw decoding error.
    """
    isolate_user_config.write_bytes(b"\xff\xfe" + b"CSTAR_LOG_LEVEL=DEBUG\n")

    with pytest.raises(CstarUserConfigError, match="cannot be read"):
        read_user_config(isolate_user_config)
