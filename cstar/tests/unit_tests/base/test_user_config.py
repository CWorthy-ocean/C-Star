import os
from pathlib import Path

import pytest

from cstar.base.env import (
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_RUNID,
    REMOVED_SETTINGS,
    capture_environment,
    get_env_item,
    load_user_config,
    read_user_config,
    user_config_problems,
)
from cstar.base.exceptions import CstarUserConfigError
from cstar.base.feature import ENV_FF_DEVELOPER_MODE
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT
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
        f"{ENV_CSTAR_LOG_LEVEL}=${{MYVAR}}/z\n"
    )

    values = read_user_config(isolate_user_config)

    assert values[ENV_CSTAR_SLURM_ACCOUNT] == str(Path.home() / "x")
    assert values[ENV_CSTAR_DATA_HOME] == "/my/var/y"
    assert values[ENV_CSTAR_LOG_LEVEL] == "/my/var/z"


def test_key_without_a_value_is_empty(isolate_user_config: Path) -> None:
    """Verify a key written without a value yields an empty string."""
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=\n")

    assert read_user_config(isolate_user_config) == {ENV_CSTAR_SLURM_ACCOUNT: ""}


def test_bad_keys_are_rejected_together(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify one error names the file and every key it may not set, and nothing
    in the file is applied.
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

    with pytest.raises(CstarUserConfigError) as excinfo:
        load_user_config()

    message = str(excinfo.value)
    assert str(isolate_user_config) in message
    for name in bad:
        assert name in message
    assert ENV_CSTAR_SLURM_ACCOUNT not in os.environ


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
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT)
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


def test_bare_name_without_equals_is_rejected(isolate_user_config: Path) -> None:
    """Verify a name written with no ``=`` is reported rather than exported empty."""
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}\n")

    with pytest.raises(CstarUserConfigError, match="has no value"):
        read_user_config(isolate_user_config)


def test_unreadable_file_is_reported_cleanly(isolate_user_config: Path) -> None:
    """Verify a file that cannot be decoded raises the configuration error
    naming the file, not a raw decoding error.
    """
    isolate_user_config.write_bytes(b"\xff\xfe" + b"CSTAR_LOG_LEVEL=DEBUG\n")

    with pytest.raises(CstarUserConfigError, match="cannot be read"):
        read_user_config(isolate_user_config)
