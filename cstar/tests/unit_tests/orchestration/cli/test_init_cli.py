"""Tests for `cstar init`, the guided setup that follows an install."""

import os
import subprocess
import typing as t
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import pytest
import typer
from click.testing import Result
from typer.testing import CliRunner

from cstar.base.env import (
    ENV_CSTAR_CATALOG,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_PROJECT_HOME,
    ENV_CSTAR_RUNID,
    REMOVED_SETTINGS,
    load_user_config,
    read_user_config,
)
from cstar.base.exceptions import CstarError, CstarUserConfigError
from cstar.base.feature import ENV_FF_DEBUG_BUILD_MODE
from cstar.cli import init as init_module
from cstar.cli.cli import app as root_app
from cstar.cli.cli import attach_subcommands
from cstar.cli.environment import register_kernel, shell_init
from cstar.cli.environment.register_kernel import RegisterKernelError
from cstar.cli.environment.shell_init import Shell
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)
from cstar.system.manager import AnvilEnvSettings, MacOSSystemContext
from cstar.system.scheduler import SlurmPartition, SlurmScheduler
from cstar.tests.unit_tests.orchestration.cli.conftest import FakeEntryPoint

REAL_QUERY_SLURM = init_module.query_slurm
"""The SLURM lookup as written, captured before the autouse fixture stubs it."""
SCRATCH_VARIABLES = ("SCRATCH", "SCRATCH_DIR", "LOCAL_SCRATCH")
CLEARED_VARIABLES = (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_QUEUE,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_PROJECT_HOME,
    ENV_CSTAR_CATALOG,
    "PROJECT",
    *SCRATCH_VARIABLES,
)
"""Variables a developer's shell may export that would change what `init` shows."""

FAKE_PREFIX = Path("/envs/cstar-env")
FAKE_KERNEL_PATH = Path("/kernels/cstar-env")
NEVER_WRITTEN = (
    *REMOVED_SETTINGS,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_RUNID,
    ENV_FF_DEBUG_BUILD_MODE,
)
"""Names that must not appear anywhere in a file `init` writes."""


class FakeSlurmContext:
    """A SLURM system whose settings class requires an account and a queue."""

    name = "fakeslurm"
    compiler = "gnu"
    mpi_prefix = "srun"

    @classmethod
    def create_scheduler(cls) -> SlurmScheduler:
        return SlurmScheduler(
            queues=[SlurmPartition(name="regular")],
            primary_queue_name="regular",
            max_cpus_per_node=128,
        )

    @classmethod
    def settings_klass(cls) -> type[AnvilEnvSettings]:
        return AnvilEnvSettings

    @classmethod
    def is_match(cls) -> bool:
        return False

    @classmethod
    def scratch_root(cls) -> None:
        return None


class Recorder:
    """What the fake shell and kernel installers were asked to do."""

    def __init__(self) -> None:
        self.installs: list[Shell] = []
        self.kernels: list[dict[str, t.Any]] = []


def use_context(monkeypatch: pytest.MonkeyPatch, context: t.Any) -> None:
    """Make `context` the system, for `init` and for the scratch lookup."""
    for target in (
        "cstar.cli.init.get_system_context",
        "cstar.system.manager.get_system_context",
    ):
        monkeypatch.setattr(target, lambda: context)


def raise_unknown_system() -> t.NoReturn:
    raise CstarError("Unknown system requested: x")


@pytest.fixture(autouse=True)
def safe_environment(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    """Run every test on a laptop with no SLURM hints and no way to touch the
    developer's shell rc files or Jupyter.
    """
    for name in CLEARED_VARIABLES:
        monkeypatch.delenv(name, raising=False)

    use_context(monkeypatch, MacOSSystemContext())
    monkeypatch.setattr("cstar.cli.init.query_slurm", lambda args: [])

    recorder = Recorder()
    monkeypatch.setattr(shell_init, "detect_shell", lambda: Shell.ZSH)
    monkeypatch.setattr(shell_init, "_install", recorder.installs.append)
    monkeypatch.setattr(register_kernel, "_env_prefix", lambda: FAKE_PREFIX)

    def fake_register_kernel(**kwargs: t.Any) -> Path:
        recorder.kernels.append(kwargs)
        return FAKE_KERNEL_PATH

    monkeypatch.setattr(register_kernel, "register_kernel", fake_register_kernel)
    return recorder


@pytest.fixture
def run_init() -> Callable[..., Result]:
    """Invoke `cstar init` through the root app, each call as a fresh process would.

    The loader caches the file once per process and exports what it read, so
    after each call what it applied is removed from the environment and the
    cache is cleared; a second call in the same test then re-reads the file.
    """

    def run(*args: str, input: str | None = None) -> Result:
        result = CliRunner().invoke(root_app, ["init", *args], input=input, color=False)
        try:
            applied = load_user_config().applied
        except CstarUserConfigError:
            applied = {}
        for name in applied:
            os.environ.pop(name, None)
        load_user_config.cache_clear()
        return result

    return run


@pytest.fixture
def slurm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the system a SLURM cluster."""
    use_context(monkeypatch, FakeSlurmContext)


@pytest.fixture
def laptop_data_home(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave CSTAR_DATA_HOME unexported so it resolves to its default."""
    monkeypatch.delenv(ENV_CSTAR_DATA_HOME)


def test_yes_on_a_laptop_writes_a_file_of_commented_examples(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    isolate_user_config: Path,
    safe_environment: Recorder,
    laptop_data_home: None,
) -> None:
    """With nothing exported, `--yes` keeps every default, so no setting is set."""
    path = isolate_user_config

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "SLURM account" not in output
    assert path.is_file()
    assert read_user_config(path) == {}
    text = path.read_text()
    assert "# CSTAR_NPROCS_POST=" in text
    assert "# CSTAR_DATA_HOME=" in text
    for name in NEVER_WRITTEN:
        assert name not in text
    assert "Shell function: installed for zsh" in output
    assert "Jupyter kernel: registered (cstar-env)" in output
    assert safe_environment.installs == [Shell.ZSH]
    assert len(safe_environment.kernels) == 1


def test_yes_moves_an_exported_data_home_into_the_file(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    isolate_user_config: Path,
) -> None:
    """A value exported in the shell is written to the file, and the user is told
    the export line can go.
    """
    value = os.environ[ENV_CSTAR_DATA_HOME]

    result = run_init("--yes")

    assert result.exit_code == 0, result.output
    assert read_user_config(isolate_user_config) == {ENV_CSTAR_DATA_HOME: value}
    assert "remove the export line" in flatten_cli_output(result.output)


def test_interactive_laptop_run_applies_each_answer(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    isolate_user_config: Path,
    safe_environment: Recorder,
) -> None:
    """Typed values override, an empty line keeps, and `n` skips shell and kernel."""
    path = isolate_user_config

    result = run_init(input="/data/x\n/proj\n\nn\nn\n")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert read_user_config(path) == {
        ENV_CSTAR_DATA_HOME: "/data/x",
        ENV_CSTAR_PROJECT_HOME: "/proj",
    }
    assert "# CSTAR_CATALOG=" in path.read_text()
    assert "Shell function: skipped" in output
    assert "Jupyter kernel: skipped" in output
    assert safe_environment.installs == []
    assert safe_environment.kernels == []


def test_yes_on_slurm_keeps_exported_scheduler_settings(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    slurm: None,
) -> None:
    """Exported account and queue move into the file; the walltime default is not pinned."""
    monkeypatch.setenv(ENV_CSTAR_SLURM_ACCOUNT, "m1234")
    monkeypatch.setenv(ENV_CSTAR_SLURM_QUEUE, "regular")

    def query(args: t.Sequence[str]) -> list[str]:
        return ["m1234|regular", "m5678|"] if args[0] == "sacctmgr" else ["regular*"]

    monkeypatch.setattr("cstar.cli.init.query_slurm", query)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "m5678 (any partition)" in output
    assert "Press Enter to keep" not in output
    assert "SLURM account (required" not in output
    config = read_user_config(isolate_user_config)
    assert config[ENV_CSTAR_SLURM_ACCOUNT] == "m1234"
    assert config[ENV_CSTAR_SLURM_QUEUE] == "regular"
    assert ENV_CSTAR_SLURM_MAX_WALLTIME not in config
    assert f"# {ENV_CSTAR_SLURM_MAX_WALLTIME}=" in isolate_user_config.read_text()
    assert "remove the export line" in output


def test_yes_on_slurm_warns_about_unset_required_settings(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    slurm: None,
) -> None:
    """Under `--yes` nothing is prompted, so an unset required setting is called out."""
    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert f"{ENV_CSTAR_SLURM_ACCOUNT} is not set; a workplan run" in output
    assert f"{ENV_CSTAR_SLURM_QUEUE} is not set; a workplan run" in output
    assert f"{ENV_CSTAR_SLURM_MAX_WALLTIME} is not set" not in output


def test_interactive_slurm_run_sets_only_what_is_answered(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    isolate_user_config: Path,
    laptop_data_home: None,
    slurm: None,
) -> None:
    """An empty answer keeps an unset setting unset, and `-` leaves a default unset."""
    result = run_init(input="m1234\n\n-\n\n\n\nn\nn\n")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "SLURM account (required on this system)" in output
    assert "[48:00:00]" in output
    assert read_user_config(isolate_user_config) == {ENV_CSTAR_SLURM_ACCOUNT: "m1234"}


def test_rerun_prefills_from_the_file_and_carries_other_settings(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    isolate_user_config: Path,
    laptop_data_home: None,
    slurm: None,
) -> None:
    """A re-run offers the file's values and keeps settings it does not prompt for."""
    path = isolate_user_config
    path.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n{ENV_CSTAR_LOG_LEVEL}=DEBUG\n")

    result = run_init(input="\n\n\n\n\n\nn\nn\n")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "exists; this run reviews and updates it" in output
    assert "[m1234]" in output
    assert read_user_config(path) == {
        ENV_CSTAR_SLURM_ACCOUNT: "m1234",
        ENV_CSTAR_LOG_LEVEL: "DEBUG",
    }
    assert f"# {ENV_CSTAR_SLURM_QUEUE}=" in path.read_text().splitlines()


def test_rerun_with_all_defaults_rewrites_the_file_identically(
    run_init: Callable[..., Result],
    isolate_user_config: Path,
    laptop_data_home: None,
    slurm: None,
) -> None:
    """Accepting every default on a re-run leaves the file byte for byte the same."""
    path = isolate_user_config
    first = run_init(input="m1234\n\n\n/data/my runs\n$HOME/proj\n\nn\nn\n")
    assert first.exit_code == 0, first.output
    before = path.read_text()

    second = run_init(input="\n\n\n\n\n\nn\nn\n")

    assert second.exit_code == 0, second.output
    assert path.read_text() == before
    config = read_user_config(path)
    assert config[ENV_CSTAR_DATA_HOME] == "/data/my runs"
    assert config[ENV_CSTAR_PROJECT_HOME] == os.path.expandvars("$HOME/proj")
    assert f"{ENV_CSTAR_PROJECT_HOME}=$HOME/proj" in before


def test_whitespace_answer_keeps_the_value(
    run_init: Callable[..., Result],
    isolate_user_config: Path,
    laptop_data_home: None,
) -> None:
    """An answer of only spaces is a blank answer, not an empty value."""
    result = run_init(input="   \n\n\nn\nn\n")

    assert result.exit_code == 0, result.output
    assert read_user_config(isolate_user_config) == {}


def test_empty_exported_value_is_not_written(
    run_init: Callable[..., Result],
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A variable exported as empty counts as unset rather than pinning ``NAME=""``."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, "")

    result = run_init("--yes")

    assert result.exit_code == 0, result.output
    assert read_user_config(isolate_user_config) == {}


def test_dash_unsets_a_value_the_file_set(
    run_init: Callable[..., Result],
    isolate_user_config: Path,
    laptop_data_home: None,
    slurm: None,
) -> None:
    """Answering `-` removes a setting the file previously held."""
    path = isolate_user_config
    path.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")

    result = run_init(input="-\n\n\n\n\n\nn\nn\n")

    assert result.exit_code == 0, result.output
    assert read_user_config(path) == {}
    assert f"# {ENV_CSTAR_SLURM_ACCOUNT}=" in path.read_text().splitlines()


def test_undetected_shell_reports_the_manual_command(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    safe_environment: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no shell is detected nothing is installed, and the user is told how."""

    def no_shell() -> Shell:
        raise typer.BadParameter(
            "Unable to choose a supported shell: no shell was detected"
        )

    monkeypatch.setattr(shell_init, "detect_shell", no_shell)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "cstar env shell-init --install" in output
    assert "Shell function: not installed (shell not detected)" in output
    assert safe_environment.installs == []


def test_failed_shell_install_reports_the_manual_command(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An install that exits non-zero is reported, not propagated."""

    def failing_install(shell: Shell) -> None:
        raise typer.Exit(1)

    monkeypatch.setattr(shell_init, "_install", failing_install)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "cstar env shell-init --install" in output
    assert "Shell function: not installed (see above)" in output


def test_kernel_is_skipped_outside_a_conda_environment(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    safe_environment: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a conda prefix the kernel step is skipped without prompting."""

    def no_prefix() -> Path:
        raise RegisterKernelError("not in a conda env")

    monkeypatch.setattr(register_kernel, "_env_prefix", no_prefix)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "Skipped: not in a conda env" in output
    assert "Jupyter kernel: skipped (not in a conda environment)" in output
    assert safe_environment.kernels == []


def test_kernel_registration_failure_is_reported(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registration error is reported with the follow-up command; exit stays 0."""

    def failing(**kwargs: t.Any) -> Path:
        raise RegisterKernelError("ipykernel is not installed")

    monkeypatch.setattr(register_kernel, "register_kernel", failing)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "Error: ipykernel is not installed" in output
    assert "cstar env register-kernel" in output
    assert "Jupyter kernel: not registered (see above)" in output


def test_kernel_write_failure_is_reported(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A kernelspec that cannot be written is reported like any other failure."""

    def failing(**kwargs: t.Any) -> Path:
        raise PermissionError("read-only kernels directory")

    monkeypatch.setattr(register_kernel, "register_kernel", failing)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "Error: read-only kernels directory" in output
    assert "Jupyter kernel: not registered (see above)" in output


def test_unknown_system_skips_scheduler_settings(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrecognised system gets no SLURM prompts, but the file is still written."""
    monkeypatch.setattr("cstar.cli.init.get_system_context", raise_unknown_system)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "System: unknown; scheduler: none" in output
    assert "Unknown system requested: x" in output
    assert "SLURM account" not in output
    assert isolate_user_config.is_file()


def test_recommends_scratch_on_slurm_without_a_scratch_file_system(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    laptop_data_home: None,
    slurm: None,
) -> None:
    """A cluster with no scratch variable gets the recommendation."""
    result = run_init("--yes")

    assert result.exit_code == 0, result.output
    assert "Recommended: no scratch file system was found" in flatten_cli_output(
        result.output
    )


def test_no_scratch_recommendation_on_a_laptop(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    laptop_data_home: None,
) -> None:
    """Without a scheduler there is nothing to recommend."""
    result = run_init("--yes")

    assert result.exit_code == 0, result.output
    assert "Recommended" not in flatten_cli_output(result.output)


def test_scratch_variable_replaces_the_recommendation(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    laptop_data_home: None,
    monkeypatch: pytest.MonkeyPatch,
    slurm: None,
    tmp_path: Path,
) -> None:
    """When $SCRATCH exists it is named as the default and nothing is recommended."""
    monkeypatch.setenv("SCRATCH", str(tmp_path))

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "the scratch variable $SCRATCH" in output
    assert "Recommended" not in output


def test_slurm_hints_list_associations_and_partitions(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
    slurm: None,
) -> None:
    """Associations print with an empty partition shown as any, and `*` is stripped."""

    def query(args: t.Sequence[str]) -> list[str]:
        if args[0] == "sacctmgr":
            return ["m1234|regular", "m5678|"]
        if args[0] == "sinfo":
            return ["regular*", "debug"]
        return []

    monkeypatch.setattr("cstar.cli.init.query_slurm", query)

    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "m1234 regular" in output
    assert "m5678 (any partition)" in output
    assert "Partitions on this system: regular, debug" in output


def test_slurm_without_hints_prints_no_hint_lines(
    run_init: Callable[..., Result],
    flatten_cli_output: Callable[[str], str],
    slurm: None,
) -> None:
    """When the queries return nothing the hint lines are omitted."""
    result = run_init("--yes")
    output = flatten_cli_output(result.output)

    assert result.exit_code == 0, result.output
    assert "Your SLURM associations" not in output
    assert "Partitions on this system" not in output


def test_query_slurm_returns_nothing_when_the_command_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SLURM command that is not installed yields no lines."""
    monkeypatch.setattr("cstar.cli.init.shutil.which", lambda name: None)

    assert REAL_QUERY_SLURM(["sinfo", "-h"]) == []


def test_query_slurm_timeout_does_not_fail_the_command(
    run_init: Callable[..., Result],
    monkeypatch: pytest.MonkeyPatch,
    slurm: None,
) -> None:
    """A slow controller yields no hints, and `init` still succeeds."""

    def timeout(*args: t.Any, **kwargs: t.Any) -> t.NoReturn:
        raise subprocess.TimeoutExpired(cmd="x", timeout=1)

    monkeypatch.setattr("cstar.cli.init.shutil.which", lambda name: "/usr/bin/x")
    monkeypatch.setattr("cstar.cli.init.subprocess.run", timeout)

    assert REAL_QUERY_SLURM(["sinfo", "-h"]) == []

    # `safe_environment` replaced the module's `query_slurm`; put the real one back.
    monkeypatch.setattr("cstar.cli.init.query_slurm", REAL_QUERY_SLURM)
    result = run_init("--yes")

    assert result.exit_code == 0, result.output


def test_init_is_reserved_against_plugins() -> None:
    """A plugin named `init` is skipped; the core command keeps the name."""
    app = typer.Typer()
    eps = [FakeEntryPoint("init", loaded=typer.Typer())]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_subcommands(app)

    assert "init" not in {g.name for g in app.registered_groups}
    assert "init" in [c.name for c in app.registered_commands]


def test_invalid_config_file_exits_without_writing(
    run_init: Callable[..., Result], isolate_user_config: Path
) -> None:
    """The root callback refuses a file naming an unknown setting; it is left alone."""
    path = isolate_user_config
    text = "CSTAR_NOT_A_SETTING=1\n"
    path.write_text(text)

    result = run_init("--yes")

    assert result.exit_code == 2
    assert path.read_text() == text
