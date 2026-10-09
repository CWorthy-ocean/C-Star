import os
import shlex
import shutil
import stat
import subprocess
import typing as t
from pathlib import Path

import pytest
import shellingham
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_CONFIG_HOME, ENV_CSTAR_LOG_LEVEL
from cstar.cli.cli import app as root_app
from cstar.cli.environment import app
from cstar.cli.environment.shell_init import (
    BLOCK_BEGIN,
    BLOCK_END,
    COMMAND_SHELL_INIT,
    Shell,
    shell_function,
)
from cstar.cli.workplan.path import CD_GUIDANCE
from cstar.entrypoint.utils import ARG_INSTALL, ARG_UNINSTALL

SHELLS: t.Final[tuple[str, ...]] = tuple(Shell)
"""Names of the shells the function is generated for; each is also its executable."""
NOUNSET: t.Final[dict[str, str]] = {"bash": "set -u", "zsh": "setopt nounset"}
"""Per shell, the command that makes an unset variable an error."""
INVOCATION: t.Final[dict[str, tuple[str, ...]]] = {
    "bash": ("--norc", "--noprofile", "-c"),
    "zsh": ("-f", "-c"),
}
"""Per shell, the arguments that run a command string without reading rc files."""

STUB = """\
#!/bin/sh
if [ "$1" = workplan ] && [ "$2" = path ]; then
    printf '%s' "${CSTAR_LOG_LEVEL-unset}" >> "$STUB_RECORD"
    for arg in "$@"; do printf '|%s' "$arg" >> "$STUB_RECORD"; done
    printf '\\n' >> "$STUB_RECORD"
    if [ "$3" = missing ]; then
        echo "stub: no such run" >&2
        exit 2
    fi
    echo "$STUB_TARGET"
    exit 0
fi
echo "passthrough: $*"
"""
"""A stand-in `cstar` executable that records `workplan path` lookups."""


class Sandbox(t.NamedTuple):
    """The throwaway locations every test in this module reads and writes."""

    home: Path
    zdotdir: Path
    config: Path
    """The `CSTAR_CONFIG_HOME`, where the function file is written."""

    def rc_file(self, shell: str) -> Path:
        """The rc file `shell` reads under this sandbox."""
        return self.zdotdir / ".zshrc" if shell == Shell.ZSH else self.home / ".bashrc"

    def function_file(self, shell: str) -> Path:
        """Where `--install` writes the function for `shell`."""
        return Path(real(self.config)) / "shell" / f"cstar.{shell}"


@pytest.fixture(autouse=True)
def sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Sandbox:
    """Point every location `--install` could write at `tmp_path`, so that no test
    can touch the real rc files or config directory.
    """
    home = tmp_path / "home"
    home.mkdir()
    locations = Sandbox(home, tmp_path / "zdotdir", tmp_path / "config")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("ZDOTDIR", str(locations.zdotdir))
    monkeypatch.setenv(ENV_CSTAR_CONFIG_HOME, str(locations.config))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    return locations


def set_detection(
    monkeypatch: pytest.MonkeyPatch, running: str | None, login: str | None
) -> list[int]:
    """Choose what each detection source reports; `None` makes it find nothing.

    Returns
    -------
    list[int]
        One entry per call to the running-shell lookup, to show when it was made.
    """
    calls: list[int] = []

    def detect() -> tuple[str, str]:
        calls.append(1)
        if running is None:
            raise shellingham.ShellDetectionFailure
        return running, f"/bin/{running}"

    monkeypatch.setattr(shellingham, "detect_shell", detect)
    if login is None:
        monkeypatch.delenv("SHELL", raising=False)
    else:
        monkeypatch.setenv("SHELL", login)
    return calls


@pytest.fixture(autouse=True)
def nothing_detected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start each test with no running shell and no `$SHELL`, whatever runs pytest."""
    set_detection(monkeypatch, None, None)


@pytest.fixture(params=SHELLS)
def shell(request: pytest.FixtureRequest) -> str:
    """Each supported shell, skipped when it is not installed."""
    name = str(request.param)
    if shutil.which(name) is None:
        pytest.skip(f"{name} is not installed")
    return name


@pytest.fixture(params=[False, True], ids=["default", "nounset"])
def nounset(request: pytest.FixtureRequest) -> bool:
    """Whether the shell treats an unset variable as an error."""
    return bool(request.param)


class Session(t.NamedTuple):
    """The outcome of a shell session that sourced the generated function."""

    proc: subprocess.CompletedProcess[str]
    start: Path
    target: Path
    record: Path

    @property
    def lookups(self) -> list[str]:
        """The recorded `workplan path` invocations as `level|arg|arg...`."""
        return self.record.read_text().splitlines() if self.record.exists() else []


def stub_environment(tmp_path: Path) -> tuple[dict[str, str], Path, Path, Path]:
    """Put the stub `cstar` first on a PATH and create the directories it uses.

    Returns
    -------
    tuple[dict[str, str], Path, Path, Path]
        The environment for a shell, a start directory, the directory the stub
        prints for a lookup, and the file the stub records lookups in.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    stub = bin_dir / "cstar"
    stub.write_text(STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)

    start, target = tmp_path / "start", tmp_path / "target dir"
    start.mkdir(exist_ok=True)
    target.mkdir(exist_ok=True)
    record = tmp_path / "record.txt"
    env = {
        **{k: v for k, v in os.environ.items() if k != ENV_CSTAR_LOG_LEVEL},
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "STUB_RECORD": str(record),
        "STUB_TARGET": str(target),
    }
    return env, start, target, record


@pytest.fixture
def run_session(tmp_path: Path, shell: str, nounset: bool) -> t.Callable[..., Session]:
    """Run commands in a fresh shell that has sourced the function and has a
    stub `cstar` on its PATH.
    """

    def _run(
        *commands: str, env: dict[str, str] | None = None, prelude: str = ""
    ) -> Session:
        stub_env, start, target, record = stub_environment(tmp_path)
        init = tmp_path / f"init.{shell}"
        init.write_text(CliRunner().invoke(app, [COMMAND_SHELL_INIT, shell]).stdout)

        steps = "\n".join(f'{c}; echo "rc=$?"; echo "pwd=$(pwd -P)"' for c in commands)
        script = (
            f"{NOUNSET[shell] if nounset else ':'}\n{prelude}\n"
            f'. "{init}"\ncd "{start}"\n{steps}\n'
        )
        proc = subprocess.run(
            [shutil.which(shell) or shell, *INVOCATION[shell], script],
            env={**stub_env, **(env or {})},
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        return Session(proc, start, target, record)

    return _run


def real(path: Path) -> str:
    """Resolve symlinks (e.g. macOS /tmp) so shell and Python paths compare equal."""
    return os.path.realpath(path)


@pytest.mark.parametrize("name", SHELLS)
def test_shell_init_prints_the_function(name: str) -> None:
    """Verify the command prints a script that defines the `cstar` function and
    nothing else on stdout.
    """
    result = CliRunner().invoke(app, [COMMAND_SHELL_INIT, name])

    assert result.exit_code == 0, result.stderr
    assert f"integration for {name}" in result.stdout.splitlines()[0]
    assert "\ncstar() {\n" in result.stdout
    assert result.stdout.endswith("}\n")
    assert not result.stdout.endswith("\n\n")
    assert result.stderr == ""


def test_shell_init_rejects_an_unsupported_shell() -> None:
    """Verify a shell without support is rejected rather than guessed at."""
    result = CliRunner().invoke(app, [COMMAND_SHELL_INIT, "fish"])

    assert result.exit_code != 0
    assert result.stdout == ""


@pytest.mark.parametrize("command", ["cstar workplan cd x", "cstar wp cd x"])
def test_function_changes_directory(
    run_session: t.Callable[..., Session], command: str
) -> None:
    """Verify the intercepted command ends in the directory the lookup printed,
    pinning the log level so no log record can reach the captured path.
    """
    session = run_session(command, env={ENV_CSTAR_LOG_LEVEL: "DEBUG"})

    assert session.proc.returncode == 0, session.proc.stderr
    assert session.proc.stdout.splitlines() == ["rc=0", f"pwd={real(session.target)}"]
    assert session.lookups == ["WARNING|workplan|path|x"]


def test_function_does_not_leak_the_pinned_log_level(
    run_session: t.Callable[..., Session],
) -> None:
    """Verify the pin applies to the lookup only, not the user's shell."""
    session = run_session(
        "cstar wp cd x",
        f'echo "level=${ENV_CSTAR_LOG_LEVEL}"',
        env={ENV_CSTAR_LOG_LEVEL: "DEBUG"},
    )

    assert "level=DEBUG" in session.proc.stdout.splitlines()


def test_function_forwards_the_step(run_session: t.Callable[..., Session]) -> None:
    """Verify the optional step reaches the lookup and the run-id keeps its spaces."""
    session = run_session('cstar wp cd "my run" spin-up')

    assert session.proc.stdout.splitlines() == ["rc=0", f"pwd={real(session.target)}"]
    assert session.lookups == ["WARNING|workplan|path|my run|spin-up"]


def test_function_stays_put_when_the_lookup_fails(
    run_session: t.Callable[..., Session],
) -> None:
    """Verify a failed lookup reports its status and does not change directory."""
    session = run_session("cstar workplan cd missing")

    assert session.proc.stdout.splitlines() == ["rc=2", f"pwd={real(session.start)}"]
    assert "stub: no such run" in session.proc.stderr


@pytest.mark.parametrize(
    "arguments",
    [
        "wp cd --help",
        "wp cd",
        "wp cd x --verbose",
        "wp cd -x",
        "wp cd x y z",
        "workplan ls",
        "wp ls cd x",
        "env show",
    ],
)
def test_function_passes_everything_else_through(
    run_session: t.Callable[..., Session], arguments: str
) -> None:
    """Verify only `workplan cd <run-id> [step]` is intercepted: any other
    command, including an incomplete or option-bearing one, reaches the
    executable unchanged and leaves the directory alone.
    """
    session = run_session(f"cstar {arguments}")

    assert session.proc.stdout.splitlines() == [
        f"passthrough: {arguments}",
        "rc=0",
        f"pwd={real(session.start)}",
    ]
    assert session.lookups == []


def test_function_leaves_the_path_command_alone(
    run_session: t.Callable[..., Session],
) -> None:
    """Verify `workplan path` reaches the executable with the user's own
    environment, and prints its result instead of changing directory.
    """
    session = run_session("cstar workplan path x")

    assert session.proc.stdout.splitlines() == [
        str(session.target),
        "rc=0",
        f"pwd={real(session.start)}",
    ]
    assert session.lookups == ["unset|workplan|path|x"]


def test_function_passes_a_bare_invocation_through(
    run_session: t.Callable[..., Session],
) -> None:
    """Verify `cstar` with no arguments is not an unbound-variable error."""
    session = run_session("cstar")

    assert session.proc.stdout.splitlines() == [
        "passthrough: ",
        "rc=0",
        f"pwd={real(session.start)}",
    ]


def test_function_replaces_a_cstar_alias(
    run_session: t.Callable[..., Session], shell: str
) -> None:
    """Verify an existing `cstar` alias does not break the function definition."""
    expand = "shopt -s expand_aliases\n" if shell == "bash" else ""
    session = run_session(
        "cstar wp cd x", prelude=f"{expand}alias cstar='echo ALIASED'"
    )

    assert session.proc.stdout.splitlines() == ["rc=0", f"pwd={real(session.target)}"]
    assert session.lookups == ["WARNING|workplan|path|x"]


def test_cd_guidance_names_a_working_setup_command(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the setup command the `cd` guidance prints exists in the CLI and
    sets the shell up, so a renamed command or flag cannot leave it stale.
    """
    set_detection(monkeypatch, None, "/bin/zsh")
    line = next(x for x in CD_GUIDANCE.splitlines() if COMMAND_SHELL_INIT in x)
    args = shlex.split(line)

    assert args[0] == "cstar"
    result = CliRunner().invoke(root_app, args[1:])

    assert result.exit_code == 0, result.stderr
    assert sandbox.function_file("zsh").read_text() == shell_function(Shell.ZSH)
    assert BLOCK_BEGIN in sandbox.rc_file("zsh").read_text()


def source_block(function_file: Path) -> str:
    """The text `--install` adds to an rc file for `function_file`."""
    path = shlex.quote(str(function_file))
    return f"{BLOCK_BEGIN}\nif [ -f {path} ]; then . {path}; fi\n{BLOCK_END}\n"


def setup(*flags: str, shell: str | None = None) -> t.Any:
    """Run `shell-init` with `flags` for `shell` (the detected shell when `None`)."""
    args = [COMMAND_SHELL_INIT, *([shell] if shell else []), *flags]
    return CliRunner().invoke(app, args)


def test_detection_prefers_the_running_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the shell running the command wins over the login shell."""
    set_detection(monkeypatch, "bash", "/bin/zsh")

    result = setup()

    assert result.exit_code == 0, result.stderr
    assert result.stdout == shell_function(Shell.BASH)


def test_detection_falls_back_to_the_login_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `$SHELL` is used when the running shell cannot be detected."""
    set_detection(monkeypatch, None, "/usr/local/bin/zsh")

    assert setup().stdout == shell_function(Shell.ZSH)


def test_detection_skips_an_unsupported_running_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a running shell without support does not hide a supported login shell."""
    set_detection(monkeypatch, "fish", "/bin/bash")

    assert setup().stdout == shell_function(Shell.BASH)


@pytest.mark.parametrize(
    ("running", "login"),
    [("fish", "/usr/bin/fish"), (None, "/bin/sh"), (None, None)],
)
def test_detection_never_guesses(
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
    running: str | None,
    login: str | None,
) -> None:
    """Verify nothing is printed or installed when no candidate is supported, and
    the error names what was found and what is supported.
    """
    set_detection(monkeypatch, running, login)

    result = setup(ARG_INSTALL)

    stderr = flatten_cli_output(result.stderr)
    assert result.exit_code != 0
    assert result.stdout == ""
    assert all(shell in stderr for shell in SHELLS)
    assert all(name in stderr for name in (running, login and Path(login).name) if name)


def test_an_explicit_shell_wins_over_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify naming a shell skips detection altogether."""
    calls = set_detection(monkeypatch, "zsh", "/bin/zsh")

    assert setup(shell="bash").stdout == shell_function(Shell.BASH)
    assert calls == []


def test_help_renders_without_detecting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `--help` neither needs nor runs detection, so it works anywhere."""
    calls = set_detection(monkeypatch, None, None)

    result = setup("--help")

    assert result.exit_code == 0, result.stderr
    assert ARG_INSTALL in result.stdout and ARG_UNINSTALL in result.stdout
    assert calls == []


@pytest.mark.parametrize("name", SHELLS)
def test_install_writes_the_function_and_sources_it(
    sandbox: Sandbox, name: str
) -> None:
    """Verify `--install` writes the function file and adds the rc file block."""
    result = setup(ARG_INSTALL, shell=name)

    function_file = sandbox.function_file(name)
    assert result.exit_code == 0, result.stderr
    assert function_file.read_text() == shell_function(Shell(name))
    assert sandbox.rc_file(name).read_text() == source_block(function_file)
    assert result.stdout.splitlines()[:4] == [
        f"Shell: {name}",
        f"Wrote {function_file}",
        f"Updated {sandbox.rc_file(name)}",
        f"Open a new shell, or run: source {shlex.quote(str(function_file))}",
    ]


def test_install_detects_the_shell(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `--install` without a shell name installs for the detected shell."""
    set_detection(monkeypatch, "bash", None)

    result = setup(ARG_INSTALL)

    assert result.exit_code == 0, result.stderr
    assert sandbox.function_file("bash").exists()
    assert not sandbox.function_file("zsh").exists()
    assert BLOCK_BEGIN in sandbox.rc_file("bash").read_text()


@pytest.mark.parametrize("name", SHELLS)
def test_install_twice_leaves_one_block(sandbox: Sandbox, name: str) -> None:
    """Verify re-running `--install` refreshes the function and keeps one block."""
    setup(ARG_INSTALL, shell=name)
    sandbox.function_file(name).write_text("stale")

    result = setup(ARG_INSTALL, shell=name)

    rc = sandbox.rc_file(name)
    assert rc.read_text().count(BLOCK_BEGIN) == 1
    assert rc.read_text() == source_block(sandbox.function_file(name))
    assert sandbox.function_file(name).read_text() == shell_function(Shell(name))
    assert f"{rc} already has the block" in result.stdout


@pytest.mark.parametrize("name", SHELLS)
def test_install_keeps_existing_rc_content(sandbox: Sandbox, name: str) -> None:
    """Verify `--install` appends after a blank line and leaves every existing byte."""
    rc = sandbox.rc_file(name)
    rc.parent.mkdir(parents=True, exist_ok=True)
    before = b"export A=1\r\nalias x='\xff'\n"
    rc.write_bytes(before)

    setup(ARG_INSTALL, shell=name)

    block = source_block(sandbox.function_file(name)).encode()
    assert rc.read_bytes() == before + b"\n" + block


def test_install_completes_a_last_line_without_a_newline(sandbox: Sandbox) -> None:
    """Verify the block starts on its own line when the rc file has no final newline."""
    rc = sandbox.rc_file("zsh")
    rc.parent.mkdir(parents=True)
    rc.write_text("export A=1")

    setup(ARG_INSTALL, shell="zsh")

    assert rc.read_text() == "export A=1\n\n" + source_block(
        sandbox.function_file("zsh")
    )


def test_install_updates_the_block_in_place(
    sandbox: Sandbox, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a moved config directory rewrites the block where it is, not elsewhere."""
    setup(ARG_INSTALL, shell="zsh")
    rc = sandbox.rc_file("zsh")
    rc.write_text(f"# before\n{rc.read_text()}# after\n")
    monkeypatch.setenv(ENV_CSTAR_CONFIG_HOME, str(tmp_path / "moved"))
    moved = Sandbox(sandbox.home, sandbox.zdotdir, tmp_path / "moved")

    result = setup(ARG_INSTALL, shell="zsh")

    assert rc.read_text() == (
        "# before\n" + source_block(moved.function_file("zsh")) + "# after\n"
    )
    assert f"Updated {rc}" in result.stdout


def test_install_writes_through_a_symlinked_rc_file(
    sandbox: Sandbox, tmp_path: Path
) -> None:
    """Verify a dotfile-manager symlink survives and its target gets the block."""
    target = tmp_path / "dotfiles" / "zshrc"
    target.parent.mkdir()
    target.write_text("export A=1\n")
    rc = sandbox.rc_file("zsh")
    rc.parent.mkdir(parents=True)
    rc.symlink_to(target)

    setup(ARG_INSTALL, shell="zsh")

    assert rc.is_symlink()
    assert rc.readlink() == target
    assert target.read_text() == "export A=1\n\n" + source_block(
        sandbox.function_file("zsh")
    )


@pytest.mark.parametrize(
    ("name", "platform", "note"),
    [
        ("bash", "darwin", True),
        ("bash", "linux", False),
        ("zsh", "darwin", False),
    ],
)
def test_install_notes_the_macos_bash_login_shell(
    monkeypatch: pytest.MonkeyPatch, name: str, platform: str, note: bool
) -> None:
    """Verify only bash on macOS is told about `~/.bash_profile`."""
    monkeypatch.setattr("sys.platform", platform)

    result = setup(ARG_INSTALL, shell=name)

    assert (".bash_profile" in result.stdout) is note


@pytest.mark.parametrize("name", SHELLS)
def test_uninstall_restores_the_rc_file(sandbox: Sandbox, name: str) -> None:
    """Verify `--uninstall` removes the block and function file and nothing else."""
    rc = sandbox.rc_file(name)
    rc.parent.mkdir(parents=True, exist_ok=True)
    before = b"export A=1\r\nalias x='\xff'\n"
    rc.write_bytes(before)
    setup(ARG_INSTALL, shell=name)

    result = setup(ARG_UNINSTALL, shell=name)

    assert result.exit_code == 0, result.stderr
    assert rc.read_bytes() == before
    assert not sandbox.function_file(name).exists()
    assert f"Removed {sandbox.function_file(name)}" in result.stdout
    assert "unset -f cstar" in result.stdout


def test_uninstall_keeps_content_around_the_block(sandbox: Sandbox) -> None:
    """Verify content the user added after the block stays when it is removed."""
    setup(ARG_INSTALL, shell="zsh")
    rc = sandbox.rc_file("zsh")
    rc.write_text(f"# before\n{rc.read_text()}# after\n")

    setup(ARG_UNINSTALL, shell="zsh")

    assert rc.read_text() == "# before\n# after\n"


def test_uninstall_when_nothing_is_installed(sandbox: Sandbox) -> None:
    """Verify `--uninstall` succeeds, creating nothing, when there is nothing to remove."""
    result = setup(ARG_UNINSTALL, shell="zsh")

    assert result.exit_code == 0, result.stderr
    assert "Nothing was installed" in result.stdout
    assert not sandbox.rc_file("zsh").exists()


def test_install_and_uninstall_cannot_be_combined(
    sandbox: Sandbox, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify both flags together are rejected, naming both, before anything is written."""
    result = setup(ARG_INSTALL, ARG_UNINSTALL, shell="zsh")

    stderr = flatten_cli_output(result.stderr)
    assert result.exit_code != 0
    assert ARG_INSTALL in stderr and ARG_UNINSTALL in stderr
    assert not sandbox.function_file("zsh").exists()


def test_installed_rc_file_defines_the_function(
    tmp_path: Path, shell: str, sandbox: Sandbox
) -> None:
    """Verify a fresh shell that reads the installed rc file can `cstar wp cd`."""
    stub_env, start, target, _ = stub_environment(tmp_path)
    assert setup(ARG_INSTALL, shell=shell).exit_code == 0
    script = (
        f'. "{sandbox.rc_file(shell)}"\ncd "{start}"\n'
        'cstar wp cd x; echo "rc=$?"; pwd -P\n'
    )

    proc = subprocess.run(
        [shutil.which(shell) or shell, *INVOCATION[shell], script],
        env=stub_env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert proc.stdout.splitlines() == ["rc=0", real(target)], proc.stderr
