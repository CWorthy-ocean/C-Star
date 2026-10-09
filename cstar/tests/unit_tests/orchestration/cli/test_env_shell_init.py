import os
import shutil
import stat
import subprocess
import typing as t
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_LOG_LEVEL
from cstar.cli.environment import app
from cstar.cli.environment.shell_init import COMMAND_SHELL_INIT, Shell

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


@pytest.fixture
def run_session(tmp_path: Path, shell: str, nounset: bool) -> t.Callable[..., Session]:
    """Run commands in a fresh shell that has sourced the function and has a
    stub `cstar` on its PATH.
    """

    def _run(*commands: str, env: dict[str, str] | None = None) -> Session:
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir(exist_ok=True)
        stub = bin_dir / "cstar"
        stub.write_text(STUB)
        stub.chmod(stub.stat().st_mode | stat.S_IXUSR)

        start, target = tmp_path / "start", tmp_path / "target dir"
        start.mkdir(exist_ok=True)
        target.mkdir(exist_ok=True)
        record = tmp_path / "record.txt"
        init = tmp_path / f"init.{shell}"
        init.write_text(CliRunner().invoke(app, [COMMAND_SHELL_INIT, shell]).stdout)

        steps = "\n".join(f'{c}; echo "rc=$?"; echo "pwd=$(pwd -P)"' for c in commands)
        script = (
            f'{NOUNSET[shell] if nounset else ":"}\n. "{init}"\ncd "{start}"\n{steps}\n'
        )
        proc = subprocess.run(
            [shutil.which(shell) or shell, *INVOCATION[shell], script],
            env={
                **{k: v for k, v in os.environ.items() if k != ENV_CSTAR_LOG_LEVEL},
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "STUB_RECORD": str(record),
                "STUB_TARGET": str(target),
                **(env or {}),
            },
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
