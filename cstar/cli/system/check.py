import shutil
import threading
import typing as t
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_SYSTEM, load_user_config_for_repair
from cstar.base.exceptions import CstarError
from cstar.cli.system.add import looks_like_path
from cstar.roms.build_verification import (
    _DECLARED_PREFIX_VARS,
    empty_prefix_vars,
    mpifort_under,
)
from cstar.system.environment import CStarEnvironment
from cstar.system.layers import world_writable
from cstar.system.manager import (
    detection_would_select,
    registered_context,
    registered_names,
    select_system,
    system_file_of,
    system_stack,
)
from cstar.system.scheduler import VISIBLE_SCHEDULERS, SlurmQueue
from cstar.system.spec import (
    DRAFT_LAYER,
    SYSTEM_FILE_SUFFIX,
    WALLTIME_QUERIES,
    SystemFile,
    SystemFileError,
    read_system_file,
)
from cstar.system.toolchain import ToolchainName

if t.TYPE_CHECKING:
    from collections.abc import Callable

    from cstar.system.scheduler import Queue
    from cstar.system.spec import SystemSpec, ToolchainSpec

app = typer.Typer()

STATUS_PASS: t.Final = "PASS"
"""The step ran here and succeeded."""
STATUS_FAIL: t.Final = "FAIL"
"""The step ran here and found a problem."""
STATUS_NOT_CHECKED: t.Final = "NOT CHECKED"
"""The step cannot be run from here, or its answer is not a problem."""

QUERY_TIMEOUT_SECONDS: t.Final = 10.0
"""How long to wait for a queue's walltime query before calling it failed."""

TOOLCHAIN_ORDER: t.Final = t.get_args(ToolchainName)
"""The toolchains a system file may describe, in the order they are checked."""

NOT_CHECKED_HERE: t.Final = (
    "whether the detection rule holds inside a job (on the compute nodes)",
    "a build and a job, which `cstar system check` does not run",
)
"""What `cstar system check` leaves to the user, reported after every check."""


@dataclass(frozen=True)
class CheckResult:
    """The outcome of one step of `cstar system check`."""

    step: str
    """What was checked."""
    status: str
    """`STATUS_PASS`, `STATUS_FAIL` or `STATUS_NOT_CHECKED`."""
    detail: str = ""
    """What was found, or why the step could not run."""


def _guarded(step: str, run: "Callable[[], list[CheckResult]]") -> list[CheckResult]:
    """Run one step, reporting an unexpected exception as the step's failure."""
    try:
        return run()
    except Exception as ex:
        return [CheckResult(step, STATUS_FAIL, f"{type(ex).__name__}: {ex}")]


_QUERY_COMMANDS: t.Final = {
    query: command for command, query in WALLTIME_QUERIES.items()
}
"""The command behind each walltime query a queue may run; a fixed walltime has none."""


def _walltime(queue: "Queue") -> str | None:
    """Ask a queue for its maximum walltime."""
    return queue.max_walltime


def _call_with_timeout(run: "Callable[[], str | None]", timeout: float) -> str | None:
    """Return what `run` returns, raising what it raises.

    Runs in a daemon thread so that a command that never answers cannot keep
    the process from exiting.

    Raises
    ------
    TimeoutError
        If `run` has not finished after `timeout` seconds.
    """
    outcome: list[str | None | BaseException] = []

    def target() -> None:
        try:
            outcome.append(run())
        except BaseException as ex:
            outcome.append(ex)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    if not outcome:
        raise TimeoutError(f"no answer after {timeout:g} seconds")
    if isinstance(result := outcome[0], BaseException):
        raise result
    return result


def read_system_to_check(argument: str | None) -> tuple[SystemFile | None, CheckResult]:
    """Read the file `cstar system check` was asked about.

    Parameters
    ----------
    argument : str | None
        A path to a draft, the name of a system, or `None` for the system in use.

    Returns
    -------
    tuple[SystemFile | None, CheckResult]
        The file, `None` when it cannot be read, and the schema step's result.
    """
    step = "schema"
    if argument is not None and looks_like_path(argument):
        try:
            file = read_system_file(Path(argument), DRAFT_LAYER, check_name=False)
        except SystemFileError as ex:
            return None, CheckResult(step, STATUS_FAIL, ex.reason)
        return file, CheckResult(step, STATUS_PASS, f"{file.path} is valid")

    try:
        context = (
            select_system().context
            if argument is None
            else registered_context(argument)
        )
    except (CstarError, OSError) as ex:
        return None, CheckResult(step, STATUS_FAIL, str(ex))
    found = system_file_of(context)
    if found is None:
        detail = f"the system {context.name!r} is defined in code: it has no file"
        return None, CheckResult(step, STATUS_FAIL, detail)
    return found, CheckResult(
        step, STATUS_PASS, f"{found.path} is valid ({found.layer})"
    )


def check_installation(file: SystemFile) -> list[CheckResult]:
    """Report how a draft file would install, and whether C-Star would refuse it.

    Parameters
    ----------
    file : SystemFile
        The file read; only a draft is examined, since the files in the layers
        were refused or accepted when they were read.

    Returns
    -------
    list[CheckResult]
        Nothing for a file already in a layer.
    """
    if file.layer != DRAFT_LAYER:
        return []
    results = []
    name = file.spec.name
    step = "installs as"
    installed = f"{name}{SYSTEM_FILE_SUFFIX}"
    detail = f"{installed} in a systems/ directory"
    if file.path.name != installed:
        detail += f" (this file is named {file.path.name})"
    results.append(CheckResult(step, STATUS_PASS, detail))

    if (below := system_stack().layers.files.get(name)) is not None:
        what = f"the {below.layer} system file {below.path}"
    elif name in registered_names():
        what = "a system registered in code"
    else:
        what = ""
    if what:
        results.append(
            CheckResult("shadowing", STATUS_PASS, f"installed, it would shadow {what}")
        )

    try:
        if world_writable(file.path):
            results.append(
                CheckResult(
                    "permissions",
                    STATUS_FAIL,
                    "C-Star refuses a world-writable system file; "
                    f"run `chmod o-w {file.path}`",
                )
            )
    except OSError as ex:
        results.append(CheckResult("permissions", STATUS_FAIL, str(ex)))
    return results


def check_prefixes(
    label: str, toolchain: "ToolchainSpec", env: CStarEnvironment
) -> list[CheckResult]:
    """Check the prefixes and tools a loaded toolchain leaves in the process.

    Parameters
    ----------
    label : str
        The toolchain's name, for the step names.
    toolchain : ToolchainSpec
        The toolchain's entry in the system file.
    env : CStarEnvironment
        The toolchain, loaded.

    Returns
    -------
    list[CheckResult]
        The prefix variables, the MPI wrapper (for the `gnu` compiler) and
        `nf-config`.
    """
    env_vars = env.environment_variables
    problems = [f"{var} is empty" for var in empty_prefix_vars(env_vars)]
    problems += [
        f"{var}={value} has no lib/ directory"
        for var in _DECLARED_PREFIX_VARS
        if (value := env_vars.get(var, "").strip())
        and not (Path(value) / "lib").is_dir()
    ]
    results = [
        CheckResult(
            f"prefixes ({label})",
            STATUS_FAIL if problems else STATUS_PASS,
            "; ".join(problems) or "every declared prefix holds a lib/ directory",
        )
    ]
    if toolchain.compiler == "gnu" and (
        mpi_home := env_vars.get("MPIHOME", "").strip()
    ):
        wrapper = mpifort_under(toolchain.compiler, mpi_home)
        results.append(
            CheckResult(
                f"mpi wrapper ({label})",
                STATUS_PASS if wrapper else STATUS_FAIL,
                wrapper or f"{mpi_home}/bin/mpifort does not exist",
            )
        )
    nf_config = shutil.which("nf-config")
    results.append(
        CheckResult(
            f"nf-config ({label})",
            STATUS_PASS if nf_config else STATUS_FAIL,
            nf_config or "not on PATH",
        )
    )
    return results


def check_toolchain(
    label: str, toolchain: "ToolchainSpec", spec: "SystemSpec", source: Path
) -> list[CheckResult]:
    """Load one toolchain as a job would, and check what it provides.

    Parameters
    ----------
    label : str
        The toolchain's name (`cpu` or `gpu`).
    toolchain : ToolchainSpec
        The toolchain's entry in the system file.
    spec : SystemSpec
        The system the file describes.
    source : Path
        The system file, named in messages.

    Returns
    -------
    list[CheckResult]
        The modules step, then the prefix steps when the modules loaded or none
        were listed. The toolchain is unloaded afterwards.
    """
    step = f"modules ({label})"
    env: CStarEnvironment | None = None
    try:
        env = CStarEnvironment(
            system_name=spec.name,
            mpi_exec_prefix=toolchain.mpi_prefix,
            compiler=toolchain.compiler,
            profile="" if label == "cpu" else label,
            modules=toolchain.modules,
            env=toolchain.env,
            source=source,
        )
    except Exception as ex:
        return [CheckResult(step, STATUS_FAIL, f"{type(ex).__name__}: {ex}")]

    try:
        if not toolchain.modules:
            modules = CheckResult(step, STATUS_PASS, "no modules to load")
        elif not env.uses_lmod:
            reason = "Lmod is not available here"
            return [
                CheckResult(step, STATUS_NOT_CHECKED, reason),
                CheckResult(
                    f"prefixes ({label})",
                    STATUS_NOT_CHECKED,
                    "the modules were not loaded",
                ),
            ]
        else:
            modules = CheckResult(
                step, STATUS_PASS, f"loaded {len(toolchain.modules)} modules"
            )
        results = [modules]
        try:
            results += check_prefixes(label, toolchain, env)
        except Exception as ex:
            results.append(
                CheckResult(
                    f"prefixes ({label})", STATUS_FAIL, f"{type(ex).__name__}: {ex}"
                )
            )
        return results
    finally:
        env.unload()


def check_scheduler(
    spec: "SystemSpec", timeout: float | None = None
) -> list[CheckResult]:
    """Check that the scheduler's commands exist and its queues answer.

    Parameters
    ----------
    spec : SystemSpec
        The system the file describes.
    timeout : float, optional
        Seconds to wait for each walltime query; defaults to
        `QUERY_TIMEOUT_SECONDS`.

    Returns
    -------
    list[CheckResult]
        The submit command, then each queue's walltime. Empty without a scheduler.
    """
    if (scheduler := spec.scheduler) is None:
        return []
    timeout = QUERY_TIMEOUT_SECONDS if timeout is None else timeout
    family = scheduler.family.upper()
    submit = next(cmd for name, cmd, _ in VISIBLE_SCHEDULERS if name == family)
    found = shutil.which(submit)
    results = [
        CheckResult(
            "scheduler",
            STATUS_PASS if found else STATUS_FAIL,
            found or f"{submit} is not on PATH: run this on the cluster's login node",
        )
    ]
    built = scheduler.build(spec.docs).queues
    for i, queue_spec in enumerate(scheduler.queues):
        queue = built[i]
        step = f"queue {queue_spec.name}"
        # a PBS queue's walltime is fixed; a SLURM queue's may be a query
        command = (
            _QUERY_COMMANDS.get(queue.walltime_method, "")
            if isinstance(queue, SlurmQueue)
            else ""
        )
        if command and not shutil.which(command):
            detail = f"{command} is not on PATH, so the walltime cannot be queried"
            results.append(CheckResult(step, STATUS_NOT_CHECKED, detail))
            continue
        try:
            walltime = _call_with_timeout(partial(_walltime, queue), timeout)
        except Exception as ex:
            detail = f"the walltime query failed: {type(ex).__name__}: {ex}"
            results.append(CheckResult(step, STATUS_FAIL, detail))
            continue
        if walltime is None and command:
            detail = f"{command} reported no walltime limit"
            results.append(CheckResult(step, STATUS_FAIL, detail))
        else:
            results.append(
                CheckResult(step, STATUS_PASS, f"maximum walltime {walltime}")
            )
    return results


def check_detection(spec: "SystemSpec") -> CheckResult:
    """Say whether detection picks the system on this machine."""
    step = "detection"
    if detection_would_select(spec):
        return CheckResult(step, STATUS_PASS, f"detection selects {spec.name} here")
    return CheckResult(
        step,
        STATUS_NOT_CHECKED,
        f"detection does not select {spec.name} here; set it with "
        f"`cstar env set {ENV_CSTAR_SYSTEM} {spec.name}`",
    )


def check_scratch(spec: "SystemSpec") -> list[CheckResult]:
    """Check that the system's scratch rule finds a directory here."""
    if spec.scratch is None:
        return []
    found = spec.scratch.resolve()
    if found is None:
        return [
            CheckResult(
                "scratch", STATUS_FAIL, f"{spec.scratch.match} matches no directory"
            )
        ]
    return [CheckResult("scratch", STATUS_PASS, str(found))]


def check_system(argument: str | None) -> list[CheckResult]:
    """Run every check on a system file, from this machine.

    Parameters
    ----------
    argument : str | None
        A path to a draft, the name of a system, or `None` for the system in use.

    Returns
    -------
    list[CheckResult]
        One result per step, ending with what could not be checked. Only the
        schema step when the file cannot be read.
    """
    file, schema = read_system_to_check(argument)
    results = [schema]
    if file is None:
        return results
    spec = file.spec
    results += check_installation(file)
    for label in TOOLCHAIN_ORDER:
        if label in spec.toolchains:
            results += check_toolchain(label, spec.toolchains[label], spec, file.path)
    results += _guarded("scheduler", lambda: check_scheduler(spec))
    results += _guarded("detection", lambda: [check_detection(spec)])
    results += _guarded("scratch", lambda: check_scratch(spec))
    results += [
        CheckResult("beyond this check", STATUS_NOT_CHECKED, d)
        for d in NOT_CHECKED_HERE
    ]
    return results


def format_result(result: CheckResult) -> str:
    """Spell one result as a line of the report."""
    line = f"{result.status:<{len(STATUS_NOT_CHECKED)}}  {result.step}"
    return f"{line}: {result.detail}" if result.detail else line


@app.command(name="check", help="Test a system file on this machine.")
def check(
    system: t.Annotated[
        str | None,
        typer.Argument(
            help="A system's name or a system file's path; omitted, the system in use."
        ),
    ] = None,
) -> None:
    """Test a system file the way a cluster run would use it.

    Loads each toolchain's modules, checks the prefixes, tools, scheduler,
    queues and scratch directory it names, and says whether detection selects the
    system here. Run it on the cluster's login node. Each step is PASS, FAIL, or
    NOT CHECKED with the reason; the command exits 1 if any step fails.

    Parameters
    ----------
    system : str | None
        A path to a system file (a draft need not be named after its system), the
        name of a system, or nothing for the system in use.

    Raises
    ------
    typer.Exit
        Code 1 if any step fails, otherwise 0.
    """
    load_user_config_for_repair()
    results = check_system(system)
    for result in results:
        typer.echo(format_result(result))
    counts = {
        status: sum(r.status == status for r in results)
        for status in (STATUS_PASS, STATUS_FAIL, STATUS_NOT_CHECKED)
    }
    typer.echo(
        f"\n{counts[STATUS_PASS]} passed, {counts[STATUS_FAIL]} failed, "
        f"{counts[STATUS_NOT_CHECKED]} not checked"
    )
    raise typer.Exit(1 if counts[STATUS_FAIL] else 0)
