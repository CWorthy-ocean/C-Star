"""`cstar init`: the guided setup that follows an install.

One command walks through everything the installation page lists: the
scheduler settings on a cluster, where C-Star puts its data, the
configuration file that records those choices, the shell function behind
`cstar run cd`, a Jupyter kernel for the environment, C-Star's skills for any
coding agent, and the datasets that need a registration. Every prompt
defaults to the value already in effect, so a re-run reviews the setup rather
than redoing it.
"""

import os
import shutil
import subprocess
import textwrap
import typing as t
from dataclasses import dataclass

import typer
from dotenv import dotenv_values

import cstar
from cstar.base.env import (
    ENV_CSTAR_CATALOG,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_PROJECT_HOME,
    ENV_CSTAR_SCRATCH_DIRS,
    ENV_CSTAR_SYSTEM,
    EnvItem,
    UserConfig,
    catalog_roots,
    default_catalog_root,
    get_env_item,
    hpc_data_directory,
    load_user_config_for_repair,
    parse_user_config,
    render_user_config,
    split_catalog,
)
from cstar.base.exceptions import CstarError, CstarUserConfigError
from cstar.base.skills import (
    SkillError,
    agent_homes,
    default_install_dirs,
    shipped_skills,
)
from cstar.cli.environment import NAME as NAME_ENV
from cstar.cli.environment import register_kernel, shell_init
from cstar.cli.environment.show import (
    SOURCE_CONFIG,
    SOURCE_DEFAULT,
    SOURCE_ENV,
    value_source,
)
from cstar.cli.run import NAME as NAME_RUN
from cstar.cli.skill.install import describe_failure, install_skills
from cstar.cli.skill.verbs import COMMAND_INSTALL as COMMAND_INSTALL_SKILL
from cstar.cli.skill.verbs import NAME as NAME_SKILL
from cstar.cli.system.add import looks_like_path, read_draft
from cstar.cli.workplan.path import COMMAND_CD
from cstar.entrypoint.utils import (
    ARG_INSTALL,
    ARG_SYSTEM_FILE,
    ARG_SYSTEM_FILE_HELP,
    ARG_YES,
)
from cstar.execution.file_system import DirectoryManager, write_bytes_atomic
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_GPU_ACCOUNT,
    ENV_CSTAR_SLURM_GPU_QUEUE,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)
from cstar.system.environment import SlurmSettingsBase
from cstar.system.layers import SystemFileConflict, install_system_file
from cstar.system.manager import (
    DOCS_DESCRIBE_CLUSTER_URL,
    SYSTEM_SKILL_HINT,
    SystemSelection,
    SystemSource,
    context_from_file,
    detect_system,
    detection_would_select,
    display_path,
    registered_context,
    registered_names,
    reload_system_files,
    select_system,
)
from cstar.system.scheduler import PBSScheduler, SlurmScheduler
from cstar.system.spec import (
    SYSTEMS_DIR_NAME,
    SystemFile,
    SystemFileError,
    SystemSpec,
    current_user,
)

if t.TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from cstar.system.manager import SystemContext
    from cstar.system.scheduler import Scheduler

COMMAND_INIT: t.Final[str] = "init"
"""Name of the root command that runs the guided setup (reserved against plugins)."""

HELP_SHORT: t.Final[str] = (
    "Set C-Star up after installing it: scheduler, locations, shell, kernel, "
    "agent skills."
)
HELP_LONG: t.Final[str] = f"""\
{HELP_SHORT}

On a cluster, asks for the SLURM account, queue and default walltime. Shows where C-Star will put run output, the shared source-data cache and your catalog entries, asks whether to use them, and lets you change each if you answer no. Writes the answers to the configuration file, then offers to install the shell function that makes `cstar {NAME_RUN} {COMMAND_CD}` work, to register a Jupyter kernel for this environment, and, when it finds a coding agent's folder in your home directory, to install C-Star's skills for it (`cstar {NAME_SKILL} {COMMAND_INSTALL_SKILL}`).

{ARG_SYSTEM_FILE} PATH sets the setup up for the machine that a system file describes, and settles where C-Star will find the file.

Every prompt defaults to the value in effect now, so re-running the command reviews and updates the setup. {ARG_YES} accepts every default without asking, for containers and scripts.
"""

UNSET_ANSWER: t.Final[str] = "-"
"""The answer that leaves a setting unset when a value is offered as the default."""
PROMPT_HINT: t.Final[str] = (
    f"Press Enter to keep the value in brackets; type {UNSET_ANSWER} to leave a "
    "setting unset."
)
"""The line that explains how to answer the prompts that follow it."""
SKILL_WIDTH: t.Final[int] = 76
"""Columns a shipped skill's description is wrapped to in the Agent skills section."""
HINT_TIMEOUT: t.Final[float] = 5.0
"""Seconds to wait for a SLURM lookup made only to show a hint."""
DOCS_PRECEDENCE_URL: t.Final[str] = (
    "https://c-star.readthedocs.io/en/latest/getting_started/configuration.html"
    "#where-a-setting-can-be-set-and-which-wins"
)
"""The documentation section on where a setting can be set and which wins."""
DOCS_DATA_ACCESS_URL: t.Final[str] = (
    "https://c-star.readthedocs.io/en/latest/getting_started/data_access.html"
)
"""The documentation page on registering for datasets."""

SOURCE_LABELS: t.Final[dict[str, str]] = {
    SOURCE_ENV: "exported in your shell",
    SOURCE_CONFIG: "from the configuration file",
}
"""How a value that `value_source` attributes to the environment is described."""
SOURCE_BUILTIN: t.Final[str] = "the built-in default"
SOURCE_NOT_SET: t.Final[str] = "not set"

LOCATION_PURPOSES: t.Final[dict[str, str]] = {
    ENV_CSTAR_DATA_HOME: "run output and generated inputs",
    ENV_CSTAR_PROJECT_HOME: (
        "durable project area for the shared source-data cache and promoted runs"
    ),
    ENV_CSTAR_CATALOG: "the catalog entries you register for Forge",
}
"""One line on what each location prompted for is used for."""


@dataclass(frozen=True, slots=True)
class Resolved:
    """A setting's value in effect and where it came from."""

    item: EnvItem
    """The setting."""
    value: str
    """The value in effect, as shown to the user; empty when unset."""
    source: str
    """`SOURCE_ENV`, `SOURCE_CONFIG` or `SOURCE_DEFAULT`, as `value_source` reports."""
    origin: str
    """Where the value came from, as a phrase for display."""

    @property
    def explicit(self) -> bool:
        """Whether the value was set by the user rather than resolved from a default."""
        return self.source != SOURCE_DEFAULT


@dataclass(frozen=True, slots=True)
class Decision:
    """What the user chose for one setting."""

    resolved: Resolved
    """The setting and its value before the prompt."""
    kept: bool
    """Whether the value in effect was accepted as it is."""
    value: str | None = None
    """The new value when not kept; `None` leaves the setting unset."""

    def apply(self, values: dict[str, str]) -> None:
        """Record the decision in the settings the file will hold.

        A kept value is written only when the user set it: one exported in the
        shell moves into the file (an empty export counts as unset), one the file
        supplied stays as the file spelled it, and a default is left as a
        commented line so the file never pins a value that the system resolves.
        """
        name = self.resolved.item.name
        if not self.kept:
            if self.value is None:
                values.pop(name, None)
            else:
                values[name] = self.value
        elif self.resolved.source == SOURCE_ENV and os.environ[name]:
            values[name] = os.environ[name]
        elif self.resolved.source != SOURCE_CONFIG:
            values.pop(name, None)


def echo(message: str = "", indent: int = 2) -> None:
    """Print a line of the walkthrough, indented under its section header."""
    typer.echo(f"{' ' * indent}{message}" if message else "")


def header(title: str) -> None:
    """Print a section header after a blank line."""
    typer.echo()
    typer.secho(title, bold=True)


def confirm(question: str, yes: bool, default: bool = True) -> bool:
    """Ask a yes/no question, answered yes under `--yes`."""
    return yes or typer.confirm(f"  {question}", default=default)


def ask(label: str, resolved: Resolved, yes: bool) -> Decision:
    """Prompt for a setting with its value in effect as the default.

    A blank answer, or one equal to the value shown, keeps that value; the
    `UNSET_ANSWER` leaves the setting unset; anything else is the new value.
    Under `--yes` every value is kept.
    """
    if yes:
        return Decision(resolved, kept=True)

    answer = typer.prompt(
        f"  {label}",
        default=resolved.value,
        show_default=bool(resolved.value),
    ).strip()
    if answer == UNSET_ANSWER:
        return Decision(resolved, kept=False, value=None)
    if not answer or answer == resolved.value:
        return Decision(resolved, kept=True)
    return Decision(resolved, kept=False, value=answer)


def describe_default(item: EnvItem, context: "SystemContext | None") -> str:
    """Say which default applied to a setting that neither the shell nor the file set."""
    if item.name == ENV_CSTAR_DATA_HOME:
        names = get_env_item(ENV_CSTAR_SCRATCH_DIRS).value.split(",")
        if scratch := next((n for n in names if os.environ.get(n)), None):
            return f"the scratch variable ${scratch}"
        if context is not None and context.scratch_root() is not None:
            return "the system's scratch convention"
    if item.indirect_var and os.environ.get(item.indirect_var):
        return f"the {item.indirect_var} variable"
    return SOURCE_BUILTIN if item.value else SOURCE_NOT_SET


def resolve(name: str, config: UserConfig, context: "SystemContext | None") -> Resolved:
    """Determine a setting's value in effect and where it came from.

    The directory settings show the directory C-Star will actually use (the
    ``cstar`` subdirectory included), as `cstar forge show-paths` would.
    """
    item = get_env_item(name)
    if name == ENV_CSTAR_DATA_HOME:
        value = DirectoryManager.data_home().as_posix()
    elif name == ENV_CSTAR_PROJECT_HOME:
        home = DirectoryManager.project_home()
        value = home.as_posix() if home else ""
    else:
        value = item.value

    source = value_source(item, config)
    origin = SOURCE_LABELS.get(source) or describe_default(item, context)
    return Resolved(item, value, source, origin)


def query_slurm(args: "Sequence[str]") -> list[str]:
    """Run a SLURM query for a hint; the lines it printed, or nothing.

    Best effort: a missing command, a failure, or a slow response (the
    controller may be busy) yields an empty list rather than an error.
    """
    if shutil.which(args[0]) is None:
        return []
    try:
        result = subprocess.run(
            list(args),
            capture_output=True,
            text=True,
            timeout=HINT_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def slurm_hints() -> None:
    """Show the user's SLURM associations and the partitions, when they can be queried."""
    user = current_user()
    assoc = query_slurm(
        ["sacctmgr", "-nP", "show", "assoc", f"user={user}", "format=Account,Partition"]
    )
    if assoc:
        echo("Your SLURM associations (account, partition):")
        for line in assoc:
            account, _, partition = line.partition("|")
            echo(f"{account}  {partition or '(any partition)'}", indent=4)
    partitions = query_slurm(["sinfo", "-h", "-o", "%P"])
    if partitions:
        names = ", ".join(p.rstrip("*") for p in partitions)
        echo(f"Partitions on this system: {names}")


def required_slurm_settings(context: "SystemContext") -> set[str]:
    """The SLURM settings the system's settings class refuses to run without."""
    klass = context.settings_klass()
    if klass is None or not issubclass(klass, SlurmSettingsBase):
        return set()
    return klass.required_settings()


def describe_file(system_file: SystemFile) -> str:
    """Name a system and the file that describes it, for display."""
    return f"{system_file.spec.name} (from {display_path(system_file.path)})"


def system_section(
    selection: SystemSelection | None,
    problem: str,
    config: UserConfig,
    yes: bool,
) -> tuple[Decision | None, SystemSelection | None, SystemFile | None]:
    """Offer to name the system when C-Star fell back to a generic context or
    could not choose one.

    Enter keeps the system in effect, so it never pins the generic context, and
    `--yes` keeps it too. A name must be a registered system; an unknown one,
    including a `CSTAR_SYSTEM` in effect that names none, is asked again, and
    `UNSET_ANSWER` removes the setting and detects the system afresh. Under
    `--yes` such a setting is left where it is, never moved into the file. An
    answer that is the path of a file is read as a system file; one that is
    refused is asked again, one that is accepted describes the system for the
    rest of the setup and is placed once the locations are settled.

    Parameters
    ----------
    selection : SystemSelection | None
        The system C-Star chose, or `None` when it could not choose one.
    problem : str
        Why no system could be chosen; empty when `selection` is not `None`.
    config : UserConfig
        What the loader exported from the configuration file.
    yes : bool
        Whether to keep the system in effect without asking.

    Returns
    -------
    tuple[Decision | None, SystemSelection | None, SystemFile | None]
        The decision on `CSTAR_SYSTEM` (`None` to leave the file's line as it is),
        the system the rest of the setup uses, and the system file the user gave
        in place of a name, if any.
    """
    header("System")
    if selection is None:
        echo(problem)
    else:
        echo(
            f"C-Star does not recognise {selection.detected!r} and uses the generic "
            f"{selection.context.name} context, which submits no jobs."
        )
    echo(f"Registered systems: {', '.join(registered_names())}")
    echo("To describe this machine to C-Star, write a system file (see")
    echo(f"{DOCS_DESCRIBE_CLUSTER_URL}) and give its path here.")
    echo(SYSTEM_SKILL_HINT)
    if selection is None:
        echo("Name the system to use.")
    else:
        echo("If this machine is one of them, name it. To keep the generic context")
        echo(
            f"without the warning other commands print, enter {selection.context.name}."
        )
    if selection is not None and (
        family := selection.unsupported_scheduler(os.environ)
    ):
        echo(f"This machine has a {family} scheduler that the generic context cannot")
        echo(f"submit to: run ROMS inside a {family} job (an interactive session or a")
        echo("batch script), not on a login node.")
    if not yes:
        echo(PROMPT_HINT)

    resolved = resolve(ENV_CSTAR_SYSTEM, config, None)
    final = selection
    while True:
        decision = ask(f"System ({ENV_CSTAR_SYSTEM})", resolved, yes)
        if decision.kept:
            if selection is not None or not resolved.value:
                return decision, final, None
            if yes:
                return None, None, None
            echo(
                f"{ENV_CSTAR_SYSTEM}={resolved.value} names no registered system; "
                f"name one, or type {UNSET_ANSWER} to unset it."
            )
            continue
        if decision.value is None:
            try:
                final = detect_system()
            except (CstarError, OSError) as ex:
                final = None
                echo(str(ex))
            break
        if looks_like_path(decision.value):
            try:
                system_file = read_draft(decision.value)
            except SystemFileError as ex:
                echo(str(ex))
                continue
            final = SystemSelection(
                context_from_file(system_file), SystemSource.SETTING
            )
            echo(f"Using {describe_file(system_file)} for the rest of the setup.")
            return None, final, system_file
        try:
            klass = registered_context(decision.value)
        except CstarError as ex:
            echo(str(ex))
            continue
        decision = Decision(resolved, kept=False, value=klass.name)
        final = SystemSelection(klass, SystemSource.SETTING)
        break

    if final is not None and final != selection:
        echo(f"Using {final.describe()} for the rest of the setup.")
    return decision, final, None


def scheduler_section(
    context: "SystemContext",
    config: UserConfig,
    yes: bool,
    scheduler: "Scheduler | None" = None,
) -> list[Decision]:
    """Prompt for the SLURM account, queue and walltime.

    On a system whose `scheduler` registers GPU nodes, also offers the optional
    GPU account and queue.
    """
    header("Scheduler (SLURM)")
    echo("A workplan run on this system submits jobs with these settings; a")
    echo("workplan can override them per step.")
    slurm_hints()
    if not yes:
        echo(PROMPT_HINT)
    required = required_slurm_settings(context)
    prompts = (
        (ENV_CSTAR_SLURM_ACCOUNT, "SLURM account"),
        (ENV_CSTAR_SLURM_QUEUE, "SLURM queue or partition"),
        (ENV_CSTAR_SLURM_MAX_WALLTIME, "Default walltime limit per job (HH:MM:SS)"),
    )
    decisions: list[Decision] = []
    for name, label in prompts:
        suffix = " (required on this system)" if name in required else ""
        decisions.append(ask(f"{label}{suffix}", resolve(name, config, context), yes))
    gpu = scheduler.gpu if scheduler is not None else None
    if gpu is not None:
        echo(
            f"GPU jobs go to the `{gpu.queue}` queue on this system unless you set one."
        )
        gpu_prompts = (
            (
                ENV_CSTAR_SLURM_GPU_ACCOUNT,
                "SLURM account for GPU jobs (optional; defaults to the SLURM account)",
            ),
            (
                ENV_CSTAR_SLURM_GPU_QUEUE,
                f"Queue for GPU jobs (optional; defaults to {gpu.queue})",
            ),
        )
        for gpu_name, gpu_label in gpu_prompts:
            decisions.append(ask(gpu_label, resolve(gpu_name, config, context), yes))
    for decision in decisions:
        setting = decision.resolved.item.name
        unset = (
            decision.value is None if not decision.kept else not decision.resolved.value
        )
        if setting in required and unset:
            echo(
                f"{setting} is not set; a workplan run on this system stops early "
                "unless the workplan supplies it."
            )
    return decisions


def pbs_section() -> None:
    """Say that a PBS system's jobs are not submitted by C-Star."""
    header("Scheduler (PBS)")
    echo("Job submission through PBS is not supported yet. Run C-Star inside a PBS")
    echo("job (`qsub -I`, or your own batch script).")


def locations_section(
    context: "SystemContext | None",
    cluster: bool,
    config: UserConfig,
    yes: bool,
) -> list[Decision]:
    """Show where C-Star will put things and let the user change each location."""
    header("Locations")
    resolved = [
        resolve(name, config, context)
        for name in (ENV_CSTAR_DATA_HOME, ENV_CSTAR_PROJECT_HOME, ENV_CSTAR_CATALOG)
    ]
    for r in resolved:
        value = r.value or f"({SOURCE_NOT_SET})"
        echo(f"{r.item.name}: {value}  [{r.origin}]")
        echo(LOCATION_PURPOSES[r.item.name], indent=4)

    recommend = cluster and not resolved[0].explicit and not hpc_data_directory()
    if recommend:
        echo()
        echo("Recommended: no scratch file system was found, so run output would go")
        echo("under your home directory. Give CSTAR_DATA_HOME your scratch directory")
        echo(f"(for example /scratch/{current_user()}/cstar).")

    if not yes:
        echo()
        echo("Answer no to choose each one.")
    question = (
        "Use these locations?"
        if any(r.explicit for r in resolved)
        else "Use C-Star's default locations?"
    )
    if confirm(question, yes, default=not recommend):
        return [Decision(r, kept=True) for r in resolved]

    echo()
    echo(f"Press Enter to keep a value; type {UNSET_ANSWER} to leave a setting unset.")
    return [ask(r.item.name, r, yes) for r in resolved]


def catalog_setting(decisions: "Sequence[Decision]") -> tuple[int, str]:
    """Find the decision on `CSTAR_CATALOG` and the value it leaves in effect.

    Returns
    -------
    tuple[int, str]
        The decision's position, and its value: what the user kept or typed, else
        the default, which is your own catalog.
    """
    index = next(
        i for i, d in enumerate(decisions) if d.resolved.item.name == ENV_CSTAR_CATALOG
    )
    decision = decisions[index]
    value = decision.resolved.value if decision.kept else decision.value
    return index, value or default_catalog_root(decision.resolved.item)


def place_system_file(
    system_file: SystemFile, decisions: "Sequence[Decision]", yes: bool
) -> list[Decision]:
    """Settle where C-Star will find a system file, once the locations are chosen.

    The catalog the file goes to is the one `CSTAR_CATALOG` will hold after this
    walkthrough, from `decisions`, not the one in the environment now. A file
    already under the ``systems/`` directory of one of its roots needs nothing. A
    file at ``<root>/systems/<name>.yaml`` for another root can have that root
    added as a shared catalog, which follows the maintainer's fixes. Any other
    file is copied into your own catalog.

    Returns
    -------
    list[Decision]
        `decisions`, with `CSTAR_CATALOG` changed when a root was added.

    Raises
    ------
    typer.Exit
        Code 1 if the file cannot be copied; nothing has been written yet.
    """
    header("System file")
    index, value = catalog_setting(decisions)
    roots = catalog_roots(split_catalog(value))
    path = system_file.path.resolve()
    name = system_file.spec.name
    home = (
        path.parent.parent
        if path.parent.name == SYSTEMS_DIR_NAME and path.stem == name
        else None
    )

    if home is not None and home in roots.local:
        owner = "your catalog" if home == roots.user else "the shared catalog"
        echo(f"{display_path(path)} is already in {owner} {display_path(home)}.")
        return list(decisions)

    if home is not None and confirm(
        f"Add {home} to {ENV_CSTAR_CATALOG} as a shared catalog? "
        "(No copies the file into your own catalog)",
        yes,
    ):
        changed = list(decisions)
        changed[index] = Decision(
            decisions[index].resolved,
            kept=False,
            value=f"{value}{os.pathsep}{home}",
        )
        echo(f"{home} will be a shared catalog; C-Star reads {display_path(path)}.")
        return changed

    if roots.user is None:
        typer.echo(
            f"Error: the first {ENV_CSTAR_CATALOG} entry is not a local directory, so "
            f"there is no catalog of your own to copy {display_path(path)} into.",
            err=True,
        )
        raise typer.Exit(1)
    try:
        try:
            installed = install_system_file(system_file, roots.user, replace=False)
        except SystemFileConflict as ex:
            question = f"Replace {display_path(ex.target)}?"
            if yes or not confirm(question, yes=False, default=False):
                echo(f"Keeping {display_path(ex.target)}; C-Star uses that file.")
                return list(decisions)
            installed = install_system_file(system_file, roots.user, replace=True)
    except (CstarError, OSError) as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1) from ex
    reload_system_files()
    echo(
        f"Copied the file to {display_path(installed.path)}."
        if installed.written
        else f"{display_path(installed.path)} is already identical."
    )
    return list(decisions)


def system_decision(spec: SystemSpec, config: UserConfig) -> Decision:
    """Decide `CSTAR_SYSTEM` for a system a file describes.

    The setting is needed only where detection does not select the system. When
    detection does, a setting that names another system would win over it, so it
    is removed from the file.
    """
    resolved = resolve(ENV_CSTAR_SYSTEM, config, None)
    name = spec.name
    named = resolved.value
    if detection_would_select(spec):
        if named and named != name:
            echo(
                f"C-Star detects {name!r} here, but {ENV_CSTAR_SYSTEM}={named} would "
                "win over that: removing it from the file."
            )
            if resolved.source == SOURCE_ENV:
                echo(
                    f"{ENV_CSTAR_SYSTEM} is exported in your shell too; remove that "
                    "export line as well."
                )
            return Decision(resolved, kept=False, value=None)
        echo(
            f"C-Star detects {name!r} on this machine; {ENV_CSTAR_SYSTEM} is not needed."
        )
        return Decision(resolved, kept=True)
    if named == name:
        echo(f"C-Star does not detect {name!r} here; {ENV_CSTAR_SYSTEM}={name} stays.")
        return Decision(resolved, kept=True)
    echo(f"C-Star does not detect {name!r} here: setting {ENV_CSTAR_SYSTEM}={name}.")
    return Decision(resolved, kept=False, value=name)


def carried_settings(
    path: "Path",
) -> tuple[dict[str, str], dict[str, tuple[str | None, str]]]:
    """The lines the file already holds, spelled as the file spells them.

    Settings the walkthrough does not prompt for (a log level set with
    `cstar env set`, say) survive the rewrite this way; values are kept
    unexpanded so a ``~`` or ``$VAR`` keeps meaning what it meant. So do the
    lines `parse_user_config` skips (a removed setting, a name from a newer
    version, a setting written without ``=``, a value the setting does not
    accept), which the rewrite keeps apart under the loader's reason.

    Returns
    -------
    tuple[dict[str, str], dict[str, tuple[str | None, str]]]
        The settings to keep, and the lines this version will not apply, with
        their values (`None` for a name written without ``=``) and the reason
        each is skipped.

    Raises
    ------
    typer.Exit
        With code 1 when the file cannot be read: rewriting it would lose what
        it holds.
    """
    if not path.is_file():
        return {}, {}
    try:
        entries = dotenv_values(path, interpolate=False)
        skipped = parse_user_config(path)[1]
    except (OSError, UnicodeDecodeError, CstarUserConfigError) as ex:
        typer.echo(f"Error: cannot read {path}: {ex}", err=True)
        raise typer.Exit(1) from ex
    settings = {
        name: value
        for name, value in entries.items()
        if value is not None and name not in skipped
    }
    unrecognised = {n: (v, skipped[n]) for n, v in entries.items() if n in skipped}
    return settings, unrecognised


def write_section(
    config: UserConfig,
    carried: tuple[dict[str, str], dict[str, tuple[str | None, str]]],
    decisions: "Sequence[Decision]",
) -> list[str]:
    """Write the configuration file from what it held and the decisions, and
    report what it holds.

    Returns
    -------
    list[str]
        The settings written, as ``NAME=value``.
    """
    header("Configuration file")
    values, unrecognised = carried
    for decision in decisions:
        decision.apply(values)
    # a skipped line for a setting the walkthrough answered is replaced
    unrecognised = {n: v for n, v in unrecognised.items() if n not in values}
    content = render_user_config(values, unrecognised)
    try:
        write_bytes_atomic(config.path, content.encode())
    except OSError as ex:
        typer.echo(f"Error: cannot write {config.path}: {ex}", err=True)
        raise typer.Exit(1) from ex

    written = [f"{name}={value}" for name, value in values.items()]
    echo(f"Wrote {config.path}")
    if written:
        echo("Settings in the file: " + ", ".join(written))
    else:
        echo("No setting is set; every line is a commented example to fill in later.")
    echo(
        "Every cstar command reads this file; an exported CSTAR_* variable takes "
        f"precedence over it (see {DOCS_PRECEDENCE_URL})."
    )

    exported = [d for d in decisions if d.resolved.source == SOURCE_ENV]
    if moved := [d.resolved.item.name for d in exported if d.kept]:
        echo(
            f"{', '.join(moved)}: exported in your shell and now in the file, so "
            "you can remove the export line(s) from your shell profile."
        )
    for d in exported:
        if not d.kept:
            name = d.resolved.item.name
            echo(
                f"{name} is exported in your shell as {os.environ[name]!r}, which "
                "takes precedence over the file until you remove the export line."
            )
    return written


def shell_section(yes: bool) -> str:
    """Offer to install the shell function; the outcome for the summary."""
    command = f"cstar {NAME_RUN} {COMMAND_CD}"
    header(f"'{command}' shell support")
    echo(f"The '{command}' command requires a shell function to operate.")
    if not confirm("Install it for your shell?", yes):
        return "skipped"

    manual = (
        f"Run `cstar {NAME_ENV} {shell_init.COMMAND_SHELL_INIT} {ARG_INSTALL} "
        "<shell>` (bash or zsh) to install it by hand."
    )
    try:
        shell = shell_init.detect_shell()
    except typer.BadParameter as ex:
        echo(ex.message)
        echo(manual)
        return "not installed (shell not detected)"
    try:
        shell_init._install(shell)
    except typer.Exit:
        echo(manual)
        return "not installed (see above)"
    return f"installed for {shell}"


def kernel_section(yes: bool) -> str:
    """Offer to register a Jupyter kernel; the outcome for the summary."""
    header("Jupyter kernel")
    try:
        prefix = register_kernel._env_prefix()
    except register_kernel.RegisterKernelError as ex:
        echo(f"Skipped: {ex}")
        return "skipped (not in a conda environment)"

    echo("Registering a kernel lets Jupyter servers, including HPC portals, run")
    echo(f"notebooks and the Forge wizard in this environment ({prefix.name}).")
    if not confirm("Register it?", yes):
        return "skipped"
    try:
        register_kernel.register_kernel(log=echo)
    except (register_kernel.RegisterKernelError, OSError, ValueError) as ex:
        # OSError: the kernelspec or wrapper cannot be written; ValueError: a
        # corrupt kernel.json (JSONDecodeError) that the rewrite cannot parse.
        echo(f"Error: {ex}")
        echo(
            f"Run `cstar {NAME_ENV} {register_kernel.COMMAND_REGISTER_KERNEL}` "
            "once that is fixed."
        )
        return "not registered (see above)"
    return f"registered ({prefix.name})"


def skills_section(yes: bool) -> str:
    """Offer to install C-Star's agent skills; the outcome for the summary.

    Offered only when one of the agents' home folders exists, so the setup never
    creates ``~/.agents`` or ``~/.claude`` for someone who uses no coding agent.
    A re-run updates the links and adds skills new in this version.
    """
    header("Agent skills")
    command = f"cstar {NAME_SKILL} {COMMAND_INSTALL_SKILL}"
    homes = agent_homes()
    if not homes:
        echo(f"No coding agent found; `{command}` installs C-Star's skills for one.")
        return "not installed (no agent found)"
    try:
        skills = shipped_skills()
    except SkillError as ex:
        echo(f"Error: {ex}")
        return "not installed (see above)"
    if not skills:
        echo("This C-Star ships no skills.")
        return "none shipped"
    found = ", ".join(display_path(home) for home in homes)
    echo(f"C-Star ships skills that coding agents can use ({found} found):")
    for skill in skills:
        for line in textwrap.wrap(f"- {skill.name}: {skill.summary}", SKILL_WIDTH):
            echo(line if line.startswith("- ") else f"  {line}")
    if not confirm("Install C-Star's skills for your coding agents?", yes):
        return "skipped"

    folders = default_install_dirs()
    failed = install_skills(skills, folders, log=echo)
    for error in failed:
        echo(f"Not installed: {describe_failure(error)}")
    if failed:
        echo(f"`{command}` retries once that is fixed.")
    refused = {error.place.parent for error in failed}
    done = ", ".join(display_path(f) for f in folders if f not in refused)
    outcome = f"{len(skills)} installed ({done})" if done else ""
    if not refused:
        return outcome
    missing = ", ".join(display_path(f) for f in folders if f in refused)
    return "; ".join(filter(None, [outcome, f"not all in {missing} (see above)"]))


def data_access_section() -> None:
    """Point at the two datasets that need a one-time registration."""
    header("Data access")
    echo("Forge downloads most datasets itself. Two need a one-time step:")
    echo("- GLORYS (Copernicus Marine): register, then run `copernicusmarine login`.")
    echo("- TPXO tides: register, download the TPXO10.v2a files and place them under")
    echo(
        "  <source-data>/TPXO/TPXO10.v2a/; `cstar forge show-paths` prints <source-data>."
    )
    echo(f"Details: {DOCS_DATA_ACCESS_URL}")


def scheduler_name(scheduler: "Scheduler | None") -> str:
    """Name the scheduler family for the banner."""
    if isinstance(scheduler, SlurmScheduler):
        return "SLURM"
    if isinstance(scheduler, PBSScheduler):
        return "PBS"
    return "none"


def system_context(
    selection: SystemSelection | None,
) -> "tuple[SystemContext | None, Scheduler | None]":
    """The context a selection names and its scheduler; neither when there is none."""
    if selection is None:
        return None, None
    context = selection.context()
    return context, context.create_scheduler()


def init(
    system_file_path: t.Annotated[
        str | None,
        typer.Option(ARG_SYSTEM_FILE, help=ARG_SYSTEM_FILE_HELP, metavar="PATH"),
    ] = None,
    yes: t.Annotated[
        bool,
        typer.Option(
            ARG_YES,
            help="Accept every default and recommendation without prompting "
            "(for containers and scripts).",
        ),
    ] = False,
) -> None:
    """Walk through the setup that follows an install."""
    # Loaded by the root callback already; when it cannot read the file this is an
    # empty record, and carried_settings stops below.
    config = load_user_config_for_repair()

    system_file: SystemFile | None = None
    selection: SystemSelection | None
    if system_file_path is not None:
        # before any question, so a file that is refused stops the walkthrough
        try:
            system_file = read_draft(system_file_path)
        except SystemFileError as ex:
            typer.echo(f"Error: {ex}", err=True)
            raise typer.Exit(1) from ex
        selection, problem = (
            SystemSelection(context_from_file(system_file), SystemSource.SETTING),
            "",
        )
    else:
        try:
            selection = select_system()
        except (CstarError, OSError) as ex:
            selection, problem = None, str(ex)
        else:
            problem = ""
    context, scheduler = system_context(selection)

    typer.echo(f"C-Star {cstar.__version__}")
    if system_file is not None:
        system = describe_file(system_file)
    else:
        system = selection.describe() if selection else "unknown"
    typer.echo(f"System: {system}; scheduler: {scheduler_name(scheduler)}")
    state = (
        "exists; this run reviews and updates it"
        if config.path.is_file()
        else "not found; it will be created"
    )
    typer.echo(f"Configuration file: {config.path} ({state})")
    # read before any question, so a file that cannot be read stops the walkthrough
    carried = carried_settings(config.path)

    decisions: list[Decision] = []
    if system_file is None and (
        selection is None or selection.source is SystemSource.FALLBACK
    ):
        decision, selection, system_file = system_section(
            selection, problem, config, yes
        )
        if decision is not None:
            decisions.append(decision)
        context, scheduler = system_context(selection)
        if selection is None:
            echo(
                "C-Star does not recognise this system; scheduler settings are skipped."
            )
    if context is not None and isinstance(scheduler, SlurmScheduler):
        decisions += scheduler_section(context, config, yes, scheduler)
    elif isinstance(scheduler, PBSScheduler):
        pbs_section()
    cluster = scheduler is not None or bool(
        selection and selection.unsupported_scheduler(os.environ)
    )
    decisions += locations_section(context, cluster, config, yes)
    if system_file is not None:
        decisions = place_system_file(system_file, decisions, yes)
        decisions.append(system_decision(system_file.spec, config))
    written = write_section(config, carried, decisions)
    shell_outcome = shell_section(yes)
    kernel_outcome = kernel_section(yes)
    skills_outcome = skills_section(yes)
    data_access_section()

    header("Summary")
    echo(f"Configuration file: {config.path} ({len(written)} setting(s) set)")
    echo(f"Shell function: {shell_outcome}")
    echo(f"Jupyter kernel: {kernel_outcome}")
    echo(f"Agent skills: {skills_outcome}")
    echo("Open a new shell so that the shell function and the file take effect.")
