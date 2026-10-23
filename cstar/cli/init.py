"""`cstar init`: the guided setup that follows an install.

One command walks through everything the installation page lists: the
scheduler settings on a cluster, where C-Star puts its data, the
configuration file that records those choices, the shell function behind
`cstar run cd`, a Jupyter kernel for the environment, and the datasets that
need a registration. Every prompt defaults to the value already in effect, so
a re-run reviews the setup rather than redoing it.
"""

import os
import shutil
import subprocess
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
    EnvItem,
    UserConfig,
    get_env_item,
    hpc_data_directory,
    load_user_config,
    render_user_config,
)
from cstar.base.exceptions import CstarError
from cstar.cli.environment import NAME as NAME_ENV
from cstar.cli.environment import register_kernel, shell_init
from cstar.cli.environment.show import (
    SOURCE_CONFIG,
    SOURCE_DEFAULT,
    SOURCE_ENV,
    value_source,
)
from cstar.cli.run import NAME as NAME_RUN
from cstar.cli.workplan.path import COMMAND_CD
from cstar.entrypoint.utils import ARG_INSTALL, ARG_YES
from cstar.execution.file_system import DirectoryManager, write_bytes_atomic
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)
from cstar.system.environment import SlurmSettingsBase, get_envfield_alias
from cstar.system.manager import current_user, get_system_context
from cstar.system.scheduler import PBSScheduler, SlurmScheduler

if t.TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from cstar.system.manager import SystemContext
    from cstar.system.scheduler import Scheduler

COMMAND_INIT: t.Final[str] = "init"
"""Name of the root command that runs the guided setup (reserved against plugins)."""

HELP_SHORT: t.Final[str] = (
    "Set C-Star up after installing it: scheduler, locations, shell, kernel."
)
HELP_LONG: t.Final[str] = f"""\
{HELP_SHORT}

On a cluster, asks for the SLURM account, queue and default walltime. Shows where C-Star will put run output, the shared source-data cache and your catalog entries, asks whether to use them, and lets you change each if you answer no. Writes the answers to the configuration file, then offers to install the shell function that makes `cstar {NAME_RUN} {COMMAND_CD}` work and to register a Jupyter kernel for this environment.

Every prompt defaults to the value in effect now, so re-running the command reviews and updates the setup. {ARG_YES} accepts every default without asking, for containers and scripts.
"""

UNSET_ANSWER: t.Final[str] = "-"
"""The answer that leaves a setting unset when a value is offered as the default."""
HINT_TIMEOUT: t.Final[float] = 5.0
"""Seconds to wait for a SLURM lookup made only to show a hint."""
DOCS_PRECEDENCE_URL: t.Final[str] = (
    "https://c-star.readthedocs.io/en/latest/configuration.html"
    "#where-a-setting-can-be-set-and-which-wins"
)
"""The documentation section on where a setting can be set and which wins."""
DOCS_DATA_ACCESS_URL: t.Final[str] = (
    "https://c-star.readthedocs.io/en/latest/data_access.html"
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
    return {
        get_envfield_alias(klass, field_name)
        for field_name, field in klass.model_fields.items()
        if any(getattr(m, "min_length", 0) for m in field.metadata)
    }


def scheduler_section(
    context: "SystemContext", config: UserConfig, yes: bool
) -> list[Decision]:
    """Prompt for the SLURM account, queue and walltime."""
    header("Scheduler (SLURM)")
    echo("A workplan run on this system submits jobs with these settings; a")
    echo("workplan can override them per step.")
    slurm_hints()
    if not yes:
        echo(
            f"Press Enter to keep the value in brackets; type {UNSET_ANSWER} to "
            "leave a setting unset."
        )
    required = required_slurm_settings(context)
    prompts = (
        (ENV_CSTAR_SLURM_ACCOUNT, "SLURM account"),
        (ENV_CSTAR_SLURM_QUEUE, "SLURM queue or partition"),
        (ENV_CSTAR_SLURM_MAX_WALLTIME, "Maximum walltime per job (HH:MM:SS)"),
    )
    decisions: list[Decision] = []
    for name, label in prompts:
        suffix = " (required on this system)" if name in required else ""
        decisions.append(ask(f"{label}{suffix}", resolve(name, config, context), yes))
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


def locations_section(
    context: "SystemContext | None",
    scheduler: "Scheduler | None",
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

    recommend = (
        scheduler is not None and not resolved[0].explicit and not hpc_data_directory()
    )
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


def carried_settings(path: "Path") -> dict[str, str]:
    """The settings the file already holds, spelled as the file spells them.

    Settings the walkthrough does not prompt for (a log level set with
    `cstar env set`, say) survive the rewrite this way; values are kept
    unexpanded so a ``~`` or ``$VAR`` keeps meaning what it meant.
    """
    if not path.is_file():
        return {}
    return {
        name: value
        for name, value in dotenv_values(path, interpolate=False).items()
        if value is not None
    }


def write_section(config: UserConfig, decisions: "Sequence[Decision]") -> list[str]:
    """Write the configuration file from the decisions and report what it holds.

    Returns
    -------
    list[str]
        The settings written, as ``NAME=value``.
    """
    header("Configuration file")
    values = carried_settings(config.path)
    for decision in decisions:
        decision.apply(values)
    content = render_user_config(values)
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


def init(
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
    # Loaded (and validated) by the root callback already; this is the cached record.
    config = load_user_config()

    context: SystemContext | None
    try:
        context = get_system_context()
    except (CstarError, OSError) as ex:
        context = None
        system, scheduler = "unknown", None
        unknown = str(ex)
    else:
        system, scheduler = context.name, context.create_scheduler()
        unknown = ""

    typer.echo(f"C-Star {cstar.__version__}")
    typer.echo(f"System: {system}; scheduler: {scheduler_name(scheduler)}")
    if unknown:
        typer.echo(f"  {unknown}")
        typer.echo(
            "  C-Star does not recognise this system; scheduler settings are skipped."
        )
    state = (
        "exists; this run reviews and updates it"
        if config.path.is_file()
        else "not found; it will be created"
    )
    typer.echo(f"Configuration file: {config.path} ({state})")

    decisions: list[Decision] = []
    if context is not None and isinstance(scheduler, SlurmScheduler):
        decisions += scheduler_section(context, config, yes)
    decisions += locations_section(context, scheduler, config, yes)
    written = write_section(config, decisions)
    shell_outcome = shell_section(yes)
    kernel_outcome = kernel_section(yes)
    data_access_section()

    header("Summary")
    echo(f"Configuration file: {config.path} ({len(written)} setting(s) set)")
    echo(f"Shell function: {shell_outcome}")
    echo(f"Jupyter kernel: {kernel_outcome}")
    echo("Open a new shell so that the shell function and the file take effect.")
