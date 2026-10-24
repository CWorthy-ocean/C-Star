import os
import sys
import textwrap
import typing as t
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from importlib import import_module
from pathlib import Path

from dotenv import dotenv_values

from cstar.base.exceptions import CstarUserConfigError

if t.TYPE_CHECKING:
    from types import ModuleType

GROUP_FF: t.Final[str] = "Feature Flags"
"""Group name for feature flag environment variables in documentation."""
GROUP_FS: t.Final[str] = "File System Configuration"
"""Group name for file system related environment variables in documentation."""
GROUP_SIM: t.Final[str] = "Simulation Configuration"
"""Group name for simulation-specific environment variables in documentation."""
GROUP_UNK: t.Final[str] = "Uncategorized Configuration"
"""Group name for uncategorized environment variables in documentation."""
GROUP_INTERNAL: t.Final[str] = "Internal"
"""Group name for variables C-Star sets for its own processes; hidden from users."""

FLAG_ON: t.Final[str] = "1"
"""Value indicating a toggle is enabled."""
FLAG_OFF: t.Final[str] = "0"
"""Value indicating a toggle is disabled."""

ENVVAR_PREFIX: t.Final[str] = "CSTAR_"
"""The common env var prefix that identifies a C-Star configuration setting."""
CONSTANT_PREFIX: t.Final[str] = "ENV_"
"""The common prefix used to name an annotated environment variable constant."""
NOT_SET: t.Final[str] = "<not-set>"
"""Default description and default for variables missing annotations."""
USER_CONFIG_FILENAME: t.Final[str] = "config.env"
"""The name of the user's configuration file under the config home."""


@dataclass(slots=True)
class EnvVar:
    """Annotation for specifying metadata about an environment variable."""

    description: str
    """Plain-text description of the setting."""
    group: str
    """A group name used to identify the variable use."""
    default: str = ""
    """The default value for the setting."""
    default_factory: Callable[["EnvVar"], str | None] | None = None
    """A function used at run-time to generate the default value."""
    indirect_var: str = ""
    """An environment variable name to be used when the primary variable is not set."""
    persistable: bool = True
    """Whether the setting may be stored in the user's configuration file."""


@dataclass(slots=True)
class EnvItem(EnvVar):
    """Runtime wrapper for an `EnvVar` that determines the actual value."""

    name: str = ""
    """The standard environment variable name used for the setting."""

    @property
    def value(self) -> str:
        if (env_value := os.getenv(self.name)) is not None:
            return env_value

        if self.default_factory and (factory_default := self.default_factory(self)):
            return factory_default

        if self.indirect_var and (indirect_value := os.getenv(self.indirect_var, "")):
            return indirect_value

        return self.default

    @classmethod
    def from_env_var(cls, env_var: EnvVar, name: str) -> "EnvItem":
        return EnvItem(
            description=env_var.description,
            group=env_var.group,
            default=env_var.default,
            default_factory=env_var.default_factory,
            indirect_var=env_var.indirect_var,
            persistable=env_var.persistable,
            name=name,
        )


def capture_environment() -> dict[str, str]:
    """Capture the C-Star-owned environment variables at the current time.

    Returns
    -------
    dict[str, str]
    """
    return {k: v for k, v in os.environ.items() if k.startswith(ENVVAR_PREFIX)}


def indirect_default_factory(env_var: EnvVar) -> str:
    """Retrieve the current value of the indirect variable.

    Return empty-string when the indirect variable is not populated.
    Returns
    -------
    str
    """
    var_name = env_var.indirect_var
    return os.environ.get(var_name, "")


def get_env_item(var_name: str) -> EnvItem:
    """Retrieve the metadata for an environment variable constant.

    Parameters:
    -----------
    var_name : str
        The string value of the environment variable (e.g. "CSTAR_CACHE_HOME")

    Returns:
    --------
    env_item: EnvItem
        The metadata associated with the environment variable
    """
    env_vars = discover_env_vars()
    if variable := env_vars.get(var_name, None):
        return variable

    msg = f"No environment variable metadata found for: {var_name}"
    raise ValueError(msg)


def find_scratch_dir(env: Mapping[str, str]) -> str | None:
    """Search *env* for the first ``CSTAR_SCRATCH_DIRS``-listed variable that is set.

    The search-order logic behind :func:`hpc_data_directory`, separated so it can
    run against an explicit environment mapping (a test double) as well as the
    real process environment.

    Parameters
    ----------
    env : Mapping[str, str]
        The environment to search, e.g. ``os.environ`` or a test double.

    Returns
    -------
    str | None
        The first configured scratch path found (as a POSIX string), or
        ``None`` if none of the listed variables is set in *env*.
    """
    scratch_variables = get_env_item(ENV_CSTAR_SCRATCH_DIRS).value.split(",")

    for env_var in scratch_variables:
        if scratch_path := env.get(env_var, ""):
            return Path(scratch_path).as_posix()

    return None


def _system_scratch_root() -> str | None:
    """The current system's own scratch convention, from its ``SystemContext``.

    Imported lazily: ``cstar.system.manager`` imports this module (through
    ``cstar.base.log``), so a module-level import would be circular. A system
    that cannot be identified (``CstarError`` for an unregistered name,
    ``OSError`` when no name can be determined at all) has no context and
    contributes nothing.
    """
    from cstar.base.exceptions import CstarError
    from cstar.system.manager import get_system_context

    try:
        context = get_system_context()
    except (CstarError, OSError):
        return None
    root = context.scratch_root()
    return root.as_posix() if root is not None else None


def hpc_data_directory() -> str | None:
    """Locate the scratch file system, the default ``CSTAR_DATA_HOME`` on HPC systems.

    Tries the ``CSTAR_SCRATCH_DIRS`` variables first (``SCRATCH``, ``SCRATCH_DIR``,
    ``LOCAL_SCRATCH`` by default), then the system's own convention through
    :meth:`cstar.system.manager.SystemContext.scratch_root` (Bouchet's
    ``scratch_pi_*`` directories, for example).

    Returns
    -------
    str | None
        The scratch path, or ``None`` when neither source names one.
    """
    return find_scratch_dir(os.environ) or _system_scratch_root()


def nprocs_factory() -> str:
    """Return the number of processors on the current machine, divided by 3."""
    return str((os.cpu_count() or 3) // 3)


def generate_run_id() -> str:
    """Generate a unique run identifier based on the current time."""
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


ENV_CSTAR_LOG_LEVEL: t.Annotated[
    t.Literal["CSTAR_LOG_LEVEL"],
    EnvVar(
        "Specify the logging level for terminal messages. Options: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
        GROUP_SIM,
        default="INFO",
    ),
] = "CSTAR_LOG_LEVEL"
"""Specify the logging level for terminal messages. Options: DEBUG, INFO, WARNING, ERROR, CRITICAL."""

ENV_CSTAR_DISABLE_BUILD_VERIFICATION: t.Annotated[
    t.Literal["CSTAR_DISABLE_BUILD_VERIFICATION"],
    EnvVar(
        "Set to `1` to skip the pre- and post-build toolchain consistency checks "
        "performed when compiling ROMS. Use at your own risk: the resulting binary "
        "may be linked against a mixed set of libraries.",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_DISABLE_BUILD_VERIFICATION"
"""Set to `1` to skip the pre- and post-build toolchain consistency checks performed when compiling ROMS."""

ENV_CSTAR_DEBUG_BUILD_MODE: t.Annotated[
    t.Literal["CSTAR_DEBUG_BUILD_MODE"],
    EnvVar(
        "Set to `1` to build ROMS and its dependencies in debug mode "
        "(`BUILD_MODE=debug`).",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_DEBUG_BUILD_MODE"
"""Set to `1` to build ROMS and its dependencies in debug mode."""

ENV_CSTAR_SLURM_DISABLE_MT: t.Annotated[
    t.Literal["CSTAR_SLURM_DISABLE_MT"],
    EnvVar(
        "Set to `1` to pass SLURM the hint `--hint=nomultithread`, which "
        "disables hardware multi-threading for submitted jobs.",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_SLURM_DISABLE_MT"
"""Set to `1` to pass SLURM a hint that disables multi-threading."""

ENV_CSTAR_DISABLE_MIGRATION: t.Annotated[
    t.Literal["CSTAR_DISABLE_MIGRATION"],
    EnvVar(
        "Set to `1` to disable automatic blueprint schema migration. Commands "
        "fail early instead of migrating when a blueprint is not at the current "
        "schema version, guaranteeing the blueprint is never modified.",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_DISABLE_MIGRATION"
"""Set to `1` to disable automatic blueprint schema migration; commands fail early on out-of-date blueprints."""

ENV_CSTAR_NPROCS_POST: t.Annotated[
    t.Literal["CSTAR_NPROCS_POST"],
    EnvVar(
        "Specify the number of processes to be used for post-processing simulation output files. Dynamic default ``os.cpu_count() // 3``",
        GROUP_SIM,
        default_factory=lambda _: nprocs_factory(),  # type: ignore[reportOptionalOperand]
    ),
] = "CSTAR_NPROCS_POST"
"""Specify the number of processes to be used for post-processing simulation output files."""

ENV_CSTAR_SCRATCH_DIRS: t.Annotated[
    t.Literal["CSTAR_SCRATCH_DIRS"],
    EnvVar(
        "A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order.",
        GROUP_FS,
        "SCRATCH,SCRATCH_DIR,LOCAL_SCRATCH",
    ),
] = "CSTAR_SCRATCH_DIRS"
"""A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order."""

ENV_CSTAR_CACHE_HOME: t.Annotated[
    t.Literal["CSTAR_CACHE_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star file cache.",
        GROUP_FS,
        "~/.cache/cstar",
        indirect_var="XDG_CACHE_HOME",
        default_factory=indirect_default_factory,
    ),
] = "CSTAR_CACHE_HOME"
"""Environment variable used to override the home directory for C-Star file cache."""

ENV_CSTAR_CONFIG_HOME: t.Annotated[
    t.Literal["CSTAR_CONFIG_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star config storage.",
        GROUP_FS,
        "~/.config/cstar",
        default_factory=indirect_default_factory,
        indirect_var="XDG_CONFIG_HOME",
        persistable=False,
    ),
] = "CSTAR_CONFIG_HOME"
"""Environment variable used to override the home directory for C-Star config storage."""

ENV_CSTAR_DATA_HOME: t.Annotated[
    t.Literal["CSTAR_DATA_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star dataset storage "
        "(workplan runs and blueprint runs). On supported HPC systems it defaults to a "
        "``cstar`` directory on the scratch file system (the first CSTAR_SCRATCH_DIRS "
        "variable that is set, else the system's own convention).",
        GROUP_FS,
        "~/cstar",
        indirect_var="XDG_DATA_HOME",
        default_factory=lambda x: hpc_data_directory() or indirect_default_factory(x),
    ),
] = "CSTAR_DATA_HOME"
"""Environment variable used to override the home directory for C-Star dataset storage."""

ENV_CSTAR_PROJECT_HOME: t.Annotated[
    t.Literal["CSTAR_PROJECT_HOME"],
    EnvVar(
        "Durable, group-shareable project directory (the directory itself; C-Star "
        "writes under a ``cstar`` subdirectory). Holds the shared source-data cache "
        "of downloaded datasets. Defaults to ``$PROJECT``; when neither is set the "
        "cache falls back to the scratch file system, then to the home directory.",
        GROUP_FS,
        indirect_var="PROJECT",
        default_factory=indirect_default_factory,
    ),
] = "CSTAR_PROJECT_HOME"
"""Durable, group-shareable project directory; defaults to ``$PROJECT``."""


def default_catalog_root(env_var: EnvVar) -> str:
    """Default catalog location: ``~/cstar/catalog``.

    Deliberately home-anchored rather than derived from ``CSTAR_DATA_HOME``:
    catalog entries are durable, user-registered content that must survive HPC
    scratch purges, and ``CSTAR_DATA_HOME`` resolves onto scratch on HPC systems
    (see :func:`hpc_data_directory`).
    """
    return (Path.home() / "cstar" / "catalog").as_posix()


ENV_CSTAR_CATALOG: t.Annotated[
    t.Literal["CSTAR_CATALOG"],
    EnvVar(
        "Catalog locations for forge blueprint authoring, os.pathsep-separated "
        "(local paths, GitHub or http URLs), stacked over the bundled catalog; the "
        "first entry is the writable user layer.",
        GROUP_FS,
        default_factory=default_catalog_root,
    ),
] = "CSTAR_CATALOG"
"""Catalog locations for forge blueprint authoring (first entry = writable user layer)."""

ENV_CSTAR_STATE_HOME: t.Annotated[
    t.Literal["CSTAR_STATE_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star state storage.",
        GROUP_FS,
        "~/.local/state/cstar",
        indirect_var="XDG_STATE_HOME",
        default_factory=indirect_default_factory,
    ),
] = "CSTAR_STATE_HOME"
"""Environment variable used to override the home directory for C-Star state storage."""

ENV_CSTAR_RUNID: t.Annotated[
    t.Literal["CSTAR_RUNID"],
    EnvVar(
        description="Unique run identifier used by the orchestrator.",
        group=GROUP_INTERNAL,
        default_factory=lambda _: generate_run_id(),
        persistable=False,
    ),
] = "CSTAR_RUNID"
"""Environment variable containing a unique run identifier used by the orchestrator."""


@dataclass(frozen=True, slots=True)
class RemovedSetting:
    """What to tell someone who still exports a setting C-Star no longer reads."""

    guidance: str
    """What to do instead; read after "<NAME> has no effect; "."""
    refused: bool = False
    """Whether the commands it used to change refuse to run while it is exported
    (`cstar.cli.common.exit_on_refused_settings`), because ignoring it there would
    do more than was asked (a dry run becoming a real one)."""


REMOVED_SETTINGS: t.Final[dict[str, RemovedSetting]] = {
    "CSTAR_CLI_DRY_RUN": RemovedSetting(
        "pass --dry-run to `cstar blueprint migrate` or `cstar admin clean`, or "
        "check a workplan without running it with `cstar workplan check`.",
        refused=True,
    ),
    "CSTAR_CLI_VERBOSE": RemovedSetting(
        "pass --verbose to `cstar blueprint run` or `cstar blueprint migrate` instead."
    ),
    "CSTAR_CLOBBER_WORKING_DIR": RemovedSetting(
        "pass --clobber to `cstar blueprint run` or `cstar blueprint migrate` "
        "instead (`--clobber <step|all>` for `cstar workplan submit`)."
    ),
    "CSTAR_FF_CLI_TEMPLATE_CREATE": RemovedSetting(
        "the `cstar template create` command was removed."
    ),
    "CSTAR_FF_CLI_WORKPLAN_COMPOSE": RemovedSetting(
        "the `cstar workplan compose` command was removed."
    ),
    "CSTAR_FF_CLI_WORKPLAN_GEN": RemovedSetting(
        "the `cstar workplan generate` command was removed."
    ),
    "CSTAR_FF_CLI_WORKPLAN_PLAN": RemovedSetting(
        "the `cstar workplan plan` command was removed."
    ),
    "CSTAR_FF_DEBUG_BUILD_MODE": RemovedSetting(
        "set CSTAR_DEBUG_BUILD_MODE=1 instead; developer mode no longer enables it."
    ),
    "CSTAR_FF_SLURM_DISABLE_MT": RemovedSetting(
        "set CSTAR_SLURM_DISABLE_MT=1 instead; developer mode no longer enables it."
    ),
    "CSTAR_FRESH_CODEBASES": RemovedSetting(
        "pass --clobber to `cstar blueprint run` (`--clobber <step|all>` for "
        "`cstar workplan submit`) to start from an empty working directory instead."
    ),
    "CSTAR_IN_ACTIVE_ALLOCATION": RemovedSetting(
        "C-Star detects an active SLURM allocation from SLURM_JOBID."
    ),
    "CSTAR_ORCH_DELAYS": RemovedSetting(
        "the orchestrator's polling intervals are fixed."
    ),
    "CSTAR_ORCH_LOCAL_DELAY": RemovedSetting(
        "the local launcher's polling interval is fixed."
    ),
    "CSTAR_ORCH_MAX_CONC": RemovedSetting(
        "the limit on concurrent disk operations is fixed."
    ),
    "CSTAR_ORCH_TRX_FREQ": RemovedSetting(
        "set the feature flag CSTAR_FF_ORCH_TRX_TIMESPLIT_FREQ instead (it is read "
        "only while CSTAR_FF_ORCH_TRX_TIMESPLIT is on)."
    ),
    "CSTAR_SLURM_POST_SUBMIT_DELAY": RemovedSetting(
        "the delay after a SLURM submission is fixed."
    ),
}
"""Settings that were removed, mapped to guidance for anyone still exporting one.

An exported removed setting has no effect; the CLI warns about it, and the
commands a `refused` one used to change refuse to run while it is exported.
"""


@lru_cache
def discover_env_vars() -> dict[str, EnvItem]:
    """Return a mapping from env-var constant to the associated metadata."""
    container: dict[str, EnvItem] = {}
    modules: list[ModuleType] = [
        sys.modules[__name__],
        import_module("cstar.orchestration.utils"),
        import_module("cstar.base.feature"),
    ]

    for module in modules:
        hints = t.get_type_hints(module, include_extras=True)
        matches = {k: v for k, v in hints.items() if k.startswith(CONSTANT_PREFIX)}
        for var_name, hint in matches.items():
            actual = getattr(module, var_name)

            if not (meta := getattr(hint, "__metadata__", None)):
                continue

            if annotation := next((x for x in meta if isinstance(x, EnvVar)), None):
                container[actual] = EnvItem.from_env_var(annotation, actual)

    return container


def env_var_groups(include_internal: bool = False) -> dict[str, list[EnvItem]]:
    """Return a mapping from group-name to the associated list of env variables.

    Parameters
    ----------
    include_internal : bool
        Include the internal group, which C-Star sets for its own processes and
        which users never set.
    """
    groups: dict[str, list[EnvItem]] = defaultdict(list)

    for item in discover_env_vars().values():
        if include_internal or item.group != GROUP_INTERNAL:
            groups[item.group].append(item)

    return groups


def user_config_path() -> Path:
    """The user's configuration file, ``config.env`` under the config home.

    The config home is resolved from the process environment alone
    (``CSTAR_CONFIG_HOME``, then ``XDG_CONFIG_HOME``): the file cannot relocate
    itself. Imported lazily because ``cstar.execution.file_system`` imports this
    module.
    """
    from cstar.execution.file_system import DirectoryManager

    return DirectoryManager.config_home() / USER_CONFIG_FILENAME


def user_config_problems(names: Iterable[str]) -> list[str]:
    """Explain why each of `names` may not appear in the configuration file.

    A name passes when it is a registered, persistable setting that is not a
    feature flag; the list is empty when every name passes.

    Parameters
    ----------
    names : Iterable[str]
        The keys to check, in the order to report them.

    Returns
    -------
    list[str]
        One message per offending name.
    """
    registry = discover_env_vars()
    problems: list[str] = []
    for name in names:
        if name in REMOVED_SETTINGS:
            problems.append(f"{name} has no effect; {REMOVED_SETTINGS[name].guidance}")
        elif (item := registry.get(name)) is None:
            problems.append(f"{name} is not a C-Star setting")
        elif item.group == GROUP_FF:
            problems.append(f"{name} is a feature flag; export it in the shell instead")
        elif not item.persistable:
            problems.append(f"{name} cannot be set from the configuration file")
    return problems


def read_user_config(path: Path) -> dict[str, str]:
    """Parse the configuration file at `path`, validating its keys.

    Values have ``~`` and ``$VAR`` references expanded against the process
    environment. A key written without a value (``CSTAR_X=``) yields an empty
    string, as an exported empty variable would; a bare name with no ``=`` is
    rejected rather than guessed at.

    Parameters
    ----------
    path : Path
        The file to read; a missing file reads as empty.

    Returns
    -------
    dict[str, str]
        The settings the file names, in file order.

    Raises
    ------
    CstarUserConfigError
        Naming the file and every key it may not set, or why the file cannot
        be read.
    """
    if not path.is_file():
        return {}

    try:
        entries = dotenv_values(path)
    except (OSError, UnicodeDecodeError) as ex:
        msg = f"The configuration file {path} cannot be read: {ex}"
        raise CstarUserConfigError(msg) from ex

    rejected = {name for name in entries if user_config_problems([name])}
    problems = user_config_problems(entries.keys()) + [
        f"{name} has no value; write {name}=<value> or comment the line out"
        for name, value in entries.items()
        if value is None and name not in rejected
    ]
    if problems:
        listing = "\n".join(f"  - {problem}" for problem in problems)
        msg = f"The configuration file {path} names settings it may not set:\n{listing}"
        raise CstarUserConfigError(msg)

    return {
        name: os.path.expandvars(os.path.expanduser(value or ""))
        for name, value in entries.items()
    }


@dataclass(frozen=True, slots=True)
class UserConfig:
    """What `load_user_config` found and exported."""

    path: Path
    """The configuration file that was looked for."""
    applied: Mapping[str, str]
    """The settings the file supplied to the environment, with the values it gave
    them; a setting the shell had already exported is not among them."""


@lru_cache
def load_user_config() -> UserConfig:
    """Export the user's configuration file into the environment, once per process.

    Each entry is applied with ``os.environ.setdefault``, so an exported variable
    always wins over the file, and the file over the defaults every reader of the
    environment falls back to (`EnvItem.value`, ``os.getenv``, pydantic settings,
    typer ``envvar`` bindings). Loading into the environment also means step
    subprocesses inherit the settings and a run record captures them.

    Called from the CLI root callback and from `CStarSystemManager` so library
    use loads it too; never at import time.

    Returns
    -------
    UserConfig
        The file's path and the settings it supplied.

    Raises
    ------
    CstarUserConfigError
        Naming the file and every key it may not set.
    """
    path = user_config_path()
    applied = {
        name: value
        for name, value in read_user_config(path).items()
        if name not in os.environ
    }
    for name, value in applied.items():
        os.environ.setdefault(name, value)
    return UserConfig(path=path, applied=applied)


USER_CONFIG_HEADER: t.Final[str] = """\
# C-Star configuration. Settings here apply to every cstar command; an exported
# environment variable of the same name takes precedence. Lines starting with
# `#` are comments; uncomment a setting and give it a value to use it.
"""
"""The comment block that opens a rendered configuration file."""


def render_setting_block(item: EnvItem, value: str | None = None) -> str:
    """Render one setting as a commented description followed by its line.

    Parameters
    ----------
    item : EnvItem
        The setting to render.
    value : str | None
        The value to set; `None` renders the setting commented out (``# NAME=``).

    Returns
    -------
    str
        The block, ending in a newline.
    """
    description = textwrap.fill(
        item.description, width=78, initial_indent="# ", subsequent_indent="# "
    )
    default = f"# Default: {item.default or NOT_SET}"
    line = (
        f"{item.name}={format_user_config_value(value)}"
        if value is not None
        else f"# {item.name}="
    )
    return f"{description}\n{default}\n{line}\n"


def format_user_config_value(value: str) -> str:
    """Quote a value when the dotenv format would otherwise misread it.

    A bare value runs to the end of the line, except that `` #`` begins a
    comment and a leading quote starts a quoted value; quoting protects
    whitespace, ``#`` and a quote character. ``~`` and ``$VAR`` are still
    expanded when the file is read, quoted or not.
    """
    if (
        not value
        or any(c.isspace() for c in value)
        or "#" in value
        or value[0] in "\"'"
    ):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def render_user_config(values: Mapping[str, str]) -> str:
    """Render a whole configuration file from the registry.

    Every persistable, user-facing setting gets a block; those in `values` are
    set, the rest are commented out so the file documents what can be set.

    Parameters
    ----------
    values : Mapping[str, str]
        The settings to set, by name; each must pass `user_config_problems`.

    Returns
    -------
    str
        The file content.

    Raises
    ------
    CstarUserConfigError
        Naming every key that may not be set.
    """
    if problems := user_config_problems(values.keys()):
        listing = "\n".join(f"  - {problem}" for problem in problems)
        raise CstarUserConfigError(f"Settings that cannot be written:\n{listing}")

    sections = [USER_CONFIG_HEADER]
    for group, items in env_var_groups(include_internal=False).items():
        blocks = [
            render_setting_block(item, values.get(item.name))
            for item in sorted(items, key=lambda x: x.name)
            if item.persistable and item.group != GROUP_FF
        ]
        if blocks:
            sections.append(
                f"\n# {'-' * 76}\n# {group}\n# {'-' * 76}\n\n" + "\n".join(blocks)
            )
    return "".join(sections)
