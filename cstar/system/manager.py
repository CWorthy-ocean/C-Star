import functools
import os
import platform as platform
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Final, Protocol, override

from cstar.base.env import (
    ENV_CSTAR_ALLOW_LOGIN_NODE_RUN,
    ENV_CSTAR_CATALOG,
    ENV_CSTAR_SYSTEM,
    CatalogRoots,
    catalog_roots,
    get_env_item,
    load_user_config,
    load_user_config_for_repair,
)
from cstar.base.exceptions import CstarError
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.system.environment import (
    CStarEnvironment,
    EnvSettingsBase,
    LmodEnvSettings,
)
from cstar.system.layers import SystemLayers, read_system_layers
from cstar.system.scheduler import (
    Scheduler,
    in_scheduler_job,
    visible_scheduler,
)
from cstar.system.spec import (
    BUNDLED_LAYER,
    SystemFile,
    SystemSpec,
    read_bundled_system_files,
)
from cstar.system.spec import (
    current_user as current_user,  # re-exported: its home before cstar.system.spec
)
from cstar.system.toolchain import Toolchain, ToolchainName

if TYPE_CHECKING:
    from collections.abc import Mapping

log = get_logger(__name__)

DOCS_MACHINES_URL: Final[str] = (
    "https://c-star.readthedocs.io/en/latest/deployments/machines.html"
)
"""The documentation page on the systems C-Star knows and what to do on any other."""
DOCS_DESCRIBE_CLUSTER_URL: Final[str] = (
    "https://c-star.readthedocs.io/en/latest/deployments/describe_cluster.html"
)
"""The documentation page on describing a machine to C-Star with a system file."""


class HostNameEvaluator:
    """The system detector: names the machine from what it reports (Lmod, a
    registered context's own test, the platform), ignoring `CSTAR_SYSTEM`.

    Only `detect_system` reads it. Everything else that names the system C-Star
    runs on asks `selected_system_name` (or `select_system`, for how it was
    chosen), so the setting and the generic fallback apply there too.
    """

    lmod_settings: Final[LmodEnvSettings]
    """LMOD-specific environment configuration."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.lmod_settings = LmodEnvSettings()

    @property
    def platform_name(self) -> str:
        """Return the system name for the current platform."""
        return platform.system()

    @property
    def machine_name(self) -> str:
        """Return the machine name for the current platform."""
        return platform.machine()

    @property
    def platform_hostname(self) -> str:
        """Aggregate of machine and platform sysname."""
        value = ""
        if self.platform_name and self.machine_name:
            value = f"{self.platform_name}_{self.machine_name}"
        return value.casefold()

    @property
    def node_name(self) -> str:
        """Return the network name of this machine; empty when it has none."""
        return platform.node()

    @property
    def _diagnostic(self) -> str:
        """Return a string useful for diagnosing failures to identify the name."""
        attributes = [
            self.lmod_settings.SYSHOST,
            self.lmod_settings.SYSTEM_NAME,
            self.platform_name,
            self.machine_name,
        ]

        return ", ".join(f"{item=}" for item in attributes)

    @property
    def name(self) -> str:
        """Determine the detected system name (not necessarily the one C-Star uses;
        see `selected_system_name`).

        Prioritizes the LMOD-based system name. When LMOD-specific environment
        variables are not present, the name is determiend by the OS.

        Raises
        ------
        EnvironmentError
            If the name cannot be determined.
        """
        if lmod_hostname := self.lmod_hostname:
            log.trace("Using lmod hostname as system name")
            return lmod_hostname

        for ctx_klass in get_registered_sys_contexts():
            if ctx_klass.is_match():
                name = ctx_klass.name
                log.trace(f"Using {name!r} as system name due to context match")
                return name

        if self.platform_hostname:
            log.trace("Using platform hostname as system name")
            return self.platform_hostname

        raise OSError(
            f"C-Star cannot determine your system name. Diagnostics: {self._diagnostic}"
        )

    @property
    def lmod_hostname(self) -> str:
        """Return a hostname using the available configuration with priority order:
        1. LMOD_SYSHOST
        2. LMOD_SYSTEM_NAME

        If neither value is set, returns empty-string.
        """
        return (self.lmod_settings.SYSHOST or self.lmod_settings.SYSTEM_NAME).casefold()


class SystemContext(Protocol):
    """The contextual dependencies for the system/platform."""

    name: ClassVar[str]
    """The unique name identifying the context."""
    compiler: ClassVar[str]
    """The compiler used when building software for the system."""
    mpi_prefix: ClassVar[str]
    """The prefix used when executing mpiexec for the system."""
    system_file: ClassVar[SystemFile | None] = None
    """The system file the context was read from; `None` for one defined in code."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        """Instantiate a scheduler configured for the system."""

    @classmethod
    def toolchain(cls, name: ToolchainName) -> Toolchain:
        """Return the compiler/MPI/module profile the system offers under `name`.

        The ``cpu`` profile is derived from `compiler` and `mpi_prefix`, so those
        class variables stay the one source for it. A system with GPU nodes
        overrides this to supply the ``gpu`` case.

        Raises
        ------
        CstarError
            If `name` is ``gpu`` and the system registers no GPU toolchain.
        """
        if name == "gpu":
            raise CstarError(f"{cls.name} has no GPU toolchain registered with C-Star")
        return Toolchain(compiler=cls.compiler, mpi_prefix=cls.mpi_prefix)

    @classmethod
    def settings_klass(cls) -> type[EnvSettingsBase] | None:
        """Return the type used to load settings required by the target system.

        NOTE: The type is returned to avoid validation failures at import time due
        to the instantation of the global `cstar_sysmgr`.
        """
        return EnvSettingsBase

    @classmethod
    def is_match(cls) -> bool:
        """Return `True` if the context identifies the current system as a match."""
        return False

    @classmethod
    def scratch_root(cls) -> Path | None:
        """Return the system's scratch file system when no environment variable names it.

        Consulted by :func:`cstar.base.env.hpc_data_directory` after the
        ``CSTAR_SCRATCH_DIRS`` search comes up empty, so ``CSTAR_DATA_HOME`` still
        resolves onto scratch on systems that export no ``SCRATCH``-style variable.
        Implementations must never raise; ``None`` means the system has no
        convention C-Star can discover.
        """
        return None


class SpecSystemContext(SystemContext):
    """A system context read from a system file (see `cstar.system.spec`).

    `context_from_file` makes one subclass per file; each class method answers
    from the file's spec.
    """

    system_file: ClassVar[SystemFile]
    docs: ClassVar[str]
    """Where the machine's documentation is."""
    _settings: ClassVar[type[EnvSettingsBase]]
    """The settings class built from the spec, once."""

    @override
    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        """Instantiate the scheduler the system file describes, if any."""
        spec = cls.system_file.spec.scheduler
        return spec.build(cls.docs) if spec is not None else None

    @override
    @classmethod
    def toolchain(cls, name: ToolchainName) -> Toolchain:
        """Return the toolchain the system file lists under `name`.

        Raises
        ------
        CstarError
            If the system file lists no toolchain under `name`.
        """
        if (spec := cls.system_file.spec.toolchains.get(name)) is None:
            raise CstarError(
                f"{cls.name} has no {name.upper()} toolchain registered with C-Star"
            )
        return spec.build(name)

    @override
    @classmethod
    def settings_klass(cls) -> type[EnvSettingsBase] | None:
        """Return the type used to load settings required by the target system."""
        return cls._settings

    @override
    @classmethod
    def is_match(cls) -> bool:
        """Return `True` if any of the system file's `detect` rules matches here."""
        return any(rule.matches() for rule in cls.system_file.spec.detect)

    @override
    @classmethod
    def scratch_root(cls) -> Path | None:
        """Return the scratch root the system file's `scratch` rule finds, if any."""
        rule = cls.system_file.spec.scratch
        return rule.resolve() if rule is not None else None


def context_from_file(system_file: SystemFile) -> type[SpecSystemContext]:
    """Build the system context a system file describes.

    Parameters
    ----------
    system_file : SystemFile
        The file, as read.

    Returns
    -------
    type[SpecSystemContext]
    """
    spec = system_file.spec
    cpu = spec.toolchains["cpu"]
    return type(
        f"{spec.class_stem}SystemContext",
        (SpecSystemContext,),
        {
            "__doc__": f"The {spec.name} system, read from {system_file.path}.",
            "name": spec.name,
            "compiler": cpu.compiler,
            "mpi_prefix": cpu.mpi_prefix,
            "docs": spec.docs,
            "system_file": system_file,
            "_settings": spec.settings_klass(),
        },
    )


@functools.cache
def _bundled_files() -> tuple[SystemFile, ...]:
    """Read the system files C-Star ships, once per process.

    Raises
    ------
    SystemFileError
        If a bundled file is refused, which is a defect of the package.
    """
    return tuple(read_bundled_system_files())


@functools.cache
def _bundled_contexts() -> dict[str, type[SystemContext]]:
    """Build the contexts of the system files C-Star ships, once per process.

    Raises
    ------
    SystemFileError
        If a bundled file is refused, which is a defect of the package.
    """
    return {f.spec.name: context_from_file(f) for f in _bundled_files()}


@dataclass(frozen=True)
class SystemStack:
    """The system files C-Star found on one search path, and their contexts."""

    layers: SystemLayers
    """What was found, used, shadowed and refused."""
    contexts: dict[str, type[SystemContext]]
    """The context of each file in effect, the top layer's first."""


@functools.lru_cache(maxsize=8)
def _system_stack(roots: CatalogRoots) -> SystemStack:
    """Read the system files on a search path and build their contexts.

    Cached by the search path, so a changed `CSTAR_CATALOG` reads the layers
    again. Warns, once per process, about each file that shadows another and each
    file refused.
    """
    layers = read_system_layers(roots, _bundled_files())
    bundled = _bundled_contexts()
    contexts = {
        name: bundled[name] if f.layer == BUNDLED_LAYER else context_from_file(f)
        for name, f in layers.files.items()
    }
    for name, below in layers.shadowed.items():
        warn_once(
            f"The system file {layers.files[name].path} shadows "
            f"{below[0].path}; C-Star uses the first for {name!r}."
        )
    for refused in layers.refused:
        used = layers.files.get(refused.name)
        instead = (
            f" C-Star uses {used.path} for {refused.name!r} instead." if used else ""
        )
        warn_once(
            f"Skipping the system file {refused.path}: {refused.reason}.{instead}"
        )
    return SystemStack(layers, contexts)


def system_stack() -> SystemStack:
    """Return the system files and contexts on the current search path.

    The search path is the ``systems/`` directory of each local `CSTAR_CATALOG`
    root, then the bundled one; load the configuration file first.
    """
    return _system_stack(catalog_roots())


def reload_system_files() -> None:
    """Forget the system files read so far, so the next lookup reads them again.

    For a process that has just written one (`cstar system add`, `cstar init`).
    """
    _system_stack.cache_clear()


CTX_REGISTRY: dict[str, type[SystemContext]] = {}
"""The contexts registered in code with `register_sys_context`, by name; they
take precedence over a system file of the same name."""


def _registry() -> dict[str, type[SystemContext]]:
    """Return every registered context by name: the system files in effect, top
    layer first, then those registered in code.
    """
    return {**system_stack().contexts, **CTX_REGISTRY}


def register_sys_context(
    wrapped_cls: type[SystemContext],
) -> type[SystemContext]:
    """Register the decorated type as an available _SystemContext."""
    CTX_REGISTRY[wrapped_cls.name] = wrapped_cls

    @functools.wraps(wrapped_cls)
    def _inner() -> type[SystemContext]:
        """Return the original type after it is registered.

        Returns
        -------
        type[_SystemContext]
            The decorated type.
        """
        return wrapped_cls

    return _inner()


def get_registered_sys_contexts() -> Sequence[type[SystemContext]]:
    """Return a sequence containing all registered context types.

    Returns
    -------
    Sequence[type[_SystemContext]]
    """
    return list(_registry().values())


def registered_names() -> list[str]:
    """Return the names of the registered system contexts, sorted."""
    return sorted(_registry())


class SystemSource(StrEnum):
    """How the system context C-Star uses was chosen."""

    SETTING = auto()
    """`CSTAR_SYSTEM` (or `cstar init`, for its own walkthrough) named it."""
    DETECTED = auto()
    """C-Star detected a registered system."""
    FALLBACK = auto()
    """C-Star detected a cluster it does not know and uses the platform's generic
    context."""


@dataclass(frozen=True, slots=True)
class SystemSelection:
    """The system context C-Star uses on this machine, and how it was chosen."""

    context: type[SystemContext]
    """The context in use."""
    source: SystemSource
    """How the context was chosen."""
    detected: str = ""
    """The unrecognised system that was detected (its Lmod name, else the
    machine's network name), when `source` is `SystemSource.FALLBACK`."""

    def describe(self) -> str:
        """Name the context for display, saying how it was chosen unless detected,
        and which file describes it unless C-Star ships that file.
        """
        name = self.context.name
        details = []
        if self.source is SystemSource.SETTING:
            details.append(f"set by {ENV_CSTAR_SYSTEM}")
        elif self.source is SystemSource.FALLBACK:
            details.append(f"generic; {self.detected!r} is not a registered system")
        if note := file_in_use(self.context):
            details.append(note)
        return f"{name} ({'; '.join(details)})" if details else name

    def unsupported_scheduler(self, environ: "Mapping[str, str]") -> str:
        """Name the scheduler this machine shows that the context cannot submit to.

        A system C-Star detected is trusted to describe its own scheduler, or to
        have none. A context it fell back to, or one `CSTAR_SYSTEM` names, is
        asked only when it has no scheduler: the generic context pinned to stop
        the fallback warning must not stop the login-node warning as well.

        Parameters
        ----------
        environ : Mapping[str, str]
            The process's environment variables.

        Returns
        -------
        str
            The scheduler family (see `visible_scheduler`) when the context was not
            detected, has no scheduler, and the machine shows one; else empty.
        """
        if (
            self.source is SystemSource.DETECTED
            or self.context.create_scheduler() is not None
        ):
            return ""
        return visible_scheduler(environ)


def system_file_of(context: type[SystemContext]) -> SystemFile | None:
    """Return the system file a context was read from; `None` for one defined in code.

    A context registered in code need not declare the protocol's default.
    """
    return getattr(context, "system_file", None)


def display_path(path: Path) -> str:
    """Spell a path for display, with the home directory as ``~``."""
    home = Path.home()
    return f"~/{path.relative_to(home)}" if path.is_relative_to(home) else str(path)


def file_in_use(context: type[SystemContext]) -> str:
    """Say which system file describes a context, unless C-Star ships it.

    Returns
    -------
    str
        ``from <path>``, with ``, shadows <what>`` when the file hides one of the
        same name below it; empty for a bundled file or a context defined in code.
    """
    system_file = system_file_of(context)
    if system_file is None or system_file.layer == BUNDLED_LAYER:
        return ""
    note = f"from {display_path(system_file.path)}"
    layers = system_stack().layers
    in_effect = layers.files.get(context.name)
    if in_effect is not None and in_effect.path == system_file.path:
        if below := layers.shadowed.get(context.name):
            hidden = below[0]
            what = (
                BUNDLED_LAYER
                if hidden.layer == BUNDLED_LAYER
                else display_path(hidden.path)
            )
            note += f", shadows {what}"
    return note


def detection_would_select(spec: SystemSpec) -> bool:
    """Tell whether detection on this machine picks the system a spec describes.

    Ignores `CSTAR_SYSTEM`. The system is picked when Lmod reports its name, or,
    when Lmod reports no registered name, when one of its `detect` rules matches.
    Another system's own rule matching first is not considered.
    """
    lmod = HostNameEvaluator().lmod_hostname
    if lmod and (lmod == spec.name or lmod in _registry()):
        return lmod == spec.name
    return any(rule.matches() for rule in spec.detect)


def registered_context(name: str) -> type[SystemContext]:
    """Look up a registered system context by name, ignoring case and surrounding
    whitespace.

    Raises
    ------
    CstarError
        If no context is registered under the name; the message lists those that
        are.
    """
    key = name.strip().casefold()
    if klass := _registry().get(key):
        return klass
    layers = system_stack().layers
    searched = ", ".join(str(d) for d in layers.searched)
    if refused := layers.refusals(key):
        reasons = "; ".join(f"{r.path} is refused: {r.reason}" for r in refused)
        raise CstarError(
            f"{ENV_CSTAR_SYSTEM} names {name!r}, but its system file cannot be used: "
            f"{reasons}. Directories searched: {searched}."
        )
    raise CstarError(
        f"{name!r} is not a registered system; {ENV_CSTAR_SYSTEM} must name one of: "
        f"{', '.join(registered_names())}. System files are read from {searched} "
        f"(a systems/ directory under each local {ENV_CSTAR_CATALOG} entry)."
    )


def detect_system() -> SystemSelection:
    """Choose the system context from what the machine reports, ignoring
    `CSTAR_SYSTEM`.

    The detected name (see `HostNameEvaluator.name`) is used when it is
    registered, then any context that recognises the machine itself, then the
    generic context for the platform. The generic context is a fallback when
    Lmod named an unregistered system, or is present without naming one: either
    marks a cluster C-Star does not know rather than a workstation.

    Raises
    ------
    CstarError
        If the detected system is not registered and the platform has no generic
        context.
    OSError
        If no system name can be determined at all.
    """
    namer = HostNameEvaluator()
    detected = namer.name
    generic = namer.platform_hostname
    registry = _registry()
    if detected != generic and (klass := registry.get(detected)):
        return SystemSelection(klass, SystemSource.DETECTED)
    # an Lmod name C-Star does not know may still be a system that matches by its own test
    if klass := next((k for k in get_registered_sys_contexts() if k.is_match()), None):
        return SystemSelection(klass, SystemSource.DETECTED)
    if klass := registry.get(generic):
        if detected != generic:
            return SystemSelection(klass, SystemSource.FALLBACK, detected)
        if namer.lmod_settings.CMD:
            unnamed = namer.node_name or "unknown"
            return SystemSelection(klass, SystemSource.FALLBACK, unnamed)
        return SystemSelection(klass, SystemSource.DETECTED)
    raise CstarError(
        f"C-Star does not recognise the system {detected!r} and has no generic "
        f"context for this platform ({namer.platform_hostname or 'unknown'}); set "
        f"{ENV_CSTAR_SYSTEM} to one of: {', '.join(registered_names())}."
    )


def select_system() -> SystemSelection:
    """Choose the system context C-Star uses on this machine.

    `CSTAR_SYSTEM` wins over detection (see `detect_system`).

    Raises
    ------
    CstarError
        If `CSTAR_SYSTEM` names an unregistered system, or detection finds an
        unregistered system on a platform with no generic context.
    OSError
        If `CSTAR_SYSTEM` is unset and no system name can be determined at all.
    """
    if requested := get_env_item(ENV_CSTAR_SYSTEM).value.strip():
        return SystemSelection(registered_context(requested), SystemSource.SETTING)
    return detect_system()


def selected_system_name() -> str:
    """Return the name of the system C-Star uses on this machine.

    The one way to ask "which system is this": `CSTAR_SYSTEM` (exported, or saved
    in the configuration file), else detection, else the platform's generic context
    (see `select_system`). The configuration file is loaded first, so library and
    notebook use sees a saved setting as the CLI does.

    Raises
    ------
    CstarError
        If the configuration file is refused, `CSTAR_SYSTEM` names an
        unregistered system, or detection finds an unregistered system on a
        platform with no generic context.
    OSError
        If no system name can be determined at all.
    """
    load_user_config()
    return select_system().context.name


def selected_system_file() -> SystemFile | None:
    """Return the system file of the system C-Star uses on this machine.

    Loads the configuration file first, as `selected_system_name` does.

    Returns
    -------
    SystemFile | None
        The file, or `None` for a context defined in code.

    Raises
    ------
    CstarError
        As `selected_system_name` does.
    OSError
        If no system name can be determined at all.
    """
    load_user_config()
    return system_file_of(select_system().context)


def get_system_context() -> SystemContext:
    """Return the system context C-Star uses on this machine (see `select_system`).

    Raises
    ------
    CstarError
        If `CSTAR_SYSTEM` names an unregistered system, or detection finds an
        unregistered system on a platform with no generic context.
    OSError
        If no system name can be determined at all.
    """
    return select_system().context()


def system_label() -> str:
    """Name the system context in use for display (see `SystemSelection.describe`).

    Never raises: a display must not fail on a misconfigured system, so it shows
    why the system could not be determined instead. A configuration file the
    loader refuses is skipped, as `cstar env` skips it.
    """
    load_user_config_for_repair()
    try:
        return select_system().describe()
    except (CstarError, OSError) as ex:
        return f"not determined: {ex}"


_WARNED_SYSTEM: set[str] = set()
"""The system warnings already logged by this process."""


def warn_once(message: str) -> None:
    """Log a warning about the system unless this process already logged it."""
    if message not in _WARNED_SYSTEM:
        _WARNED_SYSTEM.add(message)
        log.warning(message)


def warn_fallback(selection: SystemSelection) -> None:
    """Log, once per process, that C-Star fell back to the generic context.

    Parameters
    ----------
    selection : SystemSelection
        The selection; nothing is logged unless its source is
        `SystemSource.FALLBACK`.
    """
    if selection.source is not SystemSource.FALLBACK:
        return
    name = selection.context.name
    warn_once(
        f"C-Star does not recognise the system {selection.detected!r} and uses the "
        f"generic {name} context: it submits no jobs, and builds with the compilers "
        f"and MPI of your conda environment. Set {ENV_CSTAR_SYSTEM} to the system's "
        f"name if C-Star lists it, or run `cstar env set {ENV_CSTAR_SYSTEM} {name}` "
        "to keep the generic context without this warning; to submit jobs here, "
        f"register the system (see {DOCS_MACHINES_URL})."
    )


class CStarSystemManager:
    """Manage system-specific configuration and resources."""

    _selection: Final[SystemSelection]
    """The system context chosen for this machine, and how it was chosen."""
    _context: Final[SystemContext]
    """A context object configured for the current system."""
    _environments: Final[dict[ToolchainName, CStarEnvironment]]
    """The environment managers built so far, by toolchain; ``cpu`` is built eagerly."""
    _active: ToolchainName | None
    """The toolchain whose modules and env are loaded in this process."""
    _scheduler: Final[Scheduler | None]
    """The scheduler appropriate for this system."""

    def __init__(self) -> None:
        """Initialize the CStarSystemManager.

        Initialize the system manager by choosing the system context (see
        `select_system`) and initializing the environment and scheduler from it,
        warning once per process when C-Star fell back to the generic context. The
        user's configuration file is loaded first, so library and notebook use sees
        it as the CLI does.

        Raises
        ------
        CstarUserConfigError
            If the configuration file names settings it may not set.
        CstarError
            If `CSTAR_SYSTEM` names an unregistered system, or detection finds an
            unregistered system on a platform with no generic context.
        """
        load_user_config()
        self._selection = select_system()
        warn_fallback(self._selection)
        self._context = self._selection.context()
        self._environments = {}
        self._active = None
        self.environment_for("cpu")

        self._scheduler = self._context.create_scheduler()

    @property
    def name(self) -> str:
        """Get the name of this system.

        Returns
        -------
        str
            The system name
        """
        return self._context.name

    @property
    def selection(self) -> SystemSelection:
        """Get the system context chosen for this machine, and how it was chosen."""
        return self._selection

    @property
    def environment(self) -> CStarEnvironment:
        """Get the environment manager for the toolchain active in this process.

        Reading this never switches profiles: while a GPU toolchain is active it
        is the GPU environment, otherwise the CPU one (which `__init__` activates).
        Use `environment_for` to switch.

        Returns
        -------
        CStarEnvironment
            The environment manager
        """
        return self._environments[self._active or "cpu"]

    def environment_for(self, name: ToolchainName) -> CStarEnvironment:
        """Get the environment manager for one of the system's toolchains.

        An environment is built on first request and cached. Activating one loads
        its profile's modules and env into `os.environ` for the process, as the
        CPU profile always has. Only one profile is active in the process: asking
        for another restores the active profile's env variables (see
        `CStarEnvironment.unload`) and then loads the requested one, so a profile
        never inherits another's paths. Asking for the active profile changes
        nothing.

        Parameters
        ----------
        name : ToolchainName
            The toolchain whose environment to return.

        Returns
        -------
        CStarEnvironment
            The environment manager

        Raises
        ------
        CstarError
            If `name` is ``gpu`` and the system registers no GPU toolchain. If the
            requested profile fails to load, the profile that was active is loaded
            again before the error propagates.
        """
        if name == self._active:
            return self._environments[name]

        # resolve the toolchain first: an unregistered one must not disturb the active
        tc = None if name in self._environments else self._context.toolchain(name)

        previous = self._active
        if previous is not None:
            self._environments[previous].unload()
            self._active = None

        try:
            if tc is None:
                self._environments[name].load()
            else:
                system_file = self._context.system_file
                self._environments[name] = CStarEnvironment(
                    system_name=self._context.name,
                    mpi_exec_prefix=tc.mpi_prefix,
                    compiler=tc.compiler,
                    system_settings_klass=self._context.settings_klass(),
                    profile=tc.profile,
                    modules=tc.modules,
                    env=tc.env,
                    source=system_file.path if system_file is not None else None,
                )
        except Exception:
            # a profile that fails to load must not leave the process with none
            if previous is not None:
                try:
                    self._environments[previous].load()
                    self._active = previous
                except Exception:
                    # keep the original error; the reload's own is only logged
                    log.warning(
                        "Could not reload the %s profile after the %s profile failed "
                        "to load; no toolchain profile is loaded in this process",
                        previous,
                        name,
                        exc_info=True,
                    )
            raise
        self._active = name
        return self._environments[name]

    def toolchain(self, name: ToolchainName) -> Toolchain:
        """Get the compiler, MPI and module profile the system offers under `name`.

        Unlike `environment_for`, this never switches the active profile.

        Parameters
        ----------
        name : ToolchainName
            The toolchain to describe.

        Returns
        -------
        Toolchain
            The toolchain's compiler, MPI prefix, profile and GPU build facts.

        Raises
        ------
        CstarError
            If the system does not register the toolchain, as for ``gpu`` on a
            system without GPU nodes.
        """
        return self._context.toolchain(name)

    @property
    def scheduler(self) -> Scheduler | None:
        """Get the scheduler for this system.

        Returns
        -------
        Scheduler
            The system scheduler
        """
        return self._scheduler


@dataclass(frozen=True)
class OutsideSchedulerJob:
    """A process outside any scheduler job on a machine with a scheduler (a login node, say)."""

    system: str
    """The system's name; for a context C-Star fell back to or was told to use,
    how it was chosen (`SystemSelection.describe`)."""
    unsupported_scheduler: str = ""
    """The scheduler family the machine shows that the system's context cannot
    submit to; empty when the context has its own scheduler."""

    @property
    def certain(self) -> bool:
        """Whether C-Star knows the process is outside a job: the context has a scheduler."""
        return not self.unsupported_scheduler


def outside_scheduler_job(sysmgr: "CStarSystemManager") -> OutsideSchedulerJob | None:
    """Say whether this process runs outside a scheduler job where it should be in one.

    Parameters
    ----------
    sysmgr : CStarSystemManager
        The system manager of this machine.

    Returns
    -------
    OutsideSchedulerJob | None
        Where the process is, or `None` inside a scheduler job, with
        `CSTAR_ALLOW_LOGIN_NODE_RUN=1`, or on a machine that shows no scheduler.
    """
    if in_scheduler_job(os.environ) or is_flag_enabled(ENV_CSTAR_ALLOW_LOGIN_NODE_RUN):
        return None
    if sysmgr.scheduler is not None:
        return OutsideSchedulerJob(system=sysmgr.name)
    selection = sysmgr.selection
    if family := selection.unsupported_scheduler(os.environ):
        return OutsideSchedulerJob(
            system=selection.describe(), unsupported_scheduler=family
        )
    return None


@functools.lru_cache
def get_sysmgr() -> CStarSystemManager:
    """Return a cached system manager instance."""
    return CStarSystemManager()
