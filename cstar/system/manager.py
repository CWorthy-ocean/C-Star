import functools
import platform as platform
from dataclasses import dataclass
from typing import ClassVar, Final, Protocol

from pydantic import Field, ValidationError

from cstar.base.exceptions import CstarError
from cstar.system.environment import (
    CStarEnvironment,
    EnvSettingsBase,
    LmodEnvSettings,
    SlurmSettingsBase,
)
from cstar.system.scheduler import (
    PBSQueue,
    PBSScheduler,
    Scheduler,
    SlurmPartition,
    SlurmQOS,
    SlurmScheduler,
    query_max_walltime_via_sacctmgr,
)


class AnvilEnvSettings(SlurmSettingsBase):
    """Environment variables required to execute a simulation on the *Anvil* system.

    `AnvilEnvSettings` overrides behaviors of `SlurmSettingsBase` by implementing a unique
    hostname matching test in `is_match`.
    """

    HOST_IDENTIFIER: Final[str] = "anvil"
    """Constant value in `RCAC_CLUSTER` env var on *Anvil* that uniquely identifies the system."""
    RCAC_CLUSTER: str = Field(default="", alias="RCAC_CLUSTER")
    """The hostname of the machine.

    Used to identify the system as `Anvil` by matching value: `RCAC_CLUSTER=anvil`
    """

    @property
    def is_match(self) -> bool:
        """Return `True` if the current system can be identified as *Anvil* by
        inspecting the system hostname.

        Returns
        -------
        bool
        """
        return self.RCAC_CLUSTER == AnvilEnvSettings.HOST_IDENTIFIER


class EljaEnvSettings(SlurmSettingsBase):
    """Environment variables required to execute a simulation on the *Elja* system.

    NOTE: Elja does not support SLURM account names.
    """

    HOST_IDENTIFIER: Final[str] = "elja-irhpc"
    """Fixed value in HOSTNAME env var on Elja that uniquely identifies the system."""

    HOSTNAME: str = Field(default="")
    """The hostname of the machine.

    Used to identify the system as Elja by matching value: `elja-irhpc`
    """
    SLURM_QUEUE: str = Field(default="")
    """The SLURM queue name."""
    OMP_NUM_THREADS: str = Field(default="1", alias="OMP_NUM_THREADS")
    """The number of threads to be used by OpenMPI"""
    MKL_NUM_THREADS: str = Field(default="1", alias="MKL_NUM_THREADS")
    """The number of threads used by MKL"""

    @property
    def is_match(self) -> bool:
        """Return `True` if the current system is identified as *Elja*."""
        return self.HOSTNAME == self.HOST_IDENTIFIER


class HostNameEvaluator:
    """Container of host-specific names used to determine the system name that will be
    used by C-Star.
    """

    lmod_settings: Final[LmodEnvSettings]
    """LMOD-specific environment configuration."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.lmod_settings = LmodEnvSettings()

    @property
    def platform_name(self) -> str:
        return platform.system()

    @property
    def machine_name(self) -> str:
        return platform.machine()

    @property
    def platform_hostname(self) -> str:
        """Aggregate of machine and platform sysname."""
        value = ""
        if self.platform_name and self.machine_name:
            value = f"{self.platform_name}_{self.machine_name}"
        return value.casefold()

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
        """Determine the system name that will be used by C-Star.

        Prioritizes the LMOD-based system name. When LMOD-specific environment
        variables are not present, the name is determiend by the OS.

        Raises
        ------
        EnvironmentError
            If the name cannot be determined.
        """
        if self.lmod_hostname:
            return self.lmod_hostname

        try:
            if EljaEnvSettings().is_match:
                return _EljaSystemContext.name
        except ValidationError:
            ...  # not elja

        try:
            if AnvilEnvSettings().is_match:
                return _AnvilSystemContext.name
        except ValidationError:
            ...  # not anvil

        if self.platform_hostname:
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


class _SystemContext(Protocol):
    """The contextual dependencies for the system/platform."""

    name: ClassVar[str]
    """The unique name identifying the context."""
    compiler: ClassVar[str]
    """The compiler used when building software for the system."""
    mpi_prefix: ClassVar[str]
    """The prefix used when executing mpiexec for the system."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        """Instantiate a scheduler configured for the system."""

    @classmethod
    def settings_klass(cls) -> type[EnvSettingsBase] | None:
        """Return the type used to load settings required by the target system.

        NOTE: The type is returned to avoid validation failures at import time due
        to the instantation of the global `cstar_sysmgr`.
        """
        return EnvSettingsBase


_registry: dict[str, type[_SystemContext]] = {}


def register_sys_context(
    wrapped_cls: type[_SystemContext],
) -> type[_SystemContext]:
    """Register the decorated type as an available _SystemContext."""
    _registry[wrapped_cls.name] = wrapped_cls

    @functools.wraps(wrapped_cls)
    def _inner() -> type[_SystemContext]:
        """Return the original type after it is registered.

        Returns
        -------
        type[_SystemContext]
            The decorated type.
        """
        return wrapped_cls

    return _inner()


def _get_system_context() -> _SystemContext:
    """Retrieve a system context from the context registry.

    Returns
    -------
    _SystemContext
        The context matching the supplied name.

    Raises
    ------
    CStarError
        If the supplied name has not been registered.
    """
    namer = HostNameEvaluator()

    if klass := _registry.get(namer.name):
        return klass()

    raise CstarError(f"Unknown system requested: {namer.name}")


@register_sys_context
@dataclass(frozen=True)
class _PerlmutterSystemContext(_SystemContext):
    """The contextual dependencies for the Perlmutter system."""

    name: ClassVar[str] = "perlmutter"
    """The unique name identifying the Perlmutter system."""
    compiler: ClassVar[str] = "gnu"
    """The compiler used on Perlmutter."""
    mpi_prefix: ClassVar[str] = "srun"
    """The MPI prefix used on Perlmutter."""
    docs: ClassVar[str] = "https://docs.nersc.gov/systems/perlmutter/architecture/"
    """URI for documentation of the Perlmutter system."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        per_regular_q = SlurmQOS(name="regular", query_name="regular_1")
        per_shared_q = SlurmQOS(name="shared")
        per_debug_q = SlurmQOS(name="debug")

        return SlurmScheduler(
            queues=[per_regular_q, per_shared_q, per_debug_q],
            primary_queue_name="regular",
            other_scheduler_directives={"-C": "cpu"},
            requires_task_distribution=False,
            documentation=cls.docs,
            max_cpus_per_node=128,
        )


@register_sys_context
@dataclass(frozen=True)
class _AnvilSystemContext(_SystemContext):
    """The contextual dependencies for the Anvil system."""

    name: ClassVar[str] = "anvil"
    """The unique name identifying the Anvil system."""
    compiler: ClassVar[str] = "gnu"
    """The compiler used on Anvil."""
    mpi_prefix: ClassVar[str] = "srun"
    """The MPI prefix used on Anvil."""
    docs: ClassVar[str] = "https://www.rcac.purdue.edu/knowledge/anvil/architecture"
    """URI for documentation of the Anvil system."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        regular_q = SlurmPartition(
            name="wholenode",
            query_name="part-standard",
            max_walltime_method=query_max_walltime_via_sacctmgr,
        )
        shared_q = SlurmPartition(
            name="shared",
            query_name="part-shared",
            max_walltime_method=query_max_walltime_via_sacctmgr,
        )
        debug_q = SlurmPartition(
            name="debug",
            query_name="part-debug",
            max_walltime_method=query_max_walltime_via_sacctmgr,
        )

        return SlurmScheduler(
            queues=[regular_q, shared_q, debug_q],
            primary_queue_name="wholenode",
            other_scheduler_directives={},
            requires_task_distribution=False,
            documentation=cls.docs,
            max_cpus_per_node=128,
        )

    @classmethod
    def settings_klass(cls) -> type[SlurmSettingsBase] | None:
        """Return the type used to load settings required by the target system."""
        return AnvilEnvSettings


@register_sys_context
@dataclass(frozen=True)
class _DerechoSystemContext(_SystemContext):
    """The contextual dependencies for the Derecho system."""

    name: ClassVar[str] = "derecho"
    """The unique name identifying the Derecho system."""
    compiler: ClassVar[str] = "intel"
    """The compiler used on Derecho."""
    mpi_prefix: ClassVar[str] = "mpirun"
    """The MPI prefix used on Derecho."""
    docs: ClassVar[str] = (
        "https://ncar-hpc-docs.readthedocs.io/en/latest/compute-systems/derecho/"
    )
    """URI for documentation of the Derecho system."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        # https://ncar-hpc-docs.readthedocs.io/en/latest/pbs/charging/
        der_main_q = PBSQueue(name="main", max_walltime="12:00:00")
        der_preempt_q = PBSQueue(name="preempt", max_walltime="24:00:00")
        der_develop_q = PBSQueue(name="develop", max_walltime="6:00:00")

        return PBSScheduler(
            queues=[der_main_q, der_preempt_q, der_develop_q],
            primary_queue_name="main",
            requires_task_distribution=True,
            documentation=cls.docs,
        )


@register_sys_context
@dataclass(frozen=True)
class _EljaSystemContext(_SystemContext):
    """The contextual dependencies for the Elja system."""

    name: ClassVar[str] = "elja"
    """The unique name identifying the Elja system."""
    compiler: ClassVar[str] = "gnu"
    """The compiler used on Elja."""
    mpi_prefix: ClassVar[str] = "mpirun"
    """The MPI prefix used on Eja."""
    docs: ClassVar[str] = "https://wiki.irei.is"
    """URI for documentation of the Elja system."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        regular_q = SlurmPartition(
            name="128cpu_256mem",
            query_name="128cpu_256mem",
            max_walltime_method=query_max_walltime_via_sacctmgr,
        )
        any_cpu = SlurmPartition(
            name="any_cpu",
            query_name="any_cpu",
            max_walltime_method=query_max_walltime_via_sacctmgr,
        )
        shared_q = SlurmPartition(
            name="64cpu_256mem",
            query_name="64cpu_256mem",
            max_walltime_method=query_max_walltime_via_sacctmgr,
        )
        debug_q = SlurmPartition(
            name="48cpu_192mem",
            query_name="48cpu_192mem",
            max_walltime_method=query_max_walltime_via_sacctmgr,
        )

        return SlurmScheduler(
            queues=[any_cpu, regular_q, shared_q, debug_q],
            primary_queue_name="any_cpu",  # 128cpu_256mem",
            other_scheduler_directives={},
            requires_task_distribution=True,
            documentation=cls.docs,
            max_cpus_per_node=128,
        )

    @classmethod
    def settings(cls) -> EnvSettingsBase | None:
        """Return the settings required by the target system.

        Raises
        ------
        ValidationError
            If required environment variables are not set.
        """
        return EljaEnvSettings()


@register_sys_context
@dataclass(frozen=True)
class _ExpanseSystemContext(_SystemContext):
    """The contextual dependencies for the Expanse system."""

    name: ClassVar[str] = "expanse"
    """The unique name identifying the Expanse system."""
    compiler: ClassVar[str] = "intel"
    """The compiler used on Expanse."""
    mpi_prefix: ClassVar[str] = "srun --mpi=pmi2"
    """The MPI prefix used on Expanse."""
    docs: ClassVar[str] = "https://www.sdsc.edu/support/user_guides/expanse.html"
    """URI for documentation of the Expanse system."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        exp_compute_q = SlurmPartition(name="compute")
        exp_debug_q = SlurmPartition(name="debug")
        return SlurmScheduler(
            queues=[exp_compute_q, exp_debug_q],
            primary_queue_name="compute",
            requires_task_distribution=True,
            documentation=cls.docs,
        )


@register_sys_context
@dataclass(frozen=True)
class _LinuxSystemContext(_SystemContext):
    """The contextual dependencies for the Linux system on the x86_64 platform."""

    name: ClassVar[str] = "linux_x86_64"
    """The unique name identifying the Linux system on an X86_64 platform."""
    compiler: ClassVar[str] = "gnu"
    """The compiler used on Linux."""
    mpi_prefix: ClassVar[str] = "mpirun"
    """The MPI prefix used on Linux."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        """Return None - a scheduler on the Linux system is not supported."""
        return None


@register_sys_context
@dataclass(frozen=True)
class _MacOSSystemContext(_SystemContext):
    name: ClassVar[str] = "darwin_arm64"
    """The unique name identifying the MacOS system on an ARM64 platform."""
    compiler: ClassVar[str] = "gnu"
    """The compiler used on MacOS."""
    mpi_prefix: ClassVar[str] = "mpirun"
    """The MPI prefix used on MacOS."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        """Return None - a scheduler on the MacOS system is not supported."""
        return None


@register_sys_context
@dataclass(frozen=True)
class _LinuxARM64SystemContext(_SystemContext):
    name: ClassVar[str] = "linux_aarch64"
    """The unique name identifying the Linux system on an ARM64 platform."""
    compiler: ClassVar[str] = "gnu"
    """The compiler used on ARM64 Linux."""
    mpi_prefix: ClassVar[str] = "mpirun"
    """The MPI prefix used on Linux."""

    @classmethod
    def create_scheduler(cls) -> Scheduler | None:
        """Return None - a scheduler on the Linux system is not supported."""
        return None


class CStarSystemManager:
    """Manage system-specific configuration and resources."""

    _context: Final[_SystemContext]
    """A context object configured for the current system."""
    _environment: Final[CStarEnvironment]
    """An environment manager configured for the current system."""
    _scheduler: Final[Scheduler | None]
    """The scheduler appropriate for this system."""

    def __init__(self) -> None:
        """Initialize the CStarSystemManager.

        Initialize the system manager by determining the system name and initializing
        the environment and scheduler based on that name.
        """
        self._context = _get_system_context()
        self._environment = CStarEnvironment(
            system_name=self._context.name,
            mpi_exec_prefix=self._context.mpi_prefix,
            compiler=self._context.compiler,
            system_settings_klass=self._context.settings_klass(),
        )

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
    def environment(self) -> CStarEnvironment:
        """Get the environment manager for this system.

        Returns
        -------
        CStarEnvironment
            The environment manager
        """
        return self._environment

    @property
    def scheduler(self) -> Scheduler | None:
        """Get the scheduler for this system.

        Returns
        -------
        Scheduler
            The system scheduler
        """
        return self._scheduler


cstar_sysmgr = CStarSystemManager()
