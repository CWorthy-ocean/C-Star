import functools
import os
import platform as platform_
from dataclasses import dataclass, field
from typing import ClassVar, Protocol

from cstar.base.exceptions import CstarError
from cstar.system.environment import CStarEnvironment
from cstar.system.scheduler import (
    PBSQueue,
    PBSScheduler,
    Scheduler,
    SlurmPartition,
    SlurmQOS,
    SlurmScheduler,
)


@dataclass(frozen=True)
class HostNameEvaluator:
    """Container of host-specific names used to determine the system name that will be
    used by C-Star."""

    lmod_syshost: str = field(default="", init=False)
    """The lmod-specific hostname."""
    lmod_sysname: str = field(default="", init=False)
    """The lmod-specific system name."""
    lmod_name: str = field(default="", init=False)
    """Aggregate of lmod host and lmod name."""
    platform: str = field(default="", init=False)
    """The platform name."""
    machine: str = field(default="", init=False)
    """The machine name."""
    platform_name: str = field(default="", init=False)
    """Aggregate of machine and platform sysname."""

    ENV_LMOD_SYSHOST: ClassVar[str] = "LMOD_SYSHOST"
    ENV_LMOD_SYSNAME: ClassVar[str] = "LMOD_SYSTEM_NAME"

    def __post_init__(self) -> None:
        """Initialize the non-init and calculated attributes of an instance.

        NOTE: make use of setattr because attributes are read-only.
        """
        # ruff: noqa: B010
        setattr_ = object.__setattr__
        setattr_(self, "lmod_syshost", os.environ.get(self.ENV_LMOD_SYSHOST, ""))
        setattr_(self, "lmod_sysname", os.environ.get(self.ENV_LMOD_SYSNAME, ""))
        setattr_(self, "lmod_name", (self.lmod_syshost or self.lmod_sysname).casefold())
        setattr_(self, "platform", platform_.system())
        setattr_(self, "machine", platform_.machine())
        setattr_(
            self,
            "platform_name",
            (
                f"{self.platform}_{self.machine}"
                if self.platform and self.machine
                else ""
            ).casefold(),
        )

    @property
    def _diagnostic(self) -> str:
        """Return a string useful for diagnosing failures to identify the name."""
        return (
            f"{self.lmod_syshost=}, {self.lmod_sysname=}, "
            f"{self.platform=}, {self.machine=}"
        )

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
        if self.lmod_name:
            return self.lmod_name

        if self.platform_name:
            return self.platform_name

        raise EnvironmentError(
            f"C-Star cannot determine your system name. Diagnostics: {self._diagnostic}"
        )


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


_registry: dict[str, type[_SystemContext]] = {}


def register_sys_context(
    cls_: type[_SystemContext],
) -> type[_SystemContext]:
    """Register the decorated type as an available _SystemContext."""
    _registry[cls_.name] = cls_

    @functools.wraps(cls_)
    def _inner() -> type[_SystemContext]:
        """Return the original type after it is registered.

        Returns
        -------
        type[_SystemContext]
            The decorated type.
        """
        return cls_

    return _inner()


def _get_system_context(name: str) -> _SystemContext:
    """Retrieve a system context from the context registry.

    Parameters
    ----------
    name : str
        The name of the system to retrieve a context for

    Returns
    -------
    _SystemContext
        The context matching the supplied name.

    Raises
    ------
    CStarError
        If the supplied name has not been registered.
    """
    if type_ := _registry.get(name):
        return type_()

    raise CstarError(f"Unknown system requested: {name}")


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
        )


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


class CStarSystemManager:
    """Manage system-specific configuration and resources."""

    def __init__(self) -> None:
        """Initialize the CStarSystemManager.

        Initialize the system manager by determining the system name and initializing
        the environment and scheduler based on that name.
        """
        namer = HostNameEvaluator()
        self._context = _get_system_context(namer.name)
        """A context object configured for the current system."""
        self._environment = CStarEnvironment(
            system_name=self._context.name,
            mpi_exec_prefix=self._context.mpi_prefix,
            compiler=self._context.compiler,
        )
        """An environment manager configured for the current system."""

        self._scheduler = self._context.create_scheduler()
        """The scheduler appropriate for this system."""

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
