import os
import platform
from enum import Enum
from typing import Optional
from cstar.system.environment import CStarEnvironment
from cstar.system.scheduler import (
    Scheduler,
    SlurmScheduler,
    SlurmQueue,
    PBSQueue,
    PBSScheduler,
)


class SystemName(Enum):
    """Enum for representing the names of supported systems.

    Each member corresponds to a specific system name used in the
    application, derived from environment variables or platform information.

    Members:
    --------
        PERLMUTTER: Represents the "perlmutter" system.
        EXPANSE: Represents the "expanse" system.
        DERECHO: Represents the "derecho" system.
        DARWIN_ARM64: Represents a Darwin-based ARM64 system.
        LINUX_X86_64: Represents a Linux-based x86_64 system.

    Usage:
    ------
        SystemName.PERLMUTTER          # Accessing a member
        SystemName("expanse")          # Converting from a string
    """

    PERLMUTTER = "perlmutter"
    EXPANSE = "expanse"
    DERECHO = "derecho"
    DARWIN_ARM64 = "darwin_arm64"
    LINUX_X86_64 = "linux_x86_64"


class CStarSystemManager:
    _environment: Optional[CStarEnvironment] = None
    _scheduler: Optional[Scheduler] = None

    @property
    def name(self) -> str:
        """Determines the system name based on environment variables or platform
        details.

        Checks for Lmod-specific variables (`LMOD_SYSHOST` or `LMOD_SYSTEM_NAME`).
        Otherwise, constructs a system name using `platform.system()` and
        `platform.machine()`.

        Returns
        -------
        str
            The system's name in lowercase.

        Raises
        ------
        EnvironmentError
            If the system name cannot be determined from environment variables or platform information.
        """

        sysname = os.environ.get("LMOD_SYSHOST", default="") or os.environ.get(
            "LMOD_SYSTEM_NAME"
        )
        if sysname:
            pass
        elif (platform.system() is not None) and (platform.machine() is not None):
            sysname = platform.system() + "_" + platform.machine()
        else:
            raise EnvironmentError(
                f"C-Star cannot determine your system name. Diagnostics: "
                f"LMOD_SYSHOST={os.environ.get('LMOD_SYSHOST')}, "
                f"LMOD_SYSTEM_NAME={os.environ.get('LMOD_SYSTEM_NAME')}, "
                f"platform.system()={platform.system()}, "
                f"platform.machine()={platform.machine()}"
            )
            # raise EnvironmentError("C-Star cannot determine your system name")

        return sysname.casefold()

    def _system_name_enum(self) -> SystemName:
        """Converts the system name string to a validated SystemName enum."""
        return SystemName(self.name)

    @property
    def scheduler(self) -> Optional[Scheduler]:
        """todo."""
        if self._scheduler is not None:
            return self._scheduler

        match self._system_name_enum():
            case SystemName.PERLMUTTER:
                # regular -> regular_1 for < 124 nodes, regular_0 for >124 nodes
                regular_q = SlurmQueue(name="regular", query_name="regular_1")
                shared_q = SlurmQueue(name="shared")
                debug_q = SlurmQueue(name="debug")

                self._scheduler = SlurmScheduler(
                    queues=[regular_q, shared_q, debug_q],
                    primary_queue_name="regular",
                    queue_flag="qos",
                    other_scheduler_directives={"-C": "cpu"},
                    requires_task_distribution=False,
                )
            case SystemName.DERECHO:
                # https://ncar-hpc-docs.readthedocs.io/en/latest/pbs/charging/
                main_q = PBSQueue(name="main", max_walltime="12:00:00")
                preempt_q = PBSQueue(name="preempt", max_walltime="24:00:00")
                develop_q = PBSQueue(name="develop", max_walltime="6:00:00")

                self._scheduler = PBSScheduler(
                    queues=[main_q, preempt_q, develop_q],
                    primary_queue_name="main",
                    queue_flag="q",
                    requires_task_distribution=True,
                )
            case SystemName.EXPANSE:
                compute_q = SlurmQueue(name="compute")
                debug_q = SlurmQueue(name="debug")
                self._scheduler = SlurmScheduler(
                    queues=[compute_q, debug_q],
                    primary_queue_name="compute",
                    queue_flag="partition",
                    requires_task_distribution=True,
                )
            case _:
                self._scheduler = None

        return self._scheduler

    @property
    def environment(self) -> CStarEnvironment:
        """Returns a CStarEnvironment class instance corresponding to this machine.

        The instance is created when the property is first accessed and cached for
        future queries
        """
        if self._environment is not None:
            return self._environment

        match self._system_name_enum():
            case SystemName.EXPANSE:
                mpi_exec_prefix = "srun --mpi=pmi2"
                compiler = "intel"

            case SystemName.PERLMUTTER:
                mpi_exec_prefix = "srun"
                compiler = "gnu"
            case SystemName.DERECHO:
                mpi_exec_prefix = "mpirun"
                compiler = "intel"
            case SystemName.DARWIN_ARM64 | SystemName.LINUX_X86_64:
                mpi_exec_prefix = "mpirun"
                compiler = "gnu"

        self._environment = CStarEnvironment(
            system_name=self.name, mpi_exec_prefix=mpi_exec_prefix, compiler=compiler
        )
        return self._environment


cstar_sysmgr = CStarSystemManager()
