import os
import platform
from typing import Optional
from cstar.base.environment import CStarEnvironment
from cstar.base.scheduler import (
    Scheduler,
    SlurmScheduler,
    SlurmQueue,
    PBSQueue,
    PBSScheduler,
)


class CStarSystem:
    _environment: Optional[CStarEnvironment] = None
    _scheduler: Optional[Scheduler] = None

    @property
    def scheduler(self) -> Optional[Scheduler]:
        """todo."""
        if self._scheduler is not None:
            return self._scheduler

        match self.name:
            case "perlmutter":
                # regular -> regular_1 for < 124 nodes, regular_0 for >124 nodes
                regular_q = SlurmQueue(name="regular", query_name="regular_1")
                shared_q = SlurmQueue(name="shared")
                debug_q = SlurmQueue(name="debug")

                self._scheduler = SlurmScheduler(
                    queues=[regular_q, shared_q, debug_q],
                    primary_queue_name="regular",
                    queue_flag="qos",
                    other_scheduler_directives={"-C": "cpu"},
                )
            case "derecho":
                # https://ncar-hpc-docs.readthedocs.io/en/latest/pbs/charging/
                main_q = PBSQueue(name="main", max_walltime="12:00:00")
                preempt_q = PBSQueue(name="preempt", max_walltime="24:00:00")
                develop_q = PBSQueue(name="develop", max_walltime="6:00:00")

                self._scheduler = PBSScheduler(
                    queues=[main_q, preempt_q, develop_q],
                    primary_queue_name="main",
                    queue_flag="q",
                )
            case _:
                self._scheduler = None

        return self._scheduler

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

    @property
    def environment(self) -> CStarEnvironment:
        """Returns a CStarEnvironment class instance corresponding to this machine.

        The instance is created when the property is first accessed and cached for
        future queries
        """
        if self._environment is not None:
            return self._environment

        match self.name:
            case "expanse":
                mpi_exec_prefix = "srun --mpi=pmi2"
                compiler = "intel"
                # queue_flag = "partition"
                # primary_queue = "compute"
                # mem_per_node_gb = 256
                # cores_per_node = 128
                # max_walltime = "48:00:00"
                # other_scheduler_directives = {}

            case "perlmutter":
                mpi_exec_prefix = "srun"
                compiler = "gnu"
                # queue_flag = "qos"
                # primary_queue = "regular"
                # mem_per_node_gb = 512
                # cores_per_node = 128  # for CPU nodes
                # max_walltime = "24:00:00"
                # other_scheduler_directives = {"-C": "cpu"}

            case "derecho":
                mpi_exec_prefix = "mpirun"
                compiler = "intel"
                # queue_flag = "q"
                # primary_queue = "main"
                # cores_per_node = 128
                # mem_per_node_gb = 256
                # max_walltime = "12:00:00"
                # other_scheduler_directives = {}

            case "darwin_arm64" | "linux_x86_64":
                mpi_exec_prefix = "mpirun"
                compiler = "gnu"
                # queue_flag = None
                # primary_queue = None
                # max_walltime = None
                # cores_per_node = os.cpu_count()
                # mem_per_node_gb = (
                #     os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / (1024**3)
                # )
                # other_scheduler_directives = {}
            case _:
                raise EnvironmentError("Unsupported environment")

        self._environment = CStarEnvironment(
            system_name=self.name, mpi_exec_prefix=mpi_exec_prefix, compiler=compiler
        )
        return self._environment


cstar_system = CStarSystem()
