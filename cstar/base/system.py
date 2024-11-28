import os
import platform
from typing import Optional, Dict
from cstar.base.environment import CStarEnvironment
# from cstar.base.scheduler import Scheduler


class CStarSystem:
    _environment: Optional[CStarEnvironment] = None

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

        # mypy requires consistent typing across cases, declaring here:
        queue_flag: Optional[str]
        primary_queue: Optional[str]
        mem_per_node_gb: Optional[float]
        cores_per_node: Optional[int]
        max_walltime: Optional[str]
        other_scheduler_directives: Optional[Dict[str, str]]

        match self.name:
            case "expanse":
                mpi_exec_prefix = "srun --mpi=pmi2"
                compiler = "intel"
                queue_flag = "partition"
                primary_queue = "compute"
                mem_per_node_gb = 256
                cores_per_node = 128
                max_walltime = "48:00:00"
                other_scheduler_directives = {}

            case "perlmutter":
                mpi_exec_prefix = "srun"
                compiler = "gnu"
                queue_flag = "qos"
                primary_queue = "regular"
                mem_per_node_gb = 512
                cores_per_node = 128  # for CPU nodes
                max_walltime = "24:00:00"
                other_scheduler_directives = {"-C": "cpu"}

            case "derecho":
                mpi_exec_prefix = "mpirun"
                compiler = "intel"
                queue_flag = "q"
                primary_queue = "main"
                cores_per_node = 128
                mem_per_node_gb = 256
                max_walltime = "12:00:00"
                other_scheduler_directives = {}

            case "darwin_arm64" | "linux_x86_64":
                mpi_exec_prefix = "mpirun"
                compiler = "gnu"
                queue_flag = None
                primary_queue = None
                max_walltime = None
                cores_per_node = os.cpu_count()
                mem_per_node_gb = (
                    os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / (1024**3)
                )
                other_scheduler_directives = {}
            case _:
                raise EnvironmentError("Unsupported environment")

        self._environment = CStarEnvironment(
            system_name=self.name,
            mpi_exec_prefix=mpi_exec_prefix,
            compiler=compiler,
            queue_flag=queue_flag,
            primary_queue=primary_queue,
            mem_per_node_gb=mem_per_node_gb,
            cores_per_node=cores_per_node,
            max_walltime=max_walltime,
            other_scheduler_directives=other_scheduler_directives,
        )
        return self._environment


cstar_system = CStarSystem()
