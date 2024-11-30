## STIL TODO:
#
# Fill out `status` property def (unsubmitted, queued, complete, cancelled)
# Add `create_job` on SlurmScheduler that passes args to create SlurmJob
# Integrate this new framework into Component.run() etc.
# Add `updates` function to tail the output file

# Add PBSJob class
# Write unit test module
# Write docstrings and rtd pages

import os
import re
import subprocess
from abc import ABC
from pathlib import Path
from datetime import datetime
from typing import TYPE_CHECKING, Optional
from cstar.base.system import cstar_system

if TYPE_CHECKING:
    from cstar.base.scheduler import SlurmScheduler


class SchedulerJob(ABC):
    pass


class SlurmJob:
    def __init__(
        self,
        scheduler: "SlurmScheduler",
        commands: str,
        cpus: int,
        account_key: str,
        script_path: Optional[str | Path] = None,
        run_path: Optional[str | Path] = None,
        job_name: Optional[str] = None,
        output_file: Optional[str | Path] = None,
        queue_name: Optional[str] = None,
        send_email: Optional[bool] = True,
        walltime: Optional[str] = None,
    ):
        self.scheduler = scheduler
        self.commands = commands
        self.cpus = cpus

        default_name = (
            f"slurm_job_{datetime.strftime(datetime.now(),format='%Y%m%d_%H%M%S')}"
        )

        self.script_path = (
            Path.cwd() / f"{default_name}.sh"
            if script_path is None
            else Path(script_path)
        )
        self.run_path = self.script_path.parent if run_path is None else Path(run_path)
        self.job_name = default_name if job_name is None else job_name
        self.output_file = (
            Path.cwd() / f"{default_name}.out" if output_file is None else output_file
        )
        self.queue_name = (
            scheduler.primary_queue_name if queue_name is None else queue_name
        )
        self.queue = scheduler.get_queue(queue_name)

        if walltime > self.queue.max_walltime:
            raise ValueError(
                f"Selected walltime {walltime} exceeds maximum "
                + f"walltime for selected queue {queue_name}: "
                + f"{self.queue.max_walltime}"
            )
        else:
            self.walltime = walltime

        if cpus > (scheduler.global_max_cpus_per_node * self.queue.max_nodes):
            raise ValueError(
                f"Selected number of CPUs: {cpus} unsupported "
                + f"for selected queue {queue_name} with max nodes "
                + f"{self.queue.max_nodes} on system with global "
                + f"max CPUs per node {scheduler.global_max_cpus_per_node}"
            )
        else:
            self.cpus = cpus

        self.account_key = account_key

        self._id = None

    @property
    def id(self):
        if self._id is None:
            print("No Job ID found. Submit this job with SlurmJob.submit()")
        return self._id

    @property
    def status(self):
        if self.id is None:
            return "unsubmitted"
        else:
            return "undetermined"
        # TODO

    @property
    def script(self):
        scheduler_script = "#!/bin/bash"
        scheduler_script += f"\n#SBATCH --job-name={self.job_name}"
        scheduler_script += f"\n#SBATCH --output={self.job_name}.out"
        scheduler_script += f"\n#SBATCH --{self.scheduler.queue_flag}={self.queue_name}"
        scheduler_script += f"\n#SBATCH --ntasks={self.cpus}"
        scheduler_script += f"\n#SBATCH --account={self.account_key}"
        scheduler_script += "\n#SBATCH --export=NONE"
        scheduler_script += "\n#SBATCH --mail-type=ALL"
        scheduler_script += f"\n#SBATCH --time={self.walltime}"
        for (
            key,
            value,
        ) in self.scheduler.other_scheduler_directives.items():
            scheduler_script += f"\n#SBATCH {key} {value}"
            # Add linux environment modules to scheduler script
        if cstar_system.environment.uses_lmod:
            scheduler_script += "\nmodule reset"
            with open(
                f"{cstar_system.environment.package_root}/additional_files/lmod_lists/{cstar_system.name}.lmod"
            ) as F:
                modules = F.readlines()
        for m in modules:
            scheduler_script += f"\nmodule load {m}"

        scheduler_script += "\nprintenv"

        # Add environment variables to scheduler script:
        for (
            var,
            value,
        ) in cstar_system.environment.environment_variables.items():
            scheduler_script += f'\nexport {var}="{value}"'

        # Add roms command to scheduler script
        scheduler_script += f"\n\n{self.commands}"
        return scheduler_script

    def save_script(self):
        with open(self.script_path, "w") as f:
            f.write(self.script)

    def submit(self):
        self.save_script()
        # remove any slurm variables in case submitting from inside another slurm job
        slurm_env = {k: v for k, v in os.environ.items() if not k.startswith("SLURM_")}

        result = subprocess.run(
            f"sbatch {self.script_path}",
            shell=True,
            cwd=self.run_path,
            env=slurm_env,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                "Non-zero exit code when submitting job. STDERR: "
                + f"\n{result.stderr}"
            )

        # Extract the job ID from the output
        matches = re.search(r"Submitted batch job (\d+)", result.stdout)
        if matches:
            self._id = int(matches.group(1))
            return self._id
        else:
            raise RuntimeError(
                f"Failed to parse job ID from sbatch output: {result.stdout}"
            )

    def cancel(self):
        result = subprocess.run(
            f"scancel {self.id}",
            shell=True,
            cwd=self.run_path,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                "Non-zero exit code when cancelling job. STDERR: "
                + f"\n{result.stderr}"
            )
        else:
            print(f"Job {self.id} cancelled")
