import os
import re
import time
import json
import warnings
import subprocess
from math import ceil
from enum import Enum, auto
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple
from cstar.system.manager import cstar_sysmgr
from cstar.system.scheduler import (
    SlurmScheduler,
    PBSScheduler,
    Scheduler,
    SlurmQOS,
    SlurmPartition,
)


def create_scheduler_job(
    commands: str,
    account_key: str,
    cpus: int,
    nodes: Optional[int] = None,
    cpus_per_node: Optional[int] = None,
    script_path: Optional[str | Path] = None,
    run_path: Optional[str | Path] = None,
    job_name: Optional[str] = None,
    output_file: Optional[str | Path] = None,
    queue_name: Optional[str] = None,
    send_email: Optional[bool] = True,
    walltime: Optional[str] = None,
) -> "SchedulerJob":
    """Create a scheduler job for either SLURM or PBS based on the system's active
    scheduler.

    Parameters
    ----------
    commands : str
        The commands to execute within the job script.
    account_key : str
        The account key to associate with the job for resource tracking.
    cpus : int
        The total number of CPUs required for the job.
    nodes : int, optional
        The number of nodes to request. Defaults to None.
        If not provided and a specific nodes x cpus distribution is required,
        C-Star will attempt to calculate an appropriate number of nodes.
    cpus_per_node : int, optional
        The number of CPUs per node to request. Defaults to None.
        If not provided and a specific nodes x cpus distribution is required,
        C-Star will attempt to calculate an appropriate number of nodes.
    script_path : str or Path, optional
        The file path to save the job script. Defaults to the current directory with
        an auto-generated name.
    run_path : str or Path, optional
        The directory to execute the job. Defaults to the directory containing the job script.
    job_name : str, optional
        The name of the job. Defaults to an auto-generated name.
    output_file : str or Path, optional
        The file path for job output. Defaults to an auto-generated filename in the run path.
    queue_name : str, optional
        The name of the queue to submit the job to. Defaults to the scheduler's primary queue.
    send_email : bool, optional
        Whether to send email notifications about job status. Defaults to True.
    walltime : str, optional
        The maximum walltime for the job, in the format "HH:MM:SS". Defaults to the queue's maximum.

    Returns
    -------
    job: SchedulerJob
        An instance of `SlurmJob` or `PBSJob`, depending on the active scheduler.

    Raises
    ------
    TypeError
        If the active scheduler is not SLURM or PBS.
    """

    # mypy assigns type based on first condition, assigning explicitly:
    job_type: type[SlurmJob] | type[PBSJob]

    if isinstance(cstar_sysmgr.scheduler, SlurmScheduler):
        job_type = SlurmJob
    elif isinstance(cstar_sysmgr.scheduler, PBSScheduler):
        job_type = PBSJob
    else:
        raise TypeError(
            f"Unsupported scheduler type: {type(cstar_sysmgr.scheduler).__name__}"
        )

    return job_type(
        scheduler=cstar_sysmgr.scheduler,
        commands=commands,
        cpus=cpus,
        nodes=nodes,
        cpus_per_node=cpus_per_node,
        account_key=account_key,
        script_path=script_path,
        run_path=run_path,
        job_name=job_name,
        output_file=output_file,
        queue_name=queue_name,
        send_email=send_email,
        walltime=walltime,
    )


class JobStatus(Enum):
    """Enum representing possible states of a job in the scheduler.

    Each state corresponds to a stage in the lifecycle of a job, from submission
    to completion or failure.

    Attributes
    ----------
    UNSUBMITTED : JobStatus
        The job has not been submitted to the scheduler yet.
    PENDING : JobStatus
        The job has been submitted but is waiting to start.
    RUNNING : JobStatus
        The job is currently executing.
    COMPLETED : JobStatus
        The job finished successfully.
    CANCELLED : JobStatus
        The job was cancelled before completion.
    FAILED : JobStatus
        The job finished unsuccessfully.
    HELD : JobStatus
        The job is on hold and will not run until released.
    ENDING : JobStatus
        The job is in the process of ending but not fully completed.
    UNKNOWN : JobStatus
        The job state is unknown or not recognized.
    """

    UNSUBMITTED = auto()
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    CANCELLED = auto()
    FAILED = auto()
    HELD = auto()
    ENDING = auto()
    UNKNOWN = auto()

    def __str__(self) -> str:
        return self.name.lower()  # Convert enum name to lowercase for display


class SchedulerJob(ABC):
    """Abstract base class for representing a job submitted to a scheduler.

    This class defines the structure and common behavior for jobs managed by
    schedulers such as SLURM and PBS. Subclasses must implement methods for
    submitting jobs and retrieving job status.

    Attributes
    ----------
    scheduler : Scheduler
        The scheduler managing this job (e.g., a SlurmScheduler or PBSScheduler instance)
    commands : str
        The commands to execute within the job script.
    account_key : str
        The account key associated with the job for resource tracking.
    cpus : int
        The total number of CPUs required for the job.
    nodes : int or None
        The number of nodes to request.
        If not provided and a specific nodes x cpus distribution is required,
        C-Star will attempt to calculate an appropriate number of nodes.
    cpus_per_node : int or None
        The number of CPUs per node to request.
        If not provided and a specific nodes x cpus distribution is required,
        C-Star will attempt to calculate an appropriate number of nodes.
    script_path : Path
        The file path where the job script will be saved.
    run_path : Path
        The directory where the job will be executed.
    job_name : str
        The name of the job.
    output_file : Path
        The file path for job output.
    queue_name : str
        The name of the queue to which the job will be submitted.
    queue : Queue
        The queue object corresponding to `queue_name`.
    walltime : str
        The maximum walltime for the job, in the format "HH:MM:SS".
    id : int or None
        The unique job ID assigned by the scheduler. None if the job has not been submitted.
    status: JobStatus
        A representation of the current status of the job, e.g. RUNNING or CANCELLED
    script: str
        The job script to be submitted to the scheduler.

    Methods
    -------
    save_script()
        Save the job script to the specified file path.
    submit()
        Abstract method for submitting the job to the scheduler.
    updates(seconds=10)
        Stream live updates from the job's output file for the specified duration.
    """

    def __init__(
        self,
        scheduler: "Scheduler",
        commands: str,
        account_key: str,
        cpus: int,
        nodes: Optional[int] = None,
        cpus_per_node: Optional[int] = None,
        script_path: Optional[str | Path] = None,
        run_path: Optional[str | Path] = None,
        job_name: Optional[str] = None,
        output_file: Optional[str | Path] = None,
        queue_name: Optional[str] = None,
        send_email: Optional[bool] = True,
        walltime: Optional[str] = None,
    ):
        """Initialize a SchedulerJob instance.

        Parameters
        ----------
        scheduler : Scheduler
            The scheduler managing this job (e.g., a SlurmScheduler or PBSScheduler instance).
        commands : str
            The commands to execute within the job script.
        account_key : str
            The account key associated with the job for resource tracking.
        cpus : int
            The total number of CPUs required for the job.
        nodes : int, optional
            The number of nodes to request. If not provided and a specific nodes x CPUs
            distribution is required, C-Star will attempt to calculate an appropriate
            number of nodes.
        cpus_per_node : int, optional
            The number of CPUs per node to request. If not provided and a specific nodes x CPUs
            distribution is required, C-Star will attempt to calculate an appropriate
            number of CPUs per node.
        script_path : str or Path, optional
            The file path to save the job script. Defaults to the current directory with
            an auto-generated name.
        run_path : str or Path, optional
            The directory where the job will be executed. Defaults to the directory containing
            the job script.
        job_name : str, optional
            The name of the job. Defaults to an auto-generated name.
        output_file : str or Path, optional
            The file path for job output. Defaults to an auto-generated file in the run path.
        queue_name : str, optional
            The name of the queue to which the job will be submitted. Defaults to the scheduler's
            primary queue.
        send_email : bool, optional
            Whether to send email notifications about job status. Defaults to True.
        walltime : str, optional
            The maximum walltime for the job, in the format "HH:MM:SS". If not provided,
            it defaults to the queue's maximum walltime.

        Raises
        ------
        ValueError
            If no walltime is provided and the queue's maximum walltime is unavailable, or
            if the provided walltime exceeds the queue's maximum allowed walltime.
        EnvironmentError
            If neither `nodes` nor `cpus_per_node` are provided and the scheduler cannot
            determine the system's CPUs per node automatically.
        """

        self.scheduler = scheduler
        self.commands = commands
        self.cpus = cpus

        default_name = f"cstar_job_{datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')}"

        self.script_path = (
            Path.cwd() / f"{default_name}.sh"
            if script_path is None
            else Path(script_path)
        )
        self.run_path = self.script_path.parent if run_path is None else Path(run_path)
        self.job_name = default_name if job_name is None else job_name
        self.output_file = (
            self.run_path / f"{default_name}.out"
            if output_file is None
            else output_file
        )
        self.queue_name = (
            scheduler.primary_queue_name if queue_name is None else queue_name
        )
        self.queue = scheduler.get_queue(queue_name)
        self.walltime = walltime

        if (walltime is None) and (self.queue.max_walltime is None):
            raise ValueError(
                "Cannot create scheduler job: walltime parameter not provided "
                + f"and C-Star cannot default to the max walltime for the queue {queue_name} "
                + " as it cannot be determined"
            )
        elif self.queue.max_walltime is None:
            warnings.warn(
                f"WARNING: Unable to determine the maximum allowed walltime for chosen queue {queue_name}. "
                + f"If your chosen walltime {walltime} exceeds the (unknown) limit, this job may be "
                + "rejected by your system's job scheduler.",
                UserWarning,
            )
        elif walltime is None:
            warnings.warn(
                "Walltime parameter unspecified. Creating scheduler job with maximum walltime "
                + f"for queue {queue_name}, {self.queue.max_walltime}"
            )
            self.walltime = self.queue.max_walltime
        else:
            # Check walltimes
            wt_h, wt_m, wt_s = map(int, walltime.split(":"))
            mw_h, mw_m, mw_s = map(int, self.queue.max_walltime.split(":"))

            walltime_delta = timedelta(hours=wt_h, minutes=wt_m, seconds=wt_s)
            max_walltime_delta = timedelta(hours=mw_h, minutes=mw_m, seconds=mw_s)

            if walltime_delta > max_walltime_delta:
                raise ValueError(
                    f"Selected walltime {walltime} exceeds maximum "
                    + f"walltime for selected queue {queue_name}: "
                    + f"{self.queue.max_walltime}"
                )

        self.cpus = cpus

        # Explicitly typing to avoid mypy confusion in conditional pathways below
        self.cpus_per_node: Optional[int]
        self.nodes: Optional[int]

        if (
            (nodes is None)
            and (cpus_per_node is not None)
            and (scheduler.requires_task_distribution)
        ):
            self.nodes = ceil(cpus / cpus_per_node)
            self.cpus_per_node = cpus_per_node
        elif (
            (nodes is not None)
            and (cpus_per_node is None)
            and (scheduler.requires_task_distribution)
        ):
            self.nodes = nodes
            self.cpus_per_node = int(cpus / nodes)
        elif (
            (nodes is None)
            and (cpus_per_node is None)
            and (scheduler.requires_task_distribution)
        ):
            if scheduler.global_max_cpus_per_node is None:
                raise EnvironmentError(
                    "You attempted to create a scheduler job without 'nodes', and "
                    + "'cpus_per_node' parameters, but your scheduler explicitly "
                    + "requires a task distribution. C-Star is unable to determine "
                    + "your system's CPUs per node automatically and cannot continue"
                )

            nnodes, ncpus = self._calculate_node_distribution(
                cpus, scheduler.global_max_cpus_per_node
            )
            warnings.warn(
                (
                    "WARNING: Attempting to create scheduler job without 'nodes' and 'cpus_per_node' "
                    + "parameters, but your system requires an explicitly specified task distribution."
                    + "\n C-Star will attempt "
                    + f"\nto use a distribution of {nnodes} nodes with {ncpus} CPUs each, "
                    + "\nbased on your system maximum of "
                    + f"{scheduler.global_max_cpus_per_node} CPUS per node "
                    + f"\nand your job requirement of {cpus} CPUS."
                ),
                UserWarning,
            )
            self.cpus_per_node = ncpus
            self.nodes = nnodes
        else:
            self.cpus_per_node = cpus_per_node
            self.nodes = nodes

        self.account_key = account_key
        self._id: Optional[int] = None

    @property
    def id(self) -> Optional[int]:
        """Retrieve the unique job ID assigned by the scheduler.

        The job ID is assigned when the job is successfully submitted to the scheduler.
        If the job has not been submitted, a message will be displayed indicating that
        the ID is not yet available.

        Returns
        -------
        id: int or None
            The unique job ID assigned by the scheduler, or `None` if the job has not
            been submitted.
        """

        if self._id is None:
            print("No Job ID found. Submit this job with SchedulerJob.submit()")
        return self._id

    @property
    @abstractmethod
    def script(self) -> str:
        """Generate the job script to be submitted to the scheduler.

        This property constructs the job script as a string, incorporating scheduler-specific
        directives (e.g., SLURM or PBS) and the commands provided during initialization. The
        script includes information such as job name, output file, walltime, and resource
        requirements.

        Returns
        -------
        script: str
            The complete job script as a string, ready for submission.
        """
        pass

    def save_script(self):
        """Save the job script to a file.

        Writes the generated job script to the file specified by the `script_path` attribute.
        The file can then be used to submit the job to the scheduler.
        """
        with open(self.script_path, "w") as f:
            f.write(self.script)

    @abstractmethod
    def submit(self):
        """Submit the job to the scheduler.

        This method is responsible for submitting the generated job script to the
        underlying scheduler (e.g., SLURM or PBS). Subclasses must implement this method
        to handle the specifics of the submission process.

        This method updates the 'id' attribute
        """
        pass

    @property
    @abstractmethod
    def status(self) -> JobStatus:
        """Retrieve the current status of the job.

        This method queries the underlying scheduler to determine the current status of
        the job (e.g., PENDING, RUNNING, COMPLETED). Subclasses must implement this method
        to interact with the specific scheduler and map its state to a `JobStatus` enum.

        Parameters
        ----------
        None

        Returns
        -------
        status: JobStatus
            An enumeration value representing the current status of the job.
        """
        pass

    def updates(self, seconds=10):
        """Provides updates from the job's output file as a live stream for `seconds`
        seconds (default 10).

        If `seconds` is 0, updates are provided indefinitely until the user interrupts the stream.
        """

        if self.status != JobStatus.RUNNING:
            print(
                f"This job is currently not running ({self.status}). Live updates cannot be provided."
            )
            if (self.status in {JobStatus.FAILED, JobStatus.COMPLETED}) or (
                self.status == JobStatus.CANCELLED and self.output_file.exists()
            ):
                print(f"See {self.output_file.resolve()} for job output")
            return

        if seconds == 0:
            # Confirm indefinite tailing
            confirmation = (
                input(
                    "This will provide indefinite updates to your job. You can stop it anytime using Ctrl+C. "
                    "Do you want to continue? (y/n): "
                )
                .strip()
                .lower()
            )
            if confirmation not in {"y", "yes"}:
                return

        try:
            with open(self.output_file, "r") as f:
                f.seek(0, 2)  # Move to the end of the file
                start_time = time.time()

                while seconds == 0 or (time.time() - start_time < seconds):
                    line = f.readline()
                    if line:
                        print(line, end="")
                    else:
                        time.sleep(0.1)  # 100ms delay between updates
        except KeyboardInterrupt:
            print("\nLive status updates stopped by user.")

    def _calculate_node_distribution(
        self, n_cores_required: int, tot_cores_per_node: int
    ) -> Tuple[int, int]:
        """Determine how many nodes and cores per node to request from a job scheduler.

        For example, if requiring 192 cores for a job on a system with 128 cores per node,
        this method advises requesting 2 nodes with 96 cores each.

        Parameters:
        -----------
        n_cores_required: int
            The number of cores required for the job
        tot_cores_per_node: int
            The number of cores per node on the target system

        Returns:
        --------
        n_nodes_to_request: int
            The number of nodes to request from the scheduler
        cores_to_request_per_node: int
            The number of cores per node to request from the scheduler
        """

        n_nodes_to_request = ceil(n_cores_required / tot_cores_per_node)
        cores_to_request_per_node = ceil(
            tot_cores_per_node
            - ((n_nodes_to_request * tot_cores_per_node) - n_cores_required)
            / n_nodes_to_request
        )

        return n_nodes_to_request, cores_to_request_per_node


class SlurmJob(SchedulerJob):
    """Represents a job submitted to the SLURM scheduler.

    This class extends `SchedulerJob` to handle SLURM-specific functionality for
    job submission, status retrieval, and script generation.

    Attributes
    ----------
    scheduler : SlurmScheduler
        The SLURM scheduler managing this job.
    commands : str
        The commands to execute within the job script.
    account_key : str
        The account key associated with the job for resource tracking.
    cpus : int
        The total number of CPUs required for the job.
    nodes : int or None
        The number of nodes to request.
        If not provided and a specific nodes x CPUs distribution is required,
        C-Star will attempt to calculate an appropriate number of nodes.
    cpus_per_node : int or None
        The number of CPUs per node to request.
        If not provided and a specific nodes x CPUs distribution is required,
        C-Star will attempt to calculate an appropriate number of CPUs per node.
    script_path : Path
        The file path where the job script will be saved.
    run_path : Path
        The directory where the job will be executed.
    job_name : str
        The name of the job.
    output_file : Path
        The file path for job output.
    queue_name : str
        The name of the SLURM queue (partition or QOS) to which the job will be submitted.
    walltime : str
        The maximum walltime for the job, in the format "HH:MM:SS".
    id : int or None
        The unique job ID assigned by the SLURM scheduler. None if the job has not been submitted.
    status : JobStatus
        The current status of the job, retrieved from SLURM.
    script : str
        The SLURM-specific job script, including directives and commands.

    Methods
    -------
    submit()
        Submit the job to the SLURM scheduler.
    cancel()
        Cancel the job using the SLURM `scancel` command.
    """

    @property
    def status(self) -> JobStatus:
        """Retrieve the current status of the job from the SLURM scheduler.

        This property queries SLURM using the `sacct` command to determine the job's
        state and maps it to a corresponding `JobStatus` enumeration.

        Returns
        -------
        status: JobStatus
            The current status of the job. Possible values include:
            - `JobStatus.PENDING`: The job has been submitted but is waiting to start.
            - `JobStatus.RUNNING`: The job is currently executing.
            - `JobStatus.COMPLETED`: The job finished successfully.
            - `JobStatus.CANCELLED`: The job was cancelled before completion.
            - `JobStatus.FAILED`: The job finished unsuccessfully.
            - `JobStatus.UNKNOWN`: The job state could not be determined.

        Raises
        ------
        RuntimeError
            If the command to retrieve the job status fails or returns an unexpected result.
        """

        if self.id is None:
            return JobStatus.UNSUBMITTED
        else:
            sacct_cmd = f"sacct -j {self.id} --format=State%20 --noheader"
            result = subprocess.run(
                sacct_cmd, capture_output=True, text=True, shell=True
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to retrieve job status using {sacct_cmd}."
                    f"STDOUT: {result.stdout}, STDERR: {result.stderr}"
                )

        # Map sacct states to JobStatus enum
        sacct_status_map = {
            "PENDING": JobStatus.PENDING,
            "RUNNING": JobStatus.RUNNING,
            "COMPLETED": JobStatus.COMPLETED,
            "CANCELLED": JobStatus.CANCELLED,
            "FAILED": JobStatus.FAILED,
        }
        for state, status in sacct_status_map.items():
            if state in result.stdout:
                return status

        # Fallback if no known state is found
        return JobStatus.UNKNOWN

    @property
    def script(self) -> str:
        """Generate the SLURM-specific job script to be submitted to the scheduler.
        Includes standard Slurm scheduler directives as well as scheduler-specific
        directives specified by the scheduler.other_scheduler_directives attribute.

        Returns
        -------
        scheduler_script: str
            The complete SLURM job script as a string, ready for submission.
        """

        scheduler_script = "#!/bin/bash"
        scheduler_script += f"\n#SBATCH --job-name={self.job_name}"
        scheduler_script += f"\n#SBATCH --output={self.output_file}"
        if isinstance(self.queue, SlurmQOS):
            scheduler_script += f"\n#SBATCH --qos={self.queue_name}"
        elif isinstance(self.queue, SlurmPartition):
            scheduler_script += f"\n#SBATCH --partition={self.queue_name}"
        if self.scheduler.requires_task_distribution:
            scheduler_script += f"\n#SBATCH --nodes={self.nodes}"
            scheduler_script += f"\n#SBATCH --ntasks-per-node={self.cpus_per_node}"
        else:
            scheduler_script += f"\n#SBATCH --ntasks={self.cpus}"
        scheduler_script += f"\n#SBATCH --account={self.account_key}"
        scheduler_script += "\n#SBATCH --export=ALL"
        scheduler_script += "\n#SBATCH --mail-type=ALL"
        scheduler_script += f"\n#SBATCH --time={self.walltime}"
        for (
            key,
            value,
        ) in self.scheduler.other_scheduler_directives.items():
            scheduler_script += f"\n#SBATCH {key} {value}"

        # Add roms command to scheduler script
        scheduler_script += f"\n\n{self.commands}"
        return scheduler_script

    def submit(self) -> Optional[int]:
        """Submit the job to the SLURM scheduler.

        This method saves the job script to the specified `script_path` and submits it
        to the SLURM scheduler using the `sbatch` command. It extracts and stores the
        job ID assigned by SLURM to the 'id' attribute.

        Returns
        -------
        job_id : int
            The unique job ID assigned by the SLURM scheduler.

        Raises
        ------
        RuntimeError
            If the `sbatch` command fails or if the job ID cannot be extracted from the
            submission output.
        """
        self.save_script()
        # remove any slurm variables in case submitting from inside another slurm job
        env_vars_to_exclude = []
        for k in os.environ.keys():
            if k.startswith("SLURM_"):
                if k not in {"SLURM_CONF", "SLURM_VERSION"}:
                    env_vars_to_exclude.append(k)

        slurm_env = {
            k: v for k, v in os.environ.items() if k not in env_vars_to_exclude
        }

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
        """Cancel the job in the SLURM scheduler.

        This method cancels the job using the SLURM `scancel` command and provides
        feedback about the cancellation process.

        It can only be used on jobs with RUNNING or PENDING status.

        Raises
        ------
        RuntimeError
            If the `scancel` command fails or returns a non-zero exit code.
        """

        if self.status not in {JobStatus.RUNNING, JobStatus.PENDING}:
            print(f"Cannot cancel job with status {self.status}")
            return

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


class PBSJob(SchedulerJob):
    """Represents a job submitted to the PBS (Portable Batch System) scheduler.

    This class extends `SchedulerJob` to handle PBS-specific functionality for
    job submission, status retrieval, and script generation.

    Attributes
    ----------
    scheduler : PBSScheduler
        The PBS scheduler managing this job.
    commands : str
        The commands to execute within the job script.
    account_key : str
        The account key associated with the job for resource tracking.
    cpus : int
        The total number of CPUs required for the job.
    nodes : int or None
        The number of nodes to request.
        If not provided and a specific nodes x CPUs distribution is required,
        C-Star will attempt to calculate an appropriate number of nodes.
    cpus_per_node : int or None
        The number of CPUs per node to request.
        If not provided and a specific nodes x CPUs distribution is required,
        C-Star will attempt to calculate an appropriate number of CPUs per node.
    script_path : Path
        The file path where the job script will be saved.
    run_path : Path
        The directory where the job will be executed.
    job_name : str
        The name of the job.
    output_file : Path
        The file path for job output.
    queue_name : str
        The name of the PBS queue to which the job will be submitted.
    walltime : str
        The maximum walltime for the job, in the format "HH:MM:SS".
    id : int or None
        The unique job ID assigned by the PBS scheduler. None if the job has not been submitted.
    status : JobStatus
        The current status of the job, retrieved from PBS.
    script : str
        The PBS-specific job script, including directives and commands.

    Methods
    -------
    submit()
        Submit the job to the PBS scheduler.
    cancel()
        Cancel the job using the PBS `qdel` command.
    """

    @property
    def script(self):
        """Generate the PBS-specific job script to be submitted to the scheduler.
        Includes standard Slurm scheduler directives as well as scheduler-specific
        directives specified by the scheduler.other_scheduler_directives attribute.

        Generate the PBS-specific job script to be submitted to the scheduler.

        Returns
        -------
        scheduler_script : str
            The complete PBS job script as a string, ready for submission.
        """

        scheduler_script = "#PBS -S /bin/bash"
        scheduler_script += f"\n#PBS -N {self.job_name}"
        scheduler_script += f"\n#PBS -o {self.output_file}"
        scheduler_script += f"\n#PBS -A {self.account_key}"
        scheduler_script += f"\n#PBS -l select={self.nodes}:ncpus={self.cpus_per_node},walltime={self.walltime}"
        scheduler_script += f"\n#PBS -q {self.queue_name}"
        scheduler_script += "\n#PBS -j oe"
        scheduler_script += "\n#PBS -k eod"
        scheduler_script += "\n#PBS -V"
        for (
            key,
            value,
        ) in cstar_sysmgr.scheduler.other_scheduler_directives.items():
            scheduler_script += f"\n#PBS {key} {value}"
        scheduler_script += "\ncd ${PBS_O_WORKDIR}"

        scheduler_script += f"\n\n{self.commands}"
        return scheduler_script

    @property
    def status(self) -> JobStatus:
        """Retrieve the current status of the job from the PBS scheduler.

        This property queries PBS using the `qstat` command to determine the job's
        state and maps it to a corresponding `JobStatus` enumeration.

        Returns
        -------
        status : JobStatus
            The current status of the job. Possible values include:
            - `JobStatus.PENDING`: The job has been submitted but is waiting to start.
            - `JobStatus.RUNNING`: The job is currently executing.
            - `JobStatus.COMPLETED`: The job finished successfully.
            - `JobStatus.CANCELLED`: The job was cancelled before completion.
            - `JobStatus.FAILED`: The job finished unsuccessfully.
            - `JobStatus.HELD`: The job is on hold.
            - `JobStatus.ENDING`: The job is in the process of ending.
            - `JobStatus.UNKNOWN`: The job state could not be determined.

        Raises
        ------
        RuntimeError
            If the `qstat` command fails or the job cannot be found in the scheduler's records.
        """

        if self.id is None:
            return JobStatus.UNSUBMITTED

        qstat_cmd = f"qstat -x -f -F json {self.id}"
        result = subprocess.run(qstat_cmd, capture_output=True, text=True, shell=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to retrieve job status using {qstat_cmd}."
                f"STDOUT: {result.stdout}, STDERR: {result.stderr}"
            )

        # Parse the JSON output
        try:
            job_data = json.loads(result.stdout)
            try:
                job_info = next(iter(job_data["Jobs"].values()))
            except StopIteration:
                raise RuntimeError(f"Job ID {self.id} not found in qstat output.")

            # Extract the job state
            job_state = job_info["job_state"]
            pbs_status_map = {
                "Q": JobStatus.PENDING,
                "R": JobStatus.RUNNING,
                "C": JobStatus.COMPLETED,
                "H": JobStatus.HELD,
                "E": JobStatus.ENDING,
            }

            # Handle specific cases for "F" (Finished)
            if job_state == "F":
                exit_status = job_info.get("Exit_status", 1)
                return JobStatus.COMPLETED if exit_status == 0 else JobStatus.FAILED
            else:
                # Default to UNKNOWN for unmapped states
                return pbs_status_map.get(job_state, JobStatus.UNKNOWN)

        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse JSON from qstat output: {e}")

    def submit(self) -> Optional[int]:
        """Submit the job to the PBS scheduler.

        This method saves the job script to the specified `script_path` and submits it
        to the PBS scheduler using the `qsub` command. It extracts and stores the job ID
        assigned by PBS.

        Returns
        -------
        job_id : int
            The unique job ID assigned by the PBS scheduler.

        Raises
        ------
        RuntimeError
            If the `qsub` command fails or if the job ID cannot be parsed from the
            submission output.
        """

        self.save_script()

        result = subprocess.run(
            f"qsub {self.script_path}",
            shell=True,
            cwd=self.run_path,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                "Non-zero exit code when submitting job. STDERR: "
                + f"\n{result.stderr}"
            )

        # Validate the format of the job ID (e.g., "<int>.<str>")
        job_id_full = result.stdout.strip()  # Full job ID (e.g., "7063621.desched1")
        if not re.match(r"^\d+\.\w+$", job_id_full):
            raise RuntimeError(f"Unexpected job ID format from qsub: {job_id_full}")

        # Extract the job ID from the output
        self._id = int(result.stdout.strip().split(".")[0])
        return self._id

    def cancel(self):
        """Cancel the job in the PBS scheduler.

        This method cancels the job using the PBS `qdel` command and provides feedback
        about the cancellation process. The job must be in a state that allows cancellation
        (i.e., PENDING or RUNNING).

        Raises
        ------
        RuntimeError
            If the `qdel` command fails or returns a non-zero exit code.
        """

        if self.status not in {JobStatus.RUNNING, JobStatus.PENDING, JobStatus.HELD}:
            print(f"Cannot cancel job with status {self.status}")
            return

        result = subprocess.run(
            f"qdel {self.id}",
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