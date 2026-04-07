import argparse
import dataclasses as dc
import logging
import os
from abc import abstractmethod
from datetime import datetime, timezone
from typing import ClassVar, Final, Protocol, override

from pydantic import BaseModel, ConfigDict

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.base.exceptions import BlueprintError, CstarError
from cstar.base.log import get_logger, parse_log_level_name
from cstar.entrypoint.service import Service, ServiceConfiguration
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)

DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
WORKER_LOG_FILE_TPL: Final[str] = "cstar-worker.{0}.log"
JOBFILE_DATE_FORMAT: Final[str] = "%Y%m%d_%H%M%S"
LOGS_DIRECTORY: Final[str] = "logs"


def _generate_job_name() -> str:
    """Generate a unique job name based on the current date and time."""
    now_utc = datetime.now(timezone.utc)
    formatted_now_utc = now_utc.strftime(JOBFILE_DATE_FORMAT)
    return f"cstar_worker_{formatted_now_utc}"


class WorkRequest(Protocol):
    """Core API of a request to run a blueprint via worker."""

    @property
    def blueprint_uri(self) -> str:
        """The location of the blueprint."""
        ...


class BlueprintRequest(BaseModel):
    """A request to run a blueprint."""

    blueprint_uri: str
    """The location of the blueprint."""

    model_config: ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)


@dc.dataclass
class JobConfig:
    """Configuration required to submit HPC jobs."""

    account_id: str
    """HPC account used for billing."""
    walltime: str
    """Maximum walltime allowed for job."""
    priority: str
    """Job priority."""
    job_name: str = _generate_job_name()
    """User-friendly job name."""


@dc.dataclass
class RunnerResult:
    request: WorkRequest | None = None
    """The request that triggered the blueprint."""
    status: ExecutionStatus = ExecutionStatus.UNKNOWN
    """The final disposition of the runner's work."""
    errors: list[str] = dc.field(default_factory=list)


class BlueprintRunner(Service):
    """A service that executes a blueprint."""

    _disposition: ExecutionStatus = ExecutionStatus.UNKNOWN
    """The status of the work being executed by the runner."""

    _request: WorkRequest
    """The instigating request."""

    _blueprint: Blueprint | None = None
    """The Blueprint deserialized from the user-supplied URI."""

    _result: RunnerResult | None = None

    def __init__(
        self,
        request: WorkRequest,
        service_cfg: ServiceConfiguration,
        job_cfg: JobConfig,
    ) -> None:
        """Initialize the `BlueprintRunner` with the supplied configuration.

        Parameters
        ----------
        request: BlueprintRequest
            A request containing information about the simulation to run

        service_cfg: ServiceConfiguration
            Configuration for modifying behavior of the service process.

        job_cfg: JobConfig
            Configuration for submitting jobs to an HPC, such as account ID,
            walltime, job name, and priority.
        """
        super().__init__(service_cfg)

        self._job_config = job_cfg
        self._request = request

    @property
    def blueprint_uri(self) -> str:
        """Return the URI of the blueprint to run."""
        return self._request.blueprint_uri.strip()

    @property
    def blueprint(self) -> Blueprint:
        """Return the deserialized blueprint to be executed by the runner.

        Raises
        ------
        ValueError
            If the blueprint cannot be deserialized to the expected type.

        Returns
        -------
        Blueprint
        """
        if self._blueprint is None:
            self._blueprint = deserialize(
                self.blueprint_uri,
                self._blueprint_type(),
            )

        return self._blueprint

    @property
    def status(self) -> ExecutionStatus:
        """Return the status of the runner's work."""
        return self._result.status if self._result else ExecutionStatus.UNKNOWN

    def is_done(self) -> bool:
        """Return `True` if the blueprint has completed execution.

        Returns
        -------
        bool
        """
        return self.status in [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.FAILED,
        ]

    def _log_disposition(self) -> None:
        """Log the status of the runner's work at the current time."""
        msg = f"Disposition of service `{self._config.name}` is: {self.status}"
        self.log.debug(msg)

    @override
    def _can_shutdown(self) -> bool:
        """Determine if the service can shutdown.

        Returns
        -------
        bool
            `True` if the service can shutdown, `False` otherwise.
        """
        if self.is_done():
            self.log.info(f"Shutdown is allowed ({self._disposition}).")
            return True

        return False

    @override
    def _on_start(self) -> None:
        """Prepare the runner for execution.

        Performs validation of arguments received via CLI.
        """
        if not self.blueprint_uri:
            msg = "No blueprint URI provided"
            raise BlueprintError(msg)

        self._blueprint = deserialize(self.blueprint_uri, self._blueprint_type())

    @override
    async def _on_iteration(self) -> None:
        """Execute the blueprint processing implemented in the subclass."""
        try:
            self._result = self._run_blueprint(self.blueprint)
        except Exception:
            msg = f"An error occurred while running blueprint: {self.blueprint_uri}"
            self.log.exception(msg)
            self.set_result(ExecutionStatus.FAILED, [msg])

    @override
    def _on_shutdown(self) -> None:
        """Perform actions required when shutting down the service.

        By default, the `BlueprintRunner` logs the work disposition before shutdown.
        """
        self._log_disposition()

    @abstractmethod
    def _run_blueprint(self, blueprint: Blueprint) -> RunnerResult:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        ...

    @abstractmethod
    def _blueprint_type(self) -> type[Blueprint]:
        """Return the type of Blueprint to be deserialized.

        Returns
        -------
        type[Blueprint]
        """
        ...

    def set_result(
        self,
        status: ExecutionStatus,
        errors: list[str] | None = None,
    ) -> RunnerResult:
        """Create a RunnerResult instance and store the value.

        Uses

        Returns
        -------
        RunnerResult
        """
        if self._result is None:
            self._result = RunnerResult(self._request, status, errors or [])
        return self._result


def create_parser() -> argparse.ArgumentParser:
    """Create a parser for shared CLI arguments for any worker running a blueprint.

    Returns
    -------
    argparse.ArgumentParser
        An argument parser configured with the expected arguments for the
        SimulationRunner service.
    """
    parser = argparse.ArgumentParser(
        description="Execute a blueprint.",
        exit_on_error=True,
    )
    parser.add_argument(
        "-b",
        "--blueprint-uri",
        type=str,
        required=True,
        help="The URI of a blueprint.",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default=get_env_item(ENV_CSTAR_LOG_LEVEL).value,
        type=str,
        required=False,
        help="Logging level for the simulation.",
        choices=[
            logging.getLevelName(i)
            for i in [
                logging.DEBUG,
                logging.INFO,
                logging.WARNING,
                logging.ERROR,
            ]
        ],
    )
    return parser


def get_service_config(log_level: int | str) -> ServiceConfiguration:
    """Create a ServiceConfiguration instance using CLI arguments.

    Parameters
    ----------
    log_level : int or str
        The log level to be used by the worker

    Returns
    -------
    ServiceConfiguration
    """
    return ServiceConfiguration(
        as_service=False,
        loop_delay=1,
        health_check_frequency=None,
        log_level=parse_log_level_name(log_level),
        health_check_log_threshold=10,
        name="BlueprintRunner",
    )


def get_request(blueprint_uri: str) -> BlueprintRequest:
    """Create a BlueprintRequest instance from CLI arguments.

    Parameters
    ----------
    blueprint_uri : str
        The path to a blueprint file.

    Returns
    -------
    BlueprintRequest
        A request configured to run a c-star simulation via a blueprint.
    """
    return BlueprintRequest(blueprint_uri=blueprint_uri)


def get_job_config() -> JobConfig:
    """Create and configure a `JobConfig` instance from environment variables.

    Returns
    -------
    JobConfig
    """
    account_id: str = get_env_item(ENV_CSTAR_SLURM_ACCOUNT).value
    walltime: str = get_env_item(ENV_CSTAR_SLURM_MAX_WALLTIME).value
    priority: str = get_env_item(ENV_CSTAR_SLURM_QUEUE).value

    return JobConfig(account_id, walltime, priority)


def configure_environment(log: logging.Logger) -> None:
    """Configure the environment variables required by the worker.

    Parameters
    ----------
    log : logging.Logger
        A logger to log configuration details.
    """
    # ensure git works on distributed file-system, e.g. lustre
    os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] = "1"
    log.debug("Git discovery configured.")


def create_runner(
    request: BlueprintRequest,
    klass: type[BlueprintRunner],
    service_cfg: ServiceConfiguration | None = None,
    job_cfg: JobConfig | None = None,
    log_level: int | str = logging.INFO,
):
    if job_cfg is None:
        job_cfg = get_job_config()
    if service_cfg is None:
        service_cfg = get_service_config(log_level)

    return klass(request, service_cfg, job_cfg)


async def execute_runner(klass: type[BlueprintRunner]) -> RunnerResult:
    """Execute a blueprint with a SimulationRunner.

    Parameters
    ----------
    job_cfg : JobConfig
        Configuration applied to the scheduler
    service_cfg : ServiceConfiguration
        Configuration applied to the service
    request : BlueprintRequest
        A request specifying the blueprint to be executed
    """
    try:
        parser = create_parser()
        args = parser.parse_args()
    except SystemExit as ex:
        msg = "Parsing CLI arguments failed."
        return RunnerResult(None, ExecutionStatus.FAILED, [msg, str(ex)])

    job_cfg = get_job_config()
    service_cfg = get_service_config(args.log_level)
    request = get_request(args.blueprint_uri)

    log = get_logger(__name__, level=service_cfg.log_level)

    bp_runner = create_runner(
        request,
        klass,
        service_cfg,
        job_cfg,
    )

    log.debug(f"Job config: {job_cfg}")
    log.debug(f"Service config: {service_cfg}")
    log.debug(f"Request: {request}")
    log.debug(f"os.environ: {os.environ}")

    try:
        configure_environment(log)

        await bp_runner.execute()
        return bp_runner.set_result(ExecutionStatus.COMPLETED)

    except CstarError as ex:
        msg = "An error occurred while processing the blueprint"
        log.exception(msg, exc_info=ex)
        return bp_runner.set_result(ExecutionStatus.FAILED, [msg, str(ex)])
    except Exception as ex:
        msg = "An unexpected exception occurred during the simulation"
        log.exception(msg, exc_info=ex)
        return bp_runner.set_result(ExecutionStatus.FAILED, [msg, str(ex)])
