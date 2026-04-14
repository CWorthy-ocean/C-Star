import argparse
import os
import typing as t
from datetime import datetime, timezone

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.base.exceptions import CstarError
from cstar.base.log import LogLevelChoices, get_logger
from cstar.entrypoint.config import (
    JOBFILE_DATE_FORMAT,
    JobConfig,
    ServiceConfiguration,
    configure_environment,
)
from cstar.entrypoint.service import Service
from cstar.execution.file_system import local_copy
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.serialization import SerializableModel, deserialize


class XBlueprint(SerializableModel, t.Protocol):
    """Minimal API required for blueprint registration.

    TODO: Can be (should be) replaced with `cstar.orchestration.models.Blueprint`?
    """

    name: str
    """A user-friendly name."""
    application: str
    """The process type to be executed by the blueprint."""


TBlueprint = t.TypeVar("TBlueprint", bound=XBlueprint, covariant=True)


class XRunnerRequest(t.Generic[TBlueprint]):
    """Generic request containing configuration required to execute an application
    via Blueprint.
    """

    name: str | None = None
    """User-friendly name for the process (or job) handling the request."""
    blueprint_uri: str
    """The URI of a blueprint to be used to parameterize the application."""
    bp_type: type[TBlueprint]
    """The type of blueprint that the URI will be deserialized into."""
    _bp: TBlueprint | None = None
    """The deserialized blueprint."""

    def __init__(self, uri: str, bp_type: type[TBlueprint], name: str = ""):
        """Initialize the request instance.

        Parameters
        ----------
        name : str
            User-friendly name for the process (or job) handling the request.
        path : Path
            The URI of a blueprint to be used to parameterize the application.
        bp_type : type[TBlueprint]
            The type of blueprint that the path will be deserialized into.
        """
        self.blueprint_uri = uri
        self.bp_type = bp_type
        self.name = name.strip() or XRunnerRequest._generate_job_name()

    @property
    def application(self) -> str:
        """Return the string identifying the application that will be executed.

        Returns
        -------
        str
        """
        return self.blueprint.application

    @property
    def blueprint(self) -> TBlueprint:
        """Return the deserialized blueprint instance.

        Returns
        -------
        TBlueprint
        """
        if self._bp is None:
            with local_copy(str(self.blueprint_uri)) as local_path:
                self._bp = deserialize(local_path, self.bp_type)
        return self._bp

    @classmethod
    def _generate_job_name(cls) -> str:
        """Generate a unique job name based on the current date and time.

        Returns
        -------
        str
        """
        now_utc = datetime.now(timezone.utc)
        formatted_now_utc = now_utc.strftime(JOBFILE_DATE_FORMAT)
        return f"cstar_worker_{formatted_now_utc}"


class XRunnerResult(t.Generic[TBlueprint]):
    """Specifies details about the result of running an application."""

    request: XRunnerRequest[TBlueprint]
    """The request that was handled and produced the result."""
    status: t.Final[ExecutionStatus | None]
    """The final status of the application."""
    _errors: t.Final[list[str]]
    """The error messages produced by the application."""

    def __init__(
        self,
        request: XRunnerRequest[TBlueprint],
        status: (
            ExecutionStatus | None
        ) = None,  # TODO: review this... it should be able to be non-nullable.
        errors: list[str] | None = None,
    ):
        """Initialize the result instance.

        Parameters
        ----------
        request : XRunnerRequest[TBlueprint]
            The request that was handled and produced the result.
        status : ExecutionStatus | None
            The final status of the application.
        errors : list[str] | None
            The error messages produced by the appplication.
        """
        self.request = request
        self._errors = errors or []
        self.status = status

    @property
    def errors(self) -> t.Iterable[str]:
        return self._errors


class XRunner(t.Generic[TBlueprint], t.Protocol):
    """Core API required to be a hosted BlueprintRunner."""

    def __init__(
        self,
        request: XRunnerRequest[TBlueprint],
        job_cfg: JobConfig,
        service_config: ServiceConfiguration,
    ) -> None:
        """Initialize a runner instance.

        Parameters
        ----------
        request : XRunnerRequest[TBlueprint]
            The request containing configuration for executing an application.
        job_cfg : JobConfig
            Configuration required to submit jobs on an HPC.
        service_config : ServiceConfiguration
            Configuration options for the execution of an application in a service.
        """

    @property
    def blueprint(self) -> TBlueprint:
        """Return the deserialized blueprint instance.

        Returns
        -------
        TBlueprint
        """
        ...

    @property
    def request(self) -> XRunnerRequest[TBlueprint]:
        """Return the request containing configuration for executing the application.

        Returns
        -------
        XRunnerRequest[TBlueprint]
        """
        ...

    @property
    def result(self) -> XRunnerResult[TBlueprint] | None:
        """Return the result produced by executing the application.

        Returns
        -------
        XRunnerResult[TBlueprint]
        """
        ...

    def __call__(self) -> XRunnerResult[TBlueprint]:
        """Execute the application.

        Returns
        -------
        XRunnerResult[TBlueprint]
        """
        ...

    def set_result(
        self, status: ExecutionStatus, errors: list[str] | None = None
    ) -> XRunnerResult[TBlueprint]:
        """Set the status and resulting errors after executing the application.

        Returns
        -------
        XRunnerResult[TBlueprint]
        """
        ...


class XBlueprintRunner(XRunner[TBlueprint], Service):
    """A service that executes a blueprint."""

    _request: XRunnerRequest[TBlueprint]
    """The instigating request."""
    _result: XRunnerResult[TBlueprint] | None = None
    """The result produced by the application."""
    _job_cfg: JobConfig
    """Configuration required to submit jobs on an HPC."""

    def __init__(
        self,
        request: XRunnerRequest[TBlueprint],
        job_cfg: JobConfig,
        service_cfg: ServiceConfiguration,
    ) -> None:
        """Initialize the `XBlueprintRunner` with the supplied configuration.

        Parameters
        ----------
        request: BlueprintRequest
            A request containing information about the blueprint to run.
        job_cfg: JobConfig
            Configuration for submitting jobs to an HPC, such as account ID,
            walltime, job name, and priority.
        service_cfg: ServiceConfiguration
            Configuration for modifying behavior of the service process.
        """
        Service.__init__(self, service_cfg)
        self._request = request
        self._job_cfg = job_cfg

    @property
    def request(self) -> XRunnerRequest[TBlueprint]:
        return self._request

    @property
    def result(self) -> XRunnerResult[TBlueprint] | None:
        return self._result

    @property
    def blueprint(self) -> TBlueprint:
        return self._request.blueprint

    @property
    def blueprint_uri(self) -> str:
        """Return the URI of the blueprint to run."""
        return self._request.blueprint_uri

    @property
    def status(self) -> ExecutionStatus:
        """Return the status of the runner's work."""
        if self._result and self._result.status:
            return self._result.status

        return ExecutionStatus.UNKNOWN

    def __call__(self) -> XRunnerResult[TBlueprint]:
        return XRunnerResult(self.request, ExecutionStatus.COMPLETED)

    def is_done(self) -> bool:
        """Return `True` if the blueprint has completed execution.

        Returns
        -------
        bool
        """
        return ExecutionStatus.is_terminal(self.status)

    def _log_disposition(self) -> None:
        """Log the status of the runner's work at the current time."""
        msg = (
            "Final disposition of task executed by service "
            f"{self._config.name!r} is {self.status.name!r}"
        )
        self.log.debug(msg)

    @t.override
    def _can_shutdown(self) -> bool:
        """Determine if the service can shutdown.

        Returns
        -------
        bool
            `True` if the service can shutdown, `False` otherwise.
        """
        if self.is_done():
            self.log.info(f"Shutdown is allowed ({self.status}).")
            return True

        return False

    @t.override
    def _on_start(self) -> None:
        """Prepare the runner for execution.

        Performs validation of arguments received via CLI.
        """
        if not self.blueprint_uri:
            msg = "No blueprint URI provided"
            raise ValueError(msg)

    @t.override
    async def _on_iteration(self) -> None:
        """Execute an application as specified in the blueprint."""
        try:
            self._result = self()
        except Exception:
            msg = f"An error occurred while running blueprint: {self.blueprint_uri}"
            self.log.exception(msg)
            self.set_result(ExecutionStatus.FAILED, [msg])

    @t.override
    def _on_shutdown(self) -> None:
        """Perform actions required when shutting down the service.

        By default, a `BlueprintRunner` logs the work disposition before shutdown.
        """
        self._log_disposition()

    def set_result(
        self,
        status: ExecutionStatus,
        errors: list[str] | None = None,
    ) -> XRunnerResult[TBlueprint]:
        """Create a RunnerResult instance and store the value.

        Uses

        Returns
        -------
        RunnerResult
        """
        if self._result is None:
            self._result = XRunnerResult(self._request, status, errors)
        return self._result

    async def execute_xrunner(self) -> XRunnerResult[TBlueprint]:
        """Execute a blueprint with a BlueprintRunner.

        Parameters
        ----------
        klass : type[BlueprintRunner]
            The BlueprintRunner to use for execution.
        """
        log = get_logger(__name__, level=self._config.log_level)

        log.debug(f"Job config: {self._job_cfg!r}")
        log.debug(f"Service config: {self._config!r}")
        log.debug(f"Request: {self.request!r}")
        log.trace(f"Environment: {os.environ}")

        try:
            configure_environment(log)

            return self()
            # return bp_runner.set_result(ExecutionStatus.COMPLETED)

        except CstarError as ex:
            msg = "An error occurred while processing the blueprint"
            log.exception(msg, exc_info=ex)
            return self.set_result(ExecutionStatus.FAILED, [msg, str(ex)])
        except Exception as ex:
            msg = "An unexpected exception occurred while processing the blueprint"
            log.exception(msg, exc_info=ex)
            return self.set_result(ExecutionStatus.FAILED, [msg, str(ex)])


def create_parser(desc: str) -> argparse.ArgumentParser:
    """Create a parser for shared CLI arguments for any worker running a blueprint.

    Returns
    -------
    argparse.ArgumentParser
        An argument parser configured with the standard arguments for the
        BlueprintRunner service.
    """
    parser = argparse.ArgumentParser(
        description=desc,
        exit_on_error=True,
    )
    parser.add_argument(
        "-b",
        "--blueprint-uri",
        type=str,
        required=True,
        help="The URI of the blueprint to execute.",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default=get_env_item(ENV_CSTAR_LOG_LEVEL).value,
        type=str,
        required=False,
        help="Set the logging level for the worker.",
        choices=[x.value for x in LogLevelChoices],
    )
    return parser
