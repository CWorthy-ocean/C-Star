import argparse
import typing as t

from cstar.applications.core import (
    HasApplication,
    XRunner,
    XRunnerRequest,
    XRunnerResult,
)
from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.base.log import LogLevelChoices
from cstar.entrypoint.service import Service
from cstar.entrypoint.utils import (
    ARG_DIRECTIVES_URI_LONG,
    ARG_DIRECTIVES_URI_SHORT,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
    ARG_URI_LONG,
    ARG_URI_SHORT,
)
from cstar.execution.handler import ExecutionStatus

if t.TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


TBlueprint = t.TypeVar("TBlueprint", bound=HasApplication)


class XBlueprintRunner(Service, XRunner[TBlueprint]):
    """A service that executes a blueprint."""

    _request: XRunnerRequest[TBlueprint]
    """The instigating request."""
    _result: XRunnerResult[TBlueprint] | None = None
    """The result produced by the application."""
    _job_cfg: "JobConfig"
    """Configuration required to submit jobs on an HPC."""

    def __init__(
        self,
        request: XRunnerRequest[TBlueprint],
        service_cfg: "ServiceConfiguration",
        job_cfg: "JobConfig",
    ) -> None:
        """Initialize the `XBlueprintRunner` with the supplied configuration.

        Parameters
        ----------
        request: XRunnerRequest[TBlueprint]
            A request containing information about the blueprint to run.
        service_cfg: ServiceConfiguration
            Configuration for modifying behavior of the service process.
        job_cfg: JobConfig
            Configuration for submitting jobs to an HPC, such as account ID,
            walltime, job name, and priority.
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

    async def run(self) -> XRunnerResult[TBlueprint]:
        return XRunnerResult(self.request, ExecutionStatus.COMPLETED)

    def _log_disposition(self, treat_as_failure: bool = False) -> None:
        """Log the status of the simulation at shutdown time."""
        disposition = self.status

        if disposition == ExecutionStatus.COMPLETED and not treat_as_failure:
            self.log.info("Simulation completed successfully.")
        elif disposition == ExecutionStatus.FAILED or treat_as_failure:
            self.log.error("Simulation failed.")
        else:
            self.log.warning(f"Simulation ended with status: {disposition}.")

    @t.override
    def _can_shutdown(self) -> bool:
        """Determine if the service can shutdown.

        Returns
        -------
        bool
            `True` if the service can shutdown, `False` otherwise.
        """
        if ExecutionStatus.is_terminal(self.status):
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

        if self.blueprint.application != self.application:
            msg = (
                f"Expected blueprint for application {self.application!r} "
                f"but received {self.blueprint.application!r}"
            )
            raise ValueError(msg)

    @t.override
    async def _on_iteration(self) -> None:
        """Execute an application as specified in the blueprint."""
        try:
            self._result = await self.run()
        except Exception as ex:
            msg = f"An error occurred while running blueprint: {self.blueprint_uri!r}"
            self.log.exception(msg)
            self.set_state(ExecutionStatus.FAILED, [msg, str(ex)])

    @t.override
    def _on_shutdown(self) -> None:
        """Perform actions required when shutting down the service.

        By default, a `BlueprintRunner` logs the work disposition before shutdown.
        """
        self._log_disposition()

    def set_state(
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
        if errors and self._result and self._result.errors:
            errors.extend(self._result.errors)
            self._result = XRunnerResult(self._request, status, errors)
        return self._result


def create_parser() -> argparse.ArgumentParser:
    """Create a parser for CLI arguments expected by a blueprint runner.

    Returns
    -------
    argparse.ArgumentParser
        An argument parser configured with the expected arguments for the
        blueprint runner service.
    """
    parser = argparse.ArgumentParser(
        description="Run a c-star simulation.",
        exit_on_error=True,
    )
    parser.add_argument(
        ARG_URI_SHORT,
        ARG_URI_LONG,
        type=str,
        required=True,
        help="The URI of a blueprint.",
    )
    parser.add_argument(
        ARG_LOGLEVEL_SHORT,
        ARG_LOGLEVEL_LONG,
        default=get_env_item(ENV_CSTAR_LOG_LEVEL).value,
        type=str,
        required=False,
        help="Logging level for the simulation.",
        choices=list(LogLevelChoices),
    )
    parser.add_argument(
        ARG_DIRECTIVES_URI_LONG,
        ARG_DIRECTIVES_URI_SHORT,
        type=str,
        required=False,
        help="The URI of a file containing directive configuration.",
    )
    return parser
