import argparse
import typing as t

from cstar.applications.core import (
    HasApplication,
    RunnerRequest,
    RunnerResult,
    RunnerState,
    XRunner,
)
from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.base.log import LogLevelChoices
from cstar.entrypoint.service import Service
from cstar.entrypoint.utils import (
    ARG_DIRECTIVES_URI_LONG,
    ARG_DIRECTIVES_URI_SHORT,
    ARG_LOGLEVEL_HELP,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
    ARG_URI_LONG,
    ARG_URI_SHORT,
)
from cstar.execution.handler import ExecutionStatus

if t.TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


TBlueprint = t.TypeVar("TBlueprint", bound=HasApplication)


class BlueprintRunner(Service, XRunner[TBlueprint]):
    """A service that executes a blueprint."""

    _request: RunnerRequest[TBlueprint]
    """The instigating request."""
    _result: RunnerResult[TBlueprint]
    """The result produced by the application."""
    _job_cfg: "JobConfig"
    """Configuration required to submit jobs on an HPC."""

    def __init__(
        self,
        request: RunnerRequest[TBlueprint],
        service_cfg: "ServiceConfiguration",
        job_cfg: "JobConfig",
    ) -> None:
        """Initialize the instance with the supplied configuration.

        Parameters
        ----------
        request: RunnerRequest[TBlueprint]
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
        self._result = RunnerResult(
            request,
            RunnerState(ExecutionStatus.UNSUBMITTED),
        )

    @property
    def request(self) -> RunnerRequest[TBlueprint]:
        return self._request

    @property
    def state(self) -> RunnerState:
        return self._result.states[-1]

    @property
    def result(self) -> RunnerResult[TBlueprint]:
        return self._result

    @property
    def blueprint(self) -> TBlueprint:
        return self._request.blueprint

    @property
    def blueprint_uri(self) -> str:
        """Return the URI of the blueprint to run."""
        return self._request.blueprint_uri

    def _log_disposition(self) -> None:
        """Log the status of the simulation at shutdown time."""
        # log appropriate disposition if the result indicates errors occurred
        treat_as_failure = bool(self.result.errors)

        if self.state.status == ExecutionStatus.COMPLETED and not treat_as_failure:
            self.log.info("Simulation completed successfully.")
        elif self.state.status == ExecutionStatus.FAILED or treat_as_failure:
            err_clause = ""
            if self.result.errors:
                err_clause = f" Errors: {', '.join(self.result.errors)}"
            msg = f"Simulation failed.{err_clause}"
            self.log.error(msg)  # raise SimulationError(err_clause)
        else:
            msg = f"Simulation ended with status: {self.state.status}."
            self.log.warning(msg)

    @t.override
    def _can_shutdown(self) -> bool:
        """Determine if the service can shutdown.

        Returns
        -------
        bool
            `True` if the service can shutdown, `False` otherwise.
        """
        if ExecutionStatus.is_terminal(self.state.status) or self.state.errors:
            msg = f"BlueprintRunner reached a terminal state {str(self.state.status)!r}"
            self.log.info(msg)
            return True

        return False

    @t.override
    def _on_start(self) -> None:
        """Prepare the runner for execution.

        Performs validation of arguments received via CLI.
        """
        super()._on_start()
        if not self.blueprint_uri:
            msg = "No blueprint URI provided"
            raise ValueError(msg)

    @t.override
    async def _on_iteration(self) -> None:
        """Execute an application as specified in the blueprint."""
        try:
            self._result = await self.run()
        except Exception as ex:
            msg = f"An error occurred while running blueprint: {self.blueprint_uri!r}"
            self.log.exception(msg)
            self.add_state(ExecutionStatus.FAILED, [msg, str(ex)])

    @t.override
    def _on_shutdown(self) -> None:
        """Perform actions required when shutting down the service.

        By default, a `BlueprintRunner` logs the work disposition before shutdown.
        """
        self._log_disposition()

    @t.override
    def _cancel(self) -> None:
        """Handle a request to cancel the service."""
        self.add_state(
            ExecutionStatus.CANCELLED,
            ["The service was terminated on demand"],
        )
        super()._cancel()

    def add_state(
        self,
        status: ExecutionStatus,
        errors: str | list[str] | None = None,
    ) -> RunnerState:
        """Create a RunnerResult instance and store the value.

        Uses

        Returns
        -------
        RunnerResult
        """
        if ExecutionStatus.is_terminal(status):
            self._running = False

        if isinstance(errors, str) and errors:
            errors = [errors]

        self.result.add_state(RunnerState(status, errors or []))
        return self.result.state


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
        help=ARG_LOGLEVEL_HELP,
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
