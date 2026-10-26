import argparse
import os
import socket
import typing as t
from pathlib import Path
from urllib.parse import urlsplit

from cstar.applications.core import (
    HasApplication,
    RunnerRequest,
    RunnerResult,
    RunnerState,
    XRunner,
    get_application,
)
from cstar.base.env import ENV_CSTAR_LOG_LEVEL, ENV_CSTAR_RUNID, get_env_item
from cstar.base.log import LogLevelChoices, get_logger
from cstar.base.provenance import package_versions, system_name
from cstar.base.utils import utc_now
from cstar.entrypoint.service import Service
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_CLOBBER_HELP,
    ARG_DIRECTIVES_URI_LONG,
    ARG_DIRECTIVES_URI_SHORT,
    ARG_LOGLEVEL_HELP,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
    ARG_PRE_RUN,
    ARG_PRE_RUN_HELP,
    ARG_RESUME,
    ARG_RESUME_HELP,
    ARG_STEP,
    ARG_STEP_HELP,
    ARG_URI_LONG,
    ARG_URI_SHORT,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.manifest import (
    ExecutionFacts,
    StepRecord,
    expand_artifacts,
    merge_products,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.system.scheduler import slurm_job_id

if t.TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from cstar.entrypoint.config import ServiceConfiguration
    from cstar.orchestration.manifest import Product
    from cstar.orchestration.models import Blueprint


log = get_logger(__name__)

TBlueprint = t.TypeVar("TBlueprint", bound=HasApplication)


def _count(environ: "Mapping[str, str]", name: str) -> int:
    """Read a count from an environment variable.

    A record that says what ran a step must never change the outcome of the
    run, so a value that is not an integer, such as `2(x3)`, is logged and
    counts as not reported.

    Parameters
    ----------
    environ : Mapping[str, str]
        The process's environment variables.
    name : str
        The variable to read.

    Returns
    -------
    int
        The count; zero when the variable is unset, empty or not an integer.
    """
    value = environ.get(name, "").strip()
    if not value:
        return 0
    try:
        return int(value)
    except ValueError:
        log.debug(f"Ignoring {name}={value!r}: it is not a count")
        return 0


def execution_facts(environ: "Mapping[str, str]") -> ExecutionFacts:
    """Describe what is running this process, from its environment.

    Inside a SLURM job, including a standalone run in an `salloc`, the facts are
    the allocation the job was granted. Outside one, the step runs as a local
    process: its record says so and leaves the counts at zero, since the step
    does not know the compute it was asked for.

    Parameters
    ----------
    environ : Mapping[str, str]
        The process's environment variables.

    Returns
    -------
    ExecutionFacts
    """
    host = socket.gethostname()
    pid = os.getpid()

    if not (job_id := slurm_job_id(environ)):
        return ExecutionFacts(scheduler="local", host=host, pid=pid)

    return ExecutionFacts(
        scheduler="slurm",
        job_id=job_id,
        host=host,
        pid=pid,
        nodes=_count(environ, "SLURM_JOB_NUM_NODES"),
        tasks=_count(environ, "SLURM_NTASKS"),
        cpus=_count(environ, "SLURM_CPUS_ON_NODE"),
        partition=environ.get("SLURM_JOB_PARTITION", ""),
        account=environ.get("SLURM_JOB_ACCOUNT", ""),
    )


class BlueprintRunner(Service, XRunner[TBlueprint]):
    """A service that executes a blueprint."""

    _request: RunnerRequest[TBlueprint]
    """The instigating request."""
    _result: RunnerResult[TBlueprint]
    """The result produced by the application."""
    _started_at: "datetime | None"
    """When the service started, recorded as the start of the step."""
    products: "list[Product]"
    """The products the application reports while it runs."""

    def __init__(
        self,
        request: RunnerRequest[TBlueprint],
        service_cfg: "ServiceConfiguration",
    ) -> None:
        """Initialize the instance with the supplied configuration.

        Parameters
        ----------
        request: RunnerRequest[TBlueprint]
            A request containing information about the blueprint to run.
        service_cfg: ServiceConfiguration
            Configuration for modifying behavior of the service process.
        """
        Service.__init__(self, service_cfg)
        self._request = request
        self._result = RunnerResult(
            request,
            RunnerState(ExecutionStatus.UNSUBMITTED),
        )
        self.products = []
        self._started_at = None

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

    def _blueprint_file(self, root: Path) -> str:
        """Locate the blueprint this run loaded within the step root.

        Parameters
        ----------
        root : Path
            The step root, the blueprint's effective working directory.

        Returns
        -------
        str
            The POSIX path of the blueprint relative to `root`; empty for a
            remote blueprint or one outside `root`.
        """
        uri = self.blueprint_uri.strip()
        if not uri or urlsplit(uri).scheme:
            return ""

        path = Path(uri).expanduser().resolve()
        root = root.resolve()
        return path.relative_to(root).as_posix() if path.is_relative_to(root) else ""

    def _write_step_record(self) -> None:
        """Record what this step did in its working directory, with the products its
        application declares merged with those reported while it ran.

        A record that cannot be written (the blueprint did not load, the
        directory is unwritable) is logged and skipped: it never changes the
        outcome of the run.
        """
        try:
            bp = t.cast("Blueprint", self.blueprint)
            root = bp.effective_working_dir
            try:
                declared = expand_artifacts(
                    root, get_application(bp.application).artifacts(bp)
                )
                products = merge_products(declared, self.products)
            except Exception as ex:
                self.log.warning(f"Unable to expand the declared products: {ex}")
                products = list(self.products)
            record = StepRecord(
                run_id=os.getenv(ENV_CSTAR_RUNID, ""),
                step=self.request.step or bp.name,
                application=bp.application,
                blueprint=BlueprintIdentity.of(bp),
                blueprint_schema_version=bp.schema_version,
                blueprint_file=self._blueprint_file(root),
                outcome=str(self.state.status),
                start_at=self._started_at,
                finished_at=utc_now(),
                system=system_name(),
                versions=package_versions(),
                execution=execution_facts(os.environ),
                products=products,
                errors=list(self.result.errors),
            )
            path = write_step_record(root, record)
            self.log.debug(f"Wrote the step record to {path}")
        except Exception as ex:
            self.log.warning(f"Unable to write the step record: {ex}")

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
        self._started_at = utc_now()
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

        By default, a `BlueprintRunner` records the step's outcome in its
        working directory and logs the work disposition before shutdown.
        """
        self._write_step_record()
        self._log_disposition()

    @t.override
    def _cancel(self) -> None:
        """Handle a request to cancel the service."""
        self.add_state(
            ExecutionStatus.CANCELLED,
            ["The service was terminated on demand. Shutting down..."],
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
            self._stop_event.set()

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
    parser.add_argument(
        ARG_RESUME,
        action="store_true",
        help=ARG_RESUME_HELP,
    )
    parser.add_argument(
        ARG_PRE_RUN,
        action="store_true",
        help=ARG_PRE_RUN_HELP,
    )
    parser.add_argument(
        ARG_CLOBBER,
        action="store_true",
        help=ARG_CLOBBER_HELP,
    )
    parser.add_argument(
        ARG_STEP,
        type=str,
        default="",
        required=False,
        help=ARG_STEP_HELP,
    )
    return parser
