# import dataclasses as dc
# import typing as t
# from abc import abstractmethod

# from pydantic import BaseModel, ConfigDict

# from cstar.base.exceptions import BlueprintError
# from cstar.entrypoint.service import Service, ServiceConfiguration
# from cstar.entrypoint.worker.worker import JobConfig
# from cstar.execution.handler import ExecutionStatus
# from cstar.orchestration.models import Blueprint
# from cstar.orchestration.serialization import deserialize

# DATE_FORMAT: t.Final[str] = "%Y-%m-%d %H:%M:%S"
# WORKER_LOG_FILE_TPL: t.Final[str] = "cstar-worker.{0}.log"
# JOBFILE_DATE_FORMAT: t.Final[str] = "%Y%m%d_%H%M%S"
# LOGS_DIRECTORY: t.Final[str] = "logs"


# class WorkRequest(t.Protocol):
#     """Core API of a request to run a blueprint via worker."""

#     @property
#     def blueprint_uri(self) -> str:
#         """The location of the blueprint."""
#         ...


# class BaseBlueprintRequest(BaseModel):
#     """A request to run a blueprint."""

#     blueprint_uri: str
#     """The location of the blueprint."""

#     model_config: t.ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)


# @dc.dataclass
# class RunnerResult:
#     request: WorkRequest | None = None
#     """The request that triggered the blueprint."""
#     status: ExecutionStatus = ExecutionStatus.UNKNOWN
#     """The final disposition of the runner's work."""
#     errors: list[str] = dc.field(default_factory=list)


# class BlueprintRunner(Service):
#     """A service that executes a blueprint."""

#     _disposition: ExecutionStatus = ExecutionStatus.UNKNOWN
#     """The status of the work being executed by the runner."""

#     _request: WorkRequest
#     """The instigating request."""

#     _blueprint: Blueprint | None = None
#     """The Blueprint deserialized from the user-supplied URI."""

#     _result: RunnerResult | None = None

#     def __init__(
#         self,
#         request: WorkRequest,
#         service_cfg: ServiceConfiguration,
#         job_cfg: JobConfig,
#     ) -> None:
#         """Initialize the `BlueprintRunner` with the supplied configuration.

#         Parameters
#         ----------
#         request: BlueprintRequest
#             A request containing information about the blueprint to run

#         service_cfg: ServiceConfiguration
#             Configuration for modifying behavior of the service process.

#         job_cfg: JobConfig
#             Configuration for submitting jobs to an HPC, such as account ID,
#             walltime, job name, and priority.
#         """
#         super().__init__(service_cfg)

#         self._job_config = job_cfg
#         self._request = request

#     @property
#     def blueprint_uri(self) -> str:
#         """Return the URI of the blueprint to run."""
#         return self._request.blueprint_uri.strip()

#     @property
#     def blueprint(self) -> Blueprint:
#         """Return the deserialized blueprint to be executed by the runner.

#         Raises
#         ------
#         ValueError
#             If the blueprint cannot be deserialized to the expected type.

#         Returns
#         -------
#         Blueprint
#         """
#         if self._blueprint is None:
#             self._blueprint = deserialize(
#                 self.blueprint_uri,
#                 self._blueprint_type(),
#             )

#         return self._blueprint

#     @property
#     def status(self) -> ExecutionStatus:
#         """Return the status of the runner's work."""
#         return self._result.status if self._result else ExecutionStatus.UNKNOWN

#     def is_done(self) -> bool:
#         """Return `True` if the blueprint has completed execution.

#         Returns
#         -------
#         bool
#         """
#         return ExecutionStatus.is_terminal(self.status)

#     def _log_disposition(self) -> None:
#         """Log the status of the runner's work at the current time."""
#         msg = f"Final disposition of task executed by service {self._config.name!r} is {self.status.name!r}"
#         self.log.debug(msg)

#     @t.override
#     def _can_shutdown(self) -> bool:
#         """Determine if the service can shutdown.

#         Returns
#         -------
#         bool
#             `True` if the service can shutdown, `False` otherwise.
#         """
#         if self.is_done():
#             self.log.info(f"Shutdown is allowed ({self._disposition}).")
#             return True

#         return False

#     @t.override
#     def _on_start(self) -> None:
#         """Prepare the runner for execution.

#         Performs validation of arguments received via CLI.
#         """
#         if not self.blueprint_uri:
#             msg = "No blueprint URI provided"
#             raise BlueprintError(msg)

#         self._blueprint = deserialize(self.blueprint_uri, self._blueprint_type())

#     @t.override
#     async def _on_iteration(self) -> None:
#         """Execute the blueprint processing implemented in the subclass."""
#         try:
#             self._result = self._run_blueprint(self.blueprint)
#         except Exception:
#             msg = f"An error occurred while running blueprint: {self.blueprint_uri}"
#             self.log.exception(msg)
#             self.set_result(ExecutionStatus.FAILED, [msg])

#     @t.override
#     def _on_shutdown(self) -> None:
#         """Perform actions required when shutting down the service.

#         By default, the `BlueprintRunner` logs the work disposition before shutdown.
#         """
#         self._log_disposition()

#     @abstractmethod
#     def _run_blueprint(self, blueprint: Blueprint) -> RunnerResult:
#         """Process the blueprint.

#         Returns
#         -------
#         RunnerResult
#             The result of the blueprint processing.
#         """
#         ...

#     @abstractmethod
#     def _blueprint_type(self) -> type[Blueprint]:
#         """Return the type of Blueprint to be deserialized.

#         Returns
#         -------
#         type[Blueprint]
#         """
#         ...

#     def set_result(
#         self,
#         status: ExecutionStatus,
#         errors: list[str] | None = None,
#     ) -> RunnerResult:
#         """Create a RunnerResult instance and store the value.

#         Uses

#         Returns
#         -------
#         RunnerResult
#         """
#         if self._result is None:
#             self._result = RunnerResult(self._request, status, errors or [])
#         return self._result
