import asyncio
import sys
import typing as t

from cstar.entrypoint.worker.app_host import (
    BlueprintRunner,
    RunnerResult,
    create_parser,
    execute_runner,
    get_base_request,
    get_job_config,
    get_service_config,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Application, Blueprint


class HelloWorldBlueprint(Blueprint):
    """A simple blueprint demonstrating the integration of a Blueprint and it's
    runner application.
    """

    application: Application = Application.HELLO_WORLD
    """The application identifier."""
    target: str
    """The person to say hello to."""


class HelloWorldRunner(BlueprintRunner):
    """Worker class to execute a simple "Hello, world" application specified via blueprint."""

    @t.override
    @property
    def blueprint(self) -> HelloWorldBlueprint:
        """Return the deserialized blueprint to be executed by the runner.

        Overrides `BlueprintRunner.blueprint` to provide a more specific type hint.

        Raises
        ------
        ValueError
            If the blueprint cannot be deserialized to the expected type.

        Returns
        -------
        HelloWorldBlueprint
        """
        raw_bp = super().blueprint
        if not isinstance(raw_bp, HelloWorldBlueprint):
            raise ValueError(f"HelloWorldBlueprint expected. Received: {type(raw_bp)}")
        return raw_bp

    @t.override
    def _run_blueprint(self, blueprint: Blueprint) -> RunnerResult:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        if not isinstance(blueprint, HelloWorldBlueprint):
            raise ValueError(
                f"HelloWorldBlueprint expected. Received: {type(blueprint)}"
            )

        print(f"Hello, {blueprint.target}")
        return self.set_result(ExecutionStatus.COMPLETED)

    @t.override
    def _blueprint_type(self) -> type[Blueprint]:
        """Return the type of Blueprint to be deserialized.

        Returns
        -------
        type[Blueprint]
        """
        return HelloWorldBlueprint


async def main() -> int:
    """Parse CLI arguments and run a c-star HelloWorldRunner.

    Triggers the lifecycle of a `Worker` service responsible for processing
    the blueprint supplied by the user.

    Returns
    -------
    int
        The exit code of the worker script. Returns 0 on success, 1 on failure.
    """
    try:
        parser = create_parser()
        args = parser.parse_args()
    except SystemExit as ex:
        print(f"Parsing CLI arguments failed: {ex}")
        return 1

    job_cfg = get_job_config()
    service_cfg = get_service_config(args.log_level)
    request = get_base_request(args.blueprint_uri)

    result = await execute_runner(HelloWorldRunner, job_cfg, service_cfg, request)
    if result.errors:
        return 1
    return 0


if __name__ == "__main__":
    rc = asyncio.run(main())
    sys.exit(rc)
