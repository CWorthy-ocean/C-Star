import shutil
import sys
from datetime import datetime, timezone
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
import cstar.scripts.runner.util as util
import logging
import pathlib
import contextvars

from cstar import Simulation
from cstar.base.exceptions import CstarException
from cstar.roms import ROMSSimulation
from cstar.base import log


ctx_correlation_id = contextvars.ContextVar("correlation_id", default=None)
CSTAR_USER_ENV_PATH = "~/.cstar.env"
CSTAR_EXTERNALS_ROOT = "~/code/cstar/cstar/externals"


class Worker(log.LoggingMixin):
    """Worker class to run c-star simulations."""

    def __init__(self, request: util.RunRequest):
        """Initialize the worker with a request."""
        self._blueprint_uri = request.blueprint_uri
        self._log_level = request.log_level
        self._simulation: Simulation | None = None
        self._output_root = request.output_dir.expanduser()
        self._output_dir = self._get_unique_path(self._output_root)
        self._simulation = ROMSSimulation.from_blueprint(
            blueprint=self._blueprint_uri,
            directory=self._output_dir,
            start_date="2012-01-03 12:00:00",
            end_date="2012-01-04 12:00:00",
        )
        self._simulation.interactive = False
        # TODO: get this from the cstar env
        self._user_env_path = pathlib.Path(CSTAR_USER_ENV_PATH).expanduser()
        # TODO: get this from the cstar env
        self._externals_path = pathlib.Path(CSTAR_EXTERNALS_ROOT).expanduser()

    def _get_unique_path(self, root_path: pathlib.Path) -> pathlib.Path:
        """Create a unique path name for outputs to avoid collision with other runs."""
        return root_path / f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"

    def _prepare_file_system(self) -> None:
        """Clean up old directories to avoid collisions and create new locations for the
        new run.

        NOTE: this may be unnecessary if the worker starts
        up a new container w/fresh directories every time.
        """
        if self._user_env_path.exists():
            self._user_env_path.unlink()

        if self._output_root.exists():
            shutil.rmtree(self._output_root)

        if self._externals_path.exists():
            shutil.rmtree(self._externals_path)
        self._externals_path.mkdir(parents=True, exist_ok=False)

        if not self._output_dir.exists():
            self._output_dir.mkdir(parents=True, exist_ok=True)

    # @util.autolog
    def _on_pre_run(self) -> None:
        """Prepare the simulation by setting up the input and output directories."""
        self._prepare_file_system()

        if self._blueprint_uri is None:  # TODO: move out to caller
            raise ValueError("No blueprint URI provided")

        try:
            if self._simulation is None:
                raise CstarException("Unable to load the blueprint")

            # Prepare the simulation
            self._simulation.setup()
            self._simulation.build()
            self._simulation.pre_run()
        except ValueError as ex:
            raise CstarException("Failed to prepare simulation") from ex

    def _on_running(self, handler: ExecutionHandler) -> None:
        # Check for updates... handler.updates is blocking w/0
        # - consider looping for more control of output?
        handler.updates(0, interactive=False)

    def _on_complete(self, disposition: ExecutionStatus) -> None:
        if not self._simulation:
            self.log.info("No simulation for completion hook")
            return

        # TODO: we probably want to be more specific about the status
        if disposition == ExecutionStatus.COMPLETED:
            self.log.info("Simulation completed successfully.")
        elif disposition == ExecutionStatus.FAILED:
            self.log.error("Simulation failed.")
        else:
            self.log.warning(f"Simulation ended with status: {disposition}")

        self._simulation.post_run()

    # @util.autolog
    def run(self) -> int:
        """Execute the c-star simulation."""

        try:
            if not self._blueprint_uri:
                raise ValueError("No blueprint path")

            if self._simulation is None:
                self.log.error("Simulation failed to load from blueprint")
                return 1

            # Retrieve & compile external resources
            self._on_pre_run()
            if self._simulation is None:
                self.log.error("Failed to prepare simulation")
                return 1

            # Run the simulation
            handler: ExecutionHandler = self._simulation.run()
            self._on_running(handler)

        except Exception:
            logging.exception("Failed to execute blueprint")
            return 1
        else:
            result = handler.status != ExecutionStatus.COMPLETED
            self._on_complete(handler.status)

        return result


def main() -> int:
    """Main entry point for the c-star worker script."""

    try:
        parser = util.create_parser()
        args = parser.parse_args()
    except SystemExit:
        # If the argument parsing fails, we exit with a non-zero status
        return 1
    else:
        request = util.RunRequest.from_args(args=args)
        ctx_correlation_id.set(request.request_id)  # type: ignore

    # logging.basicConfig(level=request.log_level, fmt="%(message)s")
    log = logging.getLogger()
    log.setLevel(request.log_level)  # __name__)

    worker = Worker(request)

    try:
        result = worker.run()
    except CstarException:
        log.exception("An error occurred during simulation")
        return 1

    return result


if __name__ == "__main__":
    # rc = asyncio.run(main())
    rc = main()
    sys.exit(rc)
