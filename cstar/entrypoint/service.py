import asyncio
import dataclasses as dc
import logging
import signal
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from multiprocessing import Queue
from queue import Empty
from threading import Thread
from types import FrameType

from cstar.base.log import LoggingMixin


@dc.dataclass
class ServiceConfiguration:
    """Configuration options for a Service."""

    as_service: bool = False
    """Determines lifetime of the service.

    When `True`, calling `execute` on the service will run continuously until
    shutdown criteria are met. When `False`, the service completes a single
    pass through the service lifecycle and automatically exits.
    """
    loop_delay: float = 0
    """Duration (in seconds) of a forced delay between iterations of the main event
    loop."""
    health_check_frequency: float = 0
    """Time (in seconds) between calls to a health check handler.

    A value of 0 triggers the health check on every iteration.
    """
    log_level: int = logging.INFO
    """The logging level used by the service."""
    name: str = "Service"
    """A user-friendly name for logging."""


class Service(ABC, LoggingMixin):
    """Core API for standalone entrypoint scripts.

    Provides overridable hook methods to modify behaviors in the main event loop, during
    health checks, and for specifying shutdown criteria.
    """

    def __init__(
        self,
        config: ServiceConfiguration,
    ) -> None:
        """Initialize the Service.

        Parameters
        ----------
        config: ServiceConfiguration
            Configuration to modify the behavior of the service.

        Returns
        -------
        None
        """
        self._config = config
        """Runtime configuration of the `Service`"""

        self._hc_thread: Thread | None = None
        """A thread for executing an unblocked healthcheck callback."""
        self._hc_queue: Queue | None = None
        """A queue for sending shutdown messages to the healthcheck thread."""

        self._register_signal_handlers()

    @property
    def _service_type(self) -> str:
        """Determine the name of the running service class.

        Returns
        -------
        str
            The name of the class, used for logging purposes.
        """
        return self.__class__.__name__

    @abstractmethod
    def _on_iteration(self) -> None:
        """Contains the main logic into of the service event loop.

        It is executed continuously until shutdown conditions are satisfied.
        """
        self.log.debug(f"Performing main iteration of {self._service_type}")

    def _on_iteration_complete(self) -> None:
        """Empty hook method for use by subclasses.

        Called in main event loop after `_on_iteration` is invoked.
        """
        self.log.debug(f"Performing post-iteration tasks of {self._service_type}")

    @abstractmethod
    def _can_shutdown(self) -> bool:
        """Empty hook method for use by subclasses.

        Return `True` when the criteria to shut down the service are met.
        """

    @property
    def can_shutdown(self) -> bool:
        """Determine if the service is ready to shut down.

        User-defined shutdown criteria will not be evaluated when the service is
        instantiated as a task (e.g. with `as_service=False`).
        """
        if not self._config.as_service:
            return True

        try:
            return self._can_shutdown()
        except Exception:
            self.log.exception(
                "Terminating service due to failure in shutdown check. "
                "Assuming service cannot shutdown cleanly."
            )
            return True

        return False

    @property
    def is_healthcheck_running(self) -> bool:
        """Determine if the healthcheck thread is running.

        Returns
        -------
        bool
            `True` if the healthcheck thread is alive, `False` otherwise.
        """
        return self._hc_thread is not None and self._hc_thread.is_alive()

    @property
    def is_healthcheck_queue_ready(self) -> bool:
        """Determine if the healthcheck queue is available.

        Returns
        -------
        bool
            `True` if the healthcheck queue is initialized, `False` otherwise.
        """
        return self._hc_queue is not None

    def _on_start(self) -> None:
        """Empty hook method for use by subclasses.

        Called on initial entry into Service lifecycle before the main service logic
        begins execution. Allows for subclasses to perform any required initialization
        logic.
        """
        self.log.debug(f"Starting {self._service_type}")

    def _on_shutdown(self) -> None:
        """Empty hook method for use by subclasses.

        Called immediately after exiting the main event loop.
        """
        self.log.debug(f"Shutting down {self._service_type}")

    def _on_health_check(self) -> None:
        """Empty hook method for use by subclasses.

        Called by the health check thread at the configured frequency.
        """
        self.log.debug(f"Performing health check for {self._service_type}")

    def _on_delay(self) -> None:
        """Empty hook method for use by subclasses.

        Called at the completion of every event loop iteration when a user-defined delay
        is configured.
        """
        self.log.debug(f"Service delay for {self._service_type}")

    def _create_hc_update(self, content: dict[str, str]) -> dict[str, str]:
        """Create a health check update message.

        Parameters
        ----------
        content: dict[str, str]
            Key-value mapping of content to include in the health check update.

        Returns
        -------
        dict[str, str]
            The complete content of a health check update ready to be sent.
        """
        update_base = {"ts": datetime.now(tz=timezone.utc).isoformat()}
        update_base.update(content)
        return update_base

    def _send_update_to_hc(self, content: dict[str, str]) -> None:
        """Send an update to the health check thread.

        Parameters
        ----------
        content: dict[str, str]
            Key-value mapping of content to include in the health check update.

        Returns
        -------
        None
        """
        if self._hc_queue:
            msg = self._create_hc_update(content)
            self._hc_queue.put_nowait(msg)
        else:
            self.log.debug("No healthcheck queue available")

    def _send_terminate_to_hc(self, reason: str) -> None:
        """Send a termination message to the health check thread.

        Parameters
        ----------
        reason: str
            A string describing the reason for termination.

        Returns
        -------
        None
        """
        self.log.debug("Terminating healthcheck")
        self._send_update_to_hc({"cmd": "quit", "reason": reason})

    def _healthcheck(
        self,
        config: ServiceConfiguration,
        msg_queue: Queue,
    ) -> None:
        """Run the health-check event loop.

        This method runs in a separate thread to ensure that the main event loop cannot
        block health check updates. The health check waits for messages from the main
        thread and publishes a health check update at the specified interval. If the
        main thread fails to send a message, it logs a warning.

        Parameters
        ----------
        config: ServiceConfiguration
            The configuration settings for the service.

        msg_queue: Queue
            A queue for receiving messages from the main thread.

        Returns
        -------
        None
        """
        if config.health_check_frequency < 0:
            self.log.debug("Health check disabled with a negative frequency.")
            return

        last_health_check = time.time()  # timestamp of last health check
        running = True
        update_delay = 0.0
        hc_elapsed = 0.0

        while running:
            try:
                hc_elapsed = time.time() - last_health_check
                hcf_remaining = config.health_check_frequency - hc_elapsed
                remaining_wait = max(hcf_remaining, 0)

                # report large gaps between updates.
                if hc_elapsed > 3 * config.health_check_frequency:
                    self.log.warning(
                        f"No health update in last {update_delay:.2f} seconds."
                    )

                if msg := msg_queue.get_nowait():
                    if hc_elapsed >= config.health_check_frequency:
                        self._on_health_check()
                        last_health_check = time.time()

                    command = msg.get("cmd", None)
                    if not command:
                        self.log.info(
                            f"Healthcheck thread received message: {msg}",
                        )
                    elif command == "quit":
                        running = False
                else:
                    time.sleep(remaining_wait)

            except Empty:  # noqa: PERF203
                ...  # ignore empty queue; just wait for shutdown msg
            except Exception:  # noqa: BLE001
                # queue was shutdown on other side. ignore and exit.
                running = False

    def _start_healthcheck(self) -> None:
        """Create a thread for health check updates.

        The health check thread is not blocked by the main thread when service
        operations are blocking.
        """
        if self._config.health_check_frequency >= 0:
            self.log.debug(
                "Starting healthcheck thread w/frequency: %s.",
                self._config.health_check_frequency,
            )

            thread_name = f"{self._config.name}.healthcheck"

            if self._hc_queue is None:
                self._hc_queue = Queue()

            if self._hc_thread is None:
                self._hc_thread = Thread(
                    target=self._healthcheck,
                    name=thread_name,
                    kwargs={
                        "config": self._config,
                        "msg_queue": self._hc_queue,
                    },
                )
                self._hc_thread.start()

    def _terminate_hc(self, reason: str) -> None:
        """Send a termination message to the healthcheck thread.

        After sending signal, waits for the thread to complete prior to returning.

        Parameters
        ----------
        reason: str
            A string describing the reason for termination.

        Returns
        -------
        None
        """
        self._send_terminate_to_hc(reason=reason)
        if self._hc_thread and self._hc_thread.is_alive():
            self._hc_thread.join()

    def _shutdown(self) -> None:
        """Perform a clean shutdown of the service.

        This method is called when the service is ready to shut down, either the service
        has completed its work, or it has received a term signal.
        """
        self.log.debug("Shutting down service.")

        try:
            self._terminate_hc("Run complete")
            self._on_shutdown()
        except Exception:
            self.log.exception("Service shutdown may not have completed.")

    async def execute(self) -> None:
        """Execute the complete service lifecycle.

        Completes the full service life-cycle. Responsible for executing calls
        to subclass implementation of `_on_iteration`. Evaluates shutdown
        conditions to trigger automatic service termination.
        """
        try:
            running = True
            self._start_healthcheck()
            self._on_start()
        except Exception:
            self.log.exception("Unable to start service.")
            running = False

        while running:
            try:
                self._on_iteration()
                self._on_iteration_complete()
            except Exception:
                running = False
                self.log.exception("Terminating service due to failure in event loop.")
                continue

            # shutdown if not set to run as a service
            if not self._config.as_service:
                self.log.debug("Terminating non-service immediately.")
                running = False
                continue

            # shutdown if service reports completion
            if self.can_shutdown:
                self.log.info("Service is ready for shutdown.")
                running = False
                continue

            # execute delay before next iteration
            if self._config.loop_delay:
                try:
                    self._on_delay()
                    await asyncio.sleep(max(self._config.loop_delay, 0))
                except Exception:
                    running = False
                    self.log.exception(
                        "Terminating service due to failure in _on_delay."
                    )

            self._send_update_to_hc({"cmd": "heartbeat"})

        self._shutdown()

    def _handle_signal(self, sig_num: int, _frame: FrameType | None) -> None:
        """Handle OS signals requesting process shutdown.

        Parameters
        ----------
        sig_num: int
            A string describing the reason for termination.

        _frame: FrameType | None
            The current stack frame (unused).

        Returns
        -------
        None
        """
        self.log.info(f"Received signal: {sig_num}")

        try:
            # perform sub-class specific clean-up
            self._shutdown()
        except Exception:
            self.log.exception(
                "Unable to perform a clean shutdown for signal.",
            )

    def _register_signal_handlers(self) -> None:
        """Register termination signal handlers for the service.

        Attempts to shutdown the simulation cleanly when interrupted.
        """
        for sig_num in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig_num, self._handle_signal)
