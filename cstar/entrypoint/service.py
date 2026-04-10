import asyncio
import logging
import signal
import time
from abc import ABC, abstractmethod
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import TYPE_CHECKING, ClassVar, Final, Literal

from pydantic import BaseModel, Field

from cstar.base.log import LoggingMixin

if TYPE_CHECKING:
    from types import FrameType


class ServiceConfiguration(BaseModel):
    """Configuration options for a Service."""

    as_service: bool = False
    """Determines lifetime of the service.

    When `True`, calling `execute` on the service will run continuously until
    shutdown criteria are met. When `False`, the service completes a single
    pass through the service lifecycle and automatically exits.
    """
    loop_delay: float = Field(default=0.0, ge=0.0)
    """Duration (in seconds) of a delay between iterations of the main event loop."""
    health_check_frequency: float | None = Field(default=None, ge=0.0)
    """Time (in seconds) between calls to a health check handler.

    NOTE:
    - A value of `None` disables health checks.
    - A value of `0` triggers the health check on every iteration.
    """
    log_level: int = logging.INFO
    """The logging level used by the service."""
    health_check_log_threshold: int = Field(default=10, ge=3)
    """The number of health-checks that may be missed before logging."""
    name: str = "Service"
    """A user-friendly name for logging."""

    @property
    def healthcheck_enabled(self) -> bool:
        """Return `True` when the health check frequency is non-null.

        Returns
        -------
        bool
        """
        return self.health_check_frequency is not None


class Service(ABC, LoggingMixin):
    """Core API for standalone entrypoint scripts.

    Provides overridable hook methods to modify behaviors in the main event loop, during
    health checks, and for specifying shutdown criteria.
    """

    CMD_HEARTBEAT: Literal["heartbeat"] = "heartbeat"
    MIN_HCF: ClassVar[float] = 0.01

    _config: Final[ServiceConfiguration]
    """Runtime configuration of the `Service`"""
    _hc_thread: Thread | None = None
    """A thread for executing an unblocked healthcheck callback."""
    _hc_queue: Queue | None = None
    """A queue for sending shutdown messages to the healthcheck thread."""
    _hc_stop_event: Final[Event]
    """An event triggering health-check thread termination."""

    def __init__(
        self,
        config: ServiceConfiguration,
    ) -> None:
        """Initialize the Service.

        Parameters
        ----------
        config: ServiceConfiguration
            Configuration to modify the behavior of the service.
        """
        self._config = config
        self._hc_stop_event = Event()
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
    async def _on_iteration(self) -> None:
        """Perform the main logic of the service event loop.

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
        """Determine if the service has met shutdown criteria.

        This empty hook method must be implemented by subclasses.

        Returns
        -------
        bool
            `True` when the criteria to shut down the service are met.
        """

    @property
    def can_shutdown(self) -> bool:
        """Determine if the service is ready to shut down.

        User-defined shutdown criteria will not be evaluated when the service is
        instantiated as a task (e.g. with `as_service=False`).

        Returns
        -------
        bool
            `True` when the criteria to shut down the service are met
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

    def acknowledge_hc(self) -> None:
        """Confirm any requests for an update from the health check queue.

        Returns
        -------
        None
        """
        if not self._config.healthcheck_enabled:
            return

        if self._hc_queue is None:
            self.log.trace("No healthcheck queue available")
            return

        try:
            match self._hc_queue.get_nowait():
                case Service.CMD_HEARTBEAT:
                    # ACK any requests for HC updates
                    self.log.trace("Health check succeeded")
                    self._on_health_check()
                case message:
                    msg = f"Health check sent unknown message: {message}"
                    self.log.debug(msg)
        except Empty:
            # nothing to ACK
            ...

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
        if config.health_check_frequency is None:
            self.log.debug("Health check disabled.")
            return

        def _get_remaining_wait(start_at: float) -> float:
            """Determine the remaining time before the next health check message
            is expected.
            """
            elapsed = time.time() - start_at
            user_freq = config.health_check_frequency or 0
            raw_remaining = user_freq - elapsed

            # never allow exactly 0 wait (causing a busy-wait loop)
            return max(raw_remaining, Service.MIN_HCF)

        last_health_check = time.time()  # timestamp of last health check
        num_missed = 0

        while not self._hc_stop_event.is_set():
            remaining = _get_remaining_wait(last_health_check)
            if self._hc_stop_event.wait(timeout=remaining):
                return

            last_health_check = time.time()

            try:
                msg_queue.put(self.CMD_HEARTBEAT, timeout=0.1)
            except Full:
                # message was not acknowledged in expected timeframe
                num_missed += 1
            except Exception:  # noqa: BLE001
                # queue was shutdown on other side, exit HC loop
                self._hc_stop_event.set()
                num_missed = 0

            # only report consecutive gaps.
            if num_missed > config.health_check_log_threshold:
                msg = f"No successful health checks in {num_missed} attempts."
                self.log.warning(msg)
                num_missed = 0  # reset to avoid spamming log

    def _start_healthcheck(self) -> None:
        """Create a thread for health check updates.

        The health check thread is not blocked by the main thread when service
        operations are blocking.
        """
        if self.is_healthcheck_queue_ready:
            # health check is already running
            return

        freq = self._config.health_check_frequency

        if freq is not None and freq >= 0:
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
                    daemon=True,
                    kwargs={
                        "config": self._config,
                        "msg_queue": self._hc_queue,
                    },
                )
                self._hc_thread.start()

    def _terminate_hc(self) -> None:
        """Send a termination message to the healthcheck thread.

        After sending signal, waits for the thread to complete prior to returning.

        Returns
        -------
        None
        """
        self._hc_stop_event.set()

        if self._hc_thread is not None:
            self._hc_thread.join(timeout=10)

            if self._hc_thread.is_alive():
                msg = "Health check thread did not terminate before timeout"
                self.log.warning(msg)

    def _shutdown(self) -> None:
        """Perform a clean shutdown of the service.

        This method is called when the service is ready to shut down, either the service
        has completed its work, or it has received a term signal.
        """
        self.log.info("Shutting down service.")

        try:
            self._terminate_hc()
            self._on_shutdown()
        except Exception:
            self.log.debug("Service shutdown may not have completed.")
            raise

    async def execute(self) -> None:
        """Execute the complete service lifecycle.

        Completes the full service life-cycle. Responsible for executing calls
        to subclass implementation of `_on_iteration`. Evaluates shutdown
        conditions to trigger automatic service termination.
        """
        exc: Exception | None = None

        try:
            running = True
            self._on_start()
            self._start_healthcheck()
        except Exception as e:
            self.log.exception("Unable to start service.")
            running = False
            exc = e

        while running:
            try:
                self.acknowledge_hc()
                await self._on_iteration()
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
                self.log.trace("Service is ready for shutdown.")
                running = False
                continue

            # execute delay before next iteration
            if self._config.loop_delay:
                try:
                    self._on_delay()
                    await asyncio.sleep(max(self._config.loop_delay, 0))
                except Exception as e:
                    running = False
                    self.log.exception(
                        "Terminating service due to failure in _on_delay."
                    )
                    exc = e

        self._shutdown()

        if exc:
            raise exc

    def _handle_signal(self, sig_num: int, _frame: "FrameType | None") -> None:
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
            print("Unable to perform a clean shutdown for signal.")

    def _register_signal_handlers(self) -> None:
        """Register termination signal handlers for the service.

        Attempts to shutdown the simulation cleanly when interrupted.
        """
        for sig_num in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig_num, self._handle_signal)
