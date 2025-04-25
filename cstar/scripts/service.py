import asyncio
import dataclasses as dc
import logging
import time
from abc import ABC, abstractmethod
from multiprocessing import Queue
from queue import Empty, ShutDown
from threading import Thread

from cstar.base.log import LoggingMixin


@dc.dataclass
class ServiceConfiguration:
    as_service: bool = False
    """Determines lifetime of the service.

    When `True`, calling `execute`
    on the service will run continuously until shutdown criteria are met.
    Otherwise, `execute` performs a single pass through the service lifecycle
    and automatically exits (regardless of the result of `_can_shutdown`).
    """
    loop_delay: float = 0
    """Duration (in seconds) of a forced delay between iterations of the event loop."""
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

    Makes use of overridable hook methods to modify behaviors (event loop, automatic
    shutdown, cooldown) as well as simple hooks for status changes
    """

    def __init__(
        self,
        config: ServiceConfiguration,
    ) -> None:
        """Initialize the Service.

        :param as_service: Determines lifetime of the service. When `True`, calling
        execute on the service will run continuously until shutdown criteria are met.
        Otherwise, `execute` performs a single pass through the service lifecycle and
        automatically exits (regardless of the result of `_can_shutdown`).
        :param cooldown: Period of time (in seconds) to allow the service to run
        after a shutdown is permitted. Enables the service to avoid restarting if
        new work is discovered. A value of 0 disables the cooldown.
        :param loop_delay: Duration (in seconds) of a forced delay between
        iterations of the event loop
        :param health_check_frequency: Time (in seconds) between calls to a
        health check handler. A value of 0 triggers the health check on every
        iteration.
        """
        self._config = config
        """Runtime configuration of the `Service`"""

        self._hc_thread: Thread | None = None
        """A thread for executing an unblocked healthcheck callback."""
        self._hc_queue: Queue | None = None
        """A queue for sending shutdown messages to the healthcheck thread."""

    @abstractmethod
    def _on_iteration(self) -> None:
        """The user-defined event handler.

        Executed repeatedly until shutdown conditions are satisfied and cooldown is
        elapsed.
        """
        self.log.debug(f"Performing main iteration of {self.__class__.__name__}")

    def _on_iteration_complete(self) -> None:
        """Empty hook method for use by subclasses.

        Called in main event loop after `_on_iteration` is invoked.
        """
        self.log.debug(f"Performing post-iteration tasks of {self.__class__.__name__}")

    @abstractmethod
    def _can_shutdown(self) -> bool:
        """Return true when the criteria to shut down the service are met."""

    def _on_start(self) -> None:
        """Empty hook method for use by subclasses.

        Called on initial entry into Service `execute` event loop before
        `_on_iteration` is invoked.
        """
        self.log.debug(f"Starting {self.__class__.__name__}")

    def _on_shutdown(self) -> None:
        """Empty hook method for use by subclasses.

        Called immediately after exiting the main event loop during automatic shutdown.
        """
        self.log.debug(f"Shutting down {self.__class__.__name__}")

    def _on_health_check(self) -> None:
        """Empty hook method for use by subclasses.

        Invoked based on the
        value of `self._health_check_frequency`.
        """
        self.log.debug(f"Performing health check for {self.__class__.__name__}")

    def _on_delay(self) -> None:
        """Empty hook method for use by subclasses.

        Called on every event loop iteration immediately before executing a delay before
        the next iteration
        """
        self.log.debug(f"Service iteration waiting for {self.__class__.__name__}s")

    def _healthcheck(self, config: ServiceConfiguration, msg_queue: Queue) -> None:
        """Health-check event loop."""
        if config.health_check_frequency >= 0:
            last_health_check = time.time()  # timestamp of the latest health check
            running = True

            while running:
                hc_elapsed = time.time() - last_health_check
                if hc_elapsed >= config.health_check_frequency:
                    self._on_health_check()
                    last_health_check = time.time()
                    time.sleep(max(config.health_check_frequency - hc_elapsed, 0))

                try:
                    if msg := msg_queue.get(
                        True, timeout=config.health_check_frequency
                    ):
                        self.log.info(f"Healthcheck thread received command: {msg}")
                        # currently only one command to receive on queue. quit.
                        running = False
                except Empty:
                    ...  # ignore empty queue; just wait for shutdown msg
                except ShutDown:
                    # queue was shutdown on other side. allow thread to terminate
                    running = False

    def _start_healthcheck(self) -> None:
        """Create an independent thread for health check updates that is not blocked by
        the main thread if simulation is blocking."""
        if self._config.health_check_frequency >= 0:
            thread_name = f"{self._config.name}.healthcheck"
            self._hc_queue = Queue()
            self._hc_thread = Thread(
                target=self._healthcheck,
                name=thread_name,
                kwargs={"config": self._config, "msg_queue": self._hc_queue},
            )
            self._hc_thread.start()

    async def execute(self) -> None:
        """The main event loop of a service.

        Completes the full service life-cycle. Responsible for executing calls
        to subclass implementation of`_on_iteration`. Evaluates shutdown
        conditions to trigger automatic service termination.
        """

        try:
            self._start_healthcheck()
            self._on_start()
        except Exception:
            self.log.exception("Unable to start service.")
            return

        running = True

        while running:
            try:
                self._on_iteration()
            except Exception:
                running = False
                self.log.exception(
                    "Failure in event loop resulted in service termination"
                )
            else:
                self._on_iteration_complete()

            # shutdown if not set to run as a service
            if not self._config.as_service:
                running = False
                continue

            # shutdown if service reports completion
            if self._can_shutdown():
                running = False

            # execute delay before next iteration
            if self._config.loop_delay:
                self._on_delay()
                await asyncio.sleep(max(self._config.loop_delay, 0))

        try:
            if self._hc_queue:
                self._hc_queue.put_nowait("quit")

            if self._hc_thread and self._hc_thread.is_alive():
                self._hc_thread.join()
            self._on_shutdown()
        except Exception:
            self.log.exception("Service shutdown may not have completed.")
