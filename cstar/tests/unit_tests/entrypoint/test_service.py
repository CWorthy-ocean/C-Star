import asyncio
import logging
import multiprocessing as mp
import queue
import time
import types
from collections import defaultdict
from unittest import mock

import pytest
from pydantic import ValidationError

from cstar.entrypoint.service import Service, ServiceConfiguration


class PrintingService(Service):
    """A minimal Service subclass used to test core service functionality.

    This service is used in place of a Mock to enable testing behaviors that occur on
    the health-check thread, such as shutdown and iteration counts.
    """

    def __init__(
        self,
        *,
        max_iterations: int = 0,
        as_service: bool = True,
        hc_freq: float | None = None,
        max_duration: float = 0.0,
        delay: float = 0.0,
    ) -> None:
        """Initialize the PrintingService."""
        config = ServiceConfiguration(
            as_service=as_service,
            loop_delay=delay,
            health_check_frequency=hc_freq,
            log_level=logging.DEBUG,
            health_check_log_threshold=20,
            name="PrintingService",
        )

        super().__init__(config)
        self._do_shutdown = False
        self.max_iter = abs(max_iterations)
        self.max_duration = abs(max_duration)
        self.start_time = 0.0
        self.test_queue: mp.Queue = mp.Queue()
        self.metrics: dict[str, int] = defaultdict(lambda: 0)

    @property
    def n_on_iteration(self) -> int:
        """Return the number of iterations executed."""
        return self.metrics["_on_iteration"]

    def _on_start(self) -> None:
        super()._on_start()
        self.log.debug("Running PrintingService._on_start")
        self.start_time = time.time()

    def _on_delay(self) -> None:
        super()._on_delay()
        self.log.debug("Running PrintingService._on_delay")
        self.test_queue.put_nowait("_on_delay")

    @property
    def n_on_delay(self) -> int:
        """Return the number of delays executed."""
        return self.metrics["_on_delay"]

    def _on_iteration(self) -> None:
        super()._on_iteration()
        self.log.debug("Running PrintingService._on_iteration")
        self.test_queue.put_nowait("_on_iteration")
        self.summarize()  # update each loop; don't let queues grow too large

    def _on_iteration_complete(self) -> None:
        super()._on_iteration_complete()
        self.log.debug("Running PrintingService._on_iteration_complete")
        self.test_queue.put_nowait("_on_iteration_complete")
        self.summarize()

    @property
    def n_on_health_check(self) -> int:
        """Return the number of health-checks executed."""
        return self.metrics["_on_health_check"]

    def _on_health_check(self) -> None:
        super()._on_health_check()
        self.log.debug("Running PrintingService._on_health_check")
        self.test_queue.put_nowait("_on_health_check")

    @property
    def n_can_shutdown(self) -> int:
        """Return the number of shutdown checks executed."""
        return self.metrics["_can_shutdown"]

    def _can_shutdown(self) -> bool:
        super()._can_shutdown()  # type: ignore[safe-super]
        self.log.debug("Running PrintingService._can_shutdown")
        self.test_queue.put_nowait("_can_shutdown")

        if self._do_shutdown:
            return self._do_shutdown

        if self.max_iter > 0:
            self._do_shutdown = self.n_on_iteration >= self.max_iter

        if self.max_duration > 0:
            elapsed = time.time() - self.start_time
            self._do_shutdown = elapsed >= self.max_duration

        return self._do_shutdown

    def _on_shutdown(self) -> None:
        super()._on_shutdown()
        self.log.debug("Running PrintingService._on_shutdown")
        self.test_queue.put_nowait("_on_shutdown")
        self.summarize()

    def summarize(
        self,
        finalize: bool = False,  # noqa: FBT001, FBT002
    ) -> dict[str, int]:
        """Return a summary of the test tracking queue contents.

        Utility for checking invocation counts where mock.call_count can't be used due
        to a second thread executing the service methods.
        """
        if not self.test_queue.empty() and finalize:
            while not self.test_queue.empty():
                if msg := self.test_queue.get_nowait():
                    self.metrics[msg] += 1

        elif not self.test_queue.empty():
            get_another = 10
            while get_another > 0 and not self.test_queue.empty():
                try:
                    msg = self.test_queue.get_nowait()
                    get_another -= 1
                except queue.Empty:  # noqa: PERF203
                    get_another = 0
                else:
                    self.metrics[msg] += 1

        return self.metrics

    def __enter__(self) -> "PrintingService":
        """Context manager entry point."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        """Context manager exit point."""
        if exc_type is not None:
            self.log.error(
                "Exception occurred in service context: %s",
                exc_value,
            )
        self._shutdown()


async def run_a_printer() -> None:
    """Run a PrintingService instance in a separate process.

    Utility method for testing signal handling or shutdown behavior.
    """
    service = PrintingService(as_service=True, hc_freq=1.0, max_duration=10)
    await service.execute()


async def run_a_fail_on_shutdown_printer() -> None:
    """Run a PrintingService instance in a separate process.

    Utility method for testing signal handling or shutdown behavior. This service will
    raise an exception on shutdown.
    """
    service = PrintingService(as_service=True, hc_freq=1.0, max_duration=10)
    mock.patch.object(
        service,
        "_on_shutdown",
        side_effect=RuntimeError("Kaboom!"),
    )
    await service.execute()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(100000, id="Large delay"),
        pytest.param(1, id="1s delay"),
        pytest.param(0.000001, id="Tiny delay"),
        pytest.param(0.0, id="No delay"),
    ],
)
async def test_config_check_delay(value: float) -> None:
    """Verify the acceptable input range of ServiceConfiguration.loop_delay."""
    ps = PrintingService(delay=value)
    assert ps


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(-0.000001, id="Tiny negative delay"),
        pytest.param(-1, id="1s negative delay"),
        pytest.param(-100000, id="Large, negative delay"),
    ],
)
async def test_config_check_delay_out_of_range(value: float) -> None:
    """Verify the acceptable input range of ServiceConfiguration.loop_delay."""
    with pytest.raises(ValidationError):
        _ = PrintingService(delay=value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(100000, id="Large hc_freq"),
        pytest.param(1, id="1s hc_freq"),
        pytest.param(0.000001, id="Tiny hc_freq"),
        pytest.param(0.0, id="0s hc_freq"),
    ],
)
async def test_config_check_hcfreq(value: float) -> None:
    """Verify the acceptable input range of ServiceConfiguration.loop_delay."""
    ps = PrintingService(hc_freq=value)
    assert ps


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(-0.000001, id="Tiny negative hc_freq"),
        pytest.param(-1, id="1s negative hc_freq"),
        pytest.param(-100000, id="Large, negative hc_freq"),
    ],
)
async def test_config_check_hcfreq_out_of_range(value: float) -> None:
    """Verify the acceptable input range of ServiceConfiguration.loop_delay."""
    with pytest.raises(ValidationError):
        _ = PrintingService(hc_freq=value)


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [1, 10, 100])
async def test_event_loop_shutdown(loop_count: int) -> None:
    """Verify that _on_iteration repeats until _can_shutdown returns True."""
    service = PrintingService(max_iterations=loop_count)

    # Service should run until `loop_count` is exceeded
    assert not service.can_shutdown

    await service.execute()

    assert service.can_shutdown
    assert service.n_on_iteration >= loop_count - 1


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [0, 10, 50, 100])
async def test_event_loop_task_service(loop_count: int) -> None:
    """Verify that  using as_service=False executes _on_iteration 1x."""
    with PrintingService(
        max_iterations=loop_count,
        as_service=False,
    ) as service:
        mock_on_iter = mock.MagicMock()
        mock.patch.object(service, "_on_iteration", mock_on_iter)

        # Service should run until _on_iteration is invoked
        # ...but internally it should aggregate the service config.
        assert not service._can_shutdown()  # noqa: SLF001
        assert service.can_shutdown

        # .execute should run the complete service life-cycle
        await service.execute()

        # Service should ignore the "max iter" shutdown clause and use the
        # _as_service flag to exit after one invocation
        assert service.can_shutdown


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [10, 40, 100])
async def test_event_loop_hc_start(loop_count: int) -> None:
    """Verify aspects of the health-check startup.

    The health check should not start until the service is executed, and should only
    execute a single time.
    """
    # mock up the HC start method to count calls
    mock_hc_start = mock.MagicMock()

    # Configure the health check to update every event loop iteration
    # (number of start calls shouldn't be affected)
    with (
        mock.patch(
            "cstar.entrypoint.Service._start_healthcheck",
            mock_hc_start,
        ),
        PrintingService(max_iterations=loop_count, hc_freq=1) as service,
    ):
        # Confirm it isn't called on instantiation
        assert mock_hc_start.call_count == 0

        # .execute starts the health check thread
        await service.execute()

        # Confirm it was called a single time during `execute`
        assert mock_hc_start.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [10, 30, 100])
async def test_event_loop_hc_freq(loop_count: int) -> None:
    """Verify that the health check occurs at the correct frequency.

    Confirm that using a frequency of 0 results in the health-check being executed in
    lockstep with _on_iteration.
    """
    # Configure the health check to update every event loop iteration
    with PrintingService(max_iterations=loop_count, hc_freq=0) as service:
        service._start_healthcheck()  # noqa: SLF001
        await asyncio.sleep(0.1)

        # Confirm the HC thread and queue are created
        assert service.is_healthcheck_running
        assert service.is_healthcheck_queue_ready

        # Complete the service lifecycle
        await service.execute()

        # Collect any leftover call metrics from the HC thread.
        service.summarize(finalize=True)

        assert service.n_on_health_check >= loop_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_duration", "frequency"),
    [
        (2.0, 0.5),
        (3.0, 0.01),
    ],
)
async def test_event_hc_freq(max_duration: float, frequency: float) -> None:
    """Verify that the health check occurs at the correct frequency.

    Confirm a frequency greater than zero is honored.
    """
    # Configure the test service to run for <max_duration> seconds.
    with PrintingService(
        as_service=True, hc_freq=frequency, max_duration=max_duration
    ) as service:
        # Complete the service lifecycle
        await service.execute()

        # Collect any leftover call metrics from the HC thread.
        service.summarize(finalize=True)

        # Confirm the hc frequency doesn't exceed maximum count possible (if
        # each HC occurred at exactly the right timestep and takes 0 time).
        # Off by small amount is acceptable.
        max_hc_calls = max_duration / frequency
        lower_bound = (0.9 * max_hc_calls) // max_hc_calls

        assert lower_bound <= service.n_on_health_check <= max_hc_calls


@pytest.mark.asyncio
async def test_event_hc_unknown_msg() -> None:
    """Verify message handling behavior of the health check thread.

    Confirms that the health check thread does not crash when it receives an unknown
    message type.
    """
    # Configure the test service to run for 2 seconds.
    with PrintingService(
        as_service=True,
        hc_freq=0.1,
        max_duration=2,
    ) as service:
        # Complete the service lifecycle
        service._start_healthcheck()  # noqa: SLF001
        await asyncio.sleep(0.05)

        # Send trash to the the HC thread
        service._send_update_to_hc(  # noqa: SLF001
            {"command": "unknown_command"},
        )

        # Confirm the hc thread is still alive and processing
        # messages by sending more!
        service._send_terminate_to_hc(  # noqa: SLF001
            "testing the message is still processed."
        )
        # Give the HC thread time to process the message
        await asyncio.sleep(0.05)

        assert not service.is_healthcheck_running


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_duration", "frequency"),
    [
        (2.0, 0.5),
        (3.0, 0.01),
    ],
)
async def test_event_hc_term(max_duration: float, frequency: float) -> None:
    """Verify that the health check thread terminates when asked to do so."""
    # Configure the test service to run for <max-duration> seconds.
    with PrintingService(
        as_service=True, hc_freq=frequency, max_duration=max_duration
    ) as service:
        # Complete the service lifecycle
        service._start_healthcheck()  # noqa: SLF001
        await asyncio.sleep(0.1)
        service._send_terminate_to_hc("test_event_hc_term")  # noqa: SLF001
        await asyncio.sleep(0.1)

        assert not service.is_healthcheck_running

        # Collect any leftover call metrics from the HC thread.
        service.summarize(finalize=True)

        # Confirm the hc frequency doesn't exceed maximum count possible (if
        # each HC occurred at exactly the right timestep and takes 0 time). Off
        # by small amount is acceptable.
        max_hc_calls = max_duration / frequency
        lower_bound = (0.9 * max_hc_calls) // max_hc_calls

        assert lower_bound <= service.n_on_health_check <= max_hc_calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("loop_delay", "loop_count"),
    [
        (0.05, 20),
        (0.1, 10),
    ],
)
async def test_delay(loop_delay: float, loop_count: int) -> None:
    """Verify that the health check thread terminates when asked to do so."""
    # the last delay is not executed before a shutdown.
    n_loops = loop_count - 1
    expected_duration = loop_delay * n_loops

    # Configure the test service to run for a fixed number of loops
    with PrintingService(
        as_service=True,
        delay=loop_delay,
        max_iterations=loop_count,
    ) as service:
        ts = time.time()
        await service.execute()
        te = time.time()
        elapsed = te - ts

        # Collect any leftover call metrics from the HC thread.
        service.summarize(finalize=True)
        assert service.n_on_delay >= n_loops

        # Confirm the cumulative delay to the total runtime is over the minimum
        # possible time, while allowing for compute overhead of processes/threads

        upper_bound = 1.4 * expected_duration

        assert expected_duration <= elapsed
        assert elapsed <= upper_bound


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_on_shutdown", [False, True])
async def test_signal_handling(fail_on_shutdown: bool) -> None:  # noqa: FBT001
    """Verify that the service shuts down gracefully when signals are sent.

    If the service is configured to fail on shutdown, the signal handler must gracefully
    handle the failure without crashing.
    """
    process: mp.Process | None = None

    try:
        printer_fn = run_a_fail_on_shutdown_printer
        if not fail_on_shutdown:
            # If not failing on shutdown, use the regular printer
            printer_fn = run_a_printer

        # Configure the test service to run for 90 seconds so a signal will
        # clearly cause it to exit early.
        process = mp.Process(target=printer_fn)
        process.start()
        await asyncio.sleep(0.1)

        term_at = time.time()
        process.terminate()  # Send a signal to terminate the service
        time_complete = time.time()

        expected_time_to_terminate = 2.0
        elapsed = time_complete - term_at
        assert elapsed < expected_time_to_terminate

    finally:
        if process and process.is_alive():
            process.kill()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "user_hook_name",
    [
        "_on_start",
        "_on_iteration",
        "_on_iteration_complete",
        "_on_shutdown",
        "_start_healthcheck",
        "_on_delay",
        "_can_shutdown",
    ],
)
async def test_user_unhandled_exceptions(user_hook_name: str) -> None:
    """Verify unhandled exception behaviors.

    This test verifies that the service does not allow a user-uncaught exception to
    crash the main process.
    """
    # Configured to run for 3 seconds but it should blow up immediately.
    service = PrintingService(
        # run as service to avoid shutting down before delay is invoke
        as_service=True,
        max_iterations=2,
        delay=0.01,
    )
    # Any exception raised in user code should not propagate here.
    with (
        mock.patch.object(
            service, user_hook_name, side_effect=RuntimeError("Kaboom!")
        ) as bomb,
    ):
        # Complete the service lifecycle
        await service.execute()

        # Confirm that the bomb went off and the service recovered.
        assert bomb.call_count
