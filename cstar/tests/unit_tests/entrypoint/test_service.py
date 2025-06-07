import asyncio
import logging
import multiprocessing as mp
import queue
import time
from collections import defaultdict
from typing import Optional
from unittest import mock

import pytest

from cstar.entrypoint.service import Service, ServiceConfiguration


class PrintingService(Service):
    """A minimal implementation of the Service class for testing core `Service` base
    class functionality."""

    def __init__(
        self,
        *,
        max_iterations: int = 0,
        as_service: bool = True,
        hc_freq: float = -1,
        max_duration: float = 0.0,
        delay: float = 0.0,
    ):
        config = ServiceConfiguration(
            as_service, delay, hc_freq, logging.DEBUG, "PrintingService"
        )

        super().__init__(config)
        self._do_shutdown = False
        self.max_iter = max_iterations
        self.max_duration = max_duration
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

    def _on_iteration(self) -> None:
        super()._on_iteration()
        self.log.debug("Running PrintingService._on_iteration")
        self.test_queue.put_nowait("_on_iteration")
        self.summarize()  # update each loop; don't let the queues grow indefinitely

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

    def summarize(self, finalize: bool = False) -> dict[str, int]:
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
                except queue.Empty:
                    get_another = 0
                else:
                    self.metrics[msg] += 1

        return self.metrics

    def __enter__(self):
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point."""
        if exc_type is not None:
            logging.error("Exception occurred in service context: %s", exc_value)
        self._send_terminate_to_hc("context manager exit")
        if self.test_queue is not None:
            self.test_queue.close()
            self.test_queue.join_thread()
        if self._hc_thread is not None:
            self._send_terminate_to_hc("context manager exit")
            self._hc_thread.join()
        return False


async def run_a_printer(fail_on_shutdown: bool = False) -> None:
    """Utility method to run a PrintingService instance in a separate process when
    testing signal handling or shutdown behavior."""
    service = PrintingService(as_service=True, hc_freq=1.0, max_duration=90)

    if fail_on_shutdown:
        mock.patch.object(service, "_on_shutdown", side_effect=RuntimeError("Kaboom!"))

    await service.execute()


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [1, 10, 1000])
async def test_event_loop_shutdown(loop_count: int):
    """Verify that the Service class invokes _on_iteration until _can_shutdown returns
    True."""
    service = PrintingService(max_iterations=loop_count)

    # Service should run until `loop_count` is exceeded
    assert not service.can_shutdown

    await service.execute()

    assert service.can_shutdown
    assert service.n_on_iteration >= loop_count - 1


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [0, 10, 100, 1000])
async def test_event_loop_task_service(loop_count: int):
    """Verify that a service run as-a-task (as_service=False) executes the _on_iteration
    method a single time and exits."""
    with PrintingService(max_iterations=loop_count, as_service=False) as service:
        mock_on_iter = mock.MagicMock()
        mock.patch.object(service, "_on_iteration", mock_on_iter)

        # Service should run until _on_iteration is invoked
        # ...but internally it should aggregate the service config.
        assert not service._can_shutdown()
        assert service.can_shutdown

        await service.execute()

        # Service should ignore the "max iter" shutdown clause and use the
        # _as_service flag to exit after one invocation
        assert service.can_shutdown


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [10, 100, 1000])
async def test_event_loop_hc_start(loop_count: int):
    """Verify that the health check does not start until the service is executed, and is
    only executed a single time."""

    # mock up the HC start method to count calls
    mock_hc_start = mock.MagicMock()

    # Configure the health check to update every event loop iteration
    # (number of start calls shouldn't be affected)
    with (
        mock.patch("cstar.entrypoint.Service._start_healthcheck", mock_hc_start),
        PrintingService(max_iterations=loop_count, hc_freq=0) as service,
    ):
        # Confirm it isn't called on instantiation
        assert mock_hc_start.call_count == 0

        await service.execute()

        # Confirm it was called a single time during `execute`
        assert mock_hc_start.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [10, 100, 1000])
async def test_event_loop_hc_freq(loop_count: int):
    """Verify that the health check occurs at the same frequency as _on_iteration, when
    specified with the appropriate frequency."""

    # Configure the health check to update every event loop iteration
    with PrintingService(max_iterations=loop_count, hc_freq=0) as service:
        service._start_healthcheck()

        # Confirm the HC thread and queue are created
        assert service._hc_thread is not None
        assert service._hc_queue is not None

        # Complete the service lifecycle
        await service.execute()

        # Collect any leftover call metrics from the HC thread.
        summary = service.summarize(finalize=True)

        assert summary["_on_health_check"] >= loop_count


@pytest.mark.asyncio
@pytest.mark.parametrize("max_duration, frequency", [(2.0, 0.5), (3.0, 0.01)])
async def test_event_hc_freq(max_duration: float, frequency: float):
    """Verify that the health check occurs at the correct frequency when frequency is
    greater than zero."""

    try:
        # Configure the test service to run for 10 seconds.
        with PrintingService(
            as_service=True, hc_freq=frequency, max_duration=max_duration
        ) as service:
            # Complete the service lifecycle
            await service.execute()

            # Collect any leftover call metrics from the HC thread.
            summary = service.summarize(finalize=True)

            # Confirm the hc frequency doesn't exceed maximum count possible (if each HC occurred
            # at exactly the right timestep and takes 0 time). Off by small amount is acceptable
            max_hc_calls = max_duration / frequency
            lower_bound = (0.9 * max_hc_calls) // max_hc_calls

            assert lower_bound <= summary["_on_health_check"] <= max_hc_calls
    except Exception as ex:
        assert False, str(ex)


@pytest.mark.asyncio
async def test_event_hc_unknown_msg():
    """Verify that the health check thread does not crash when it receives an unknown
    message type."""

    try:
        # Configure the test service to run for 10 seconds.
        with PrintingService(as_service=True, hc_freq=0.1, max_duration=2) as service:
            # Complete the service lifecycle
            service._start_healthcheck()
            await asyncio.sleep(0.05)

            # Send trash to the the HC thread
            service._send_update_to_hc({"command": "unknown_command"})
            # Confirm the hc thread is still alive and processing messages by sending more!
            service._send_terminate_to_hc("testing the message is still processed.")
            # Give the HC thread time to process the message
            await asyncio.sleep(0.05)

            assert service._hc_thread is not None
            assert not service._hc_thread.is_alive()

    except Exception as ex:
        assert False, str(ex)


@pytest.mark.asyncio
@pytest.mark.parametrize("max_duration, frequency", [(2.0, 0.5), (3.0, 0.01)])
async def test_event_hc_terminate(max_duration: float, frequency: float):
    """Verify that the health check thread terminates when asked to do so."""

    try:
        # Configure the test service to run for 10 seconds.
        with PrintingService(
            as_service=True, hc_freq=frequency, max_duration=max_duration
        ) as service:
            # Complete the service lifecycle
            service._start_healthcheck()
            await asyncio.sleep(0.1)
            service._send_terminate_to_hc("test_event_hc_terminate")
            await asyncio.sleep(0.01)

            assert service._hc_thread is None or not service._hc_thread.is_alive()

            # Collect any leftover call metrics from the HC thread.
            summary = service.summarize(finalize=True)

            # Confirm the hc frequency doesn't exceed maximum count possible (if each HC occurred
            # at exactly the right timestep and takes 0 time). Off by small amount is acceptable
            max_hc_calls = max_duration / frequency
            lower_bound = (0.9 * max_hc_calls) // max_hc_calls

            assert lower_bound <= summary["_on_health_check"] <= max_hc_calls
    except Exception as ex:
        assert False, str(ex)


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_delay,loop_count", [(0.05, 10), (0.05, 20), (0.1, 10)])
async def test_delay(loop_delay: float, loop_count: int):
    """Verify that the health check thread terminates when asked to do so."""

    try:
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
            summary = service.summarize(finalize=True)
            assert summary["_on_delay"] >= n_loops

            # Confirm the delay plus "other time" combine to a total runtime over
            # the minimum possible time, but don't let them vastly exceed min.

            upper_bound = 1.2 * expected_duration

            assert expected_duration <= elapsed
            assert elapsed <= upper_bound
    except Exception as ex:
        assert False, str(ex)


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_on_shutdown", [False, True])
async def test_signal_handling(fail_on_shutdown: bool):
    """Verify that the service responds to signals correctly and shuts down gracefully.

    If the service is configured to fail on shutdown, it should the signal handler must
    gracefully handle the failure without crashing.
    """

    process: Optional[mp.Process] = None

    try:
        # Configure the test service to run for 90 seconds so a signal will
        # clearly cause it to exit early.
        process = mp.Process(
            target=run_a_printer, kwargs={"fail_on_shutdown": fail_on_shutdown}
        )
        process.start()
        await asyncio.sleep(1)

        term_at = time.time()
        process.terminate()  # Simulate a signal to terminate the service
        time_complete = time.time()

        elapsed = time_complete - term_at
        assert elapsed < 2, "Service did not terminate quickly enough after signal."

    # assert lower_bound <= summary["_on_health_check"] <= max_hc_calls
    except Exception as ex:
        assert False, str(ex)
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
        "_on_health_check",  # done in 2nd thread, mock fails to patch
        "_on_delay",
        "_can_shutdown",
    ],
)
async def test_user_unhandled_exceptions(user_hook_name: str):
    """Verify that the service does not allow a user-uncaught exception to crash it."""

    try:
        # Configured to run for 3 seconds but it should blow up immediately.
        service = PrintingService(
            as_service=True, hc_freq=0.1, max_duration=3, delay=0.1
        )
        # Any exception raised in user code should not propagate here.
        with (
            mock.patch.object(
                service, user_hook_name, side_effect=RuntimeError("Kaboom!")
            ) as bomb,
            # mock.patch.object(service, "_start_healthcheck", mock.MagicMock()),
        ):
            # Complete the service lifecycle
            await service.execute()

            # Confirm that the bomb went off and the service recovered.
            assert bomb.call_count

    except Exception as ex:
        assert False, str(ex)
