import asyncio
import logging
import multiprocessing as mp
import multiprocessing.synchronize as mp_sync
import time
import types
import typing as t
from collections import defaultdict, deque
from math import ceil
from unittest import mock

import pytest
from pydantic import ValidationError

from cstar.entrypoint.config import ServiceConfiguration
from cstar.entrypoint.service import Service


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
        # a deque, not a queue: deque.append/popleft are atomic and lock-free,
        # so the signal handler (which reaches _on_shutdown) can never
        # self-deadlock by re-entering a lock the interrupted main thread
        # already holds. The service runs in a single process, so nothing
        # cross-process is needed here.
        self.test_queue: deque[str] = deque()
        self.metrics: dict[str, int] = defaultdict(lambda: 0)

    @property
    def n_on_iteration(self) -> int:
        """Return the number of iterations executed."""
        return self.metrics["_on_iteration"]

    def _on_start(self) -> None:
        super()._on_start()
        self.log.trace("Running PrintingService._on_start")
        self.start_time = time.time()

    def _on_delay(self) -> None:
        super()._on_delay()
        self.log.trace("Running PrintingService._on_delay")
        self.test_queue.append("_on_delay")

    @property
    def n_on_delay(self) -> int:
        """Return the number of delays executed."""
        return self.metrics["_on_delay"]

    async def _on_iteration(self) -> None:
        await super()._on_iteration()
        self.log.trace("Running PrintingService._on_iteration")
        self.test_queue.append("_on_iteration")
        self.summarize()  # update each loop; don't let queues grow too large

    def _on_iteration_complete(self) -> None:
        super()._on_iteration_complete()
        self.log.trace("Running PrintingService._on_iteration_complete")
        self.test_queue.append("_on_iteration_complete")
        self.summarize()

    @property
    def n_on_health_check(self) -> int:
        """Return the number of health-checks executed."""
        return self.metrics["_on_health_check"]

    def _on_health_check(self) -> None:
        super()._on_health_check()
        self.log.trace("Running PrintingService._on_health_check")
        self.test_queue.append("_on_health_check")

    @property
    def n_can_shutdown(self) -> int:
        """Return the number of shutdown checks executed."""
        return self.metrics["_can_shutdown"]

    def _can_shutdown(self) -> bool:
        super()._can_shutdown()  # type: ignore[safe-super]
        self.log.trace("Running PrintingService._can_shutdown")
        self.test_queue.append("_can_shutdown")

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
        self.log.trace("Running PrintingService._on_shutdown")
        self.test_queue.append("_on_shutdown")
        self.summarize()

    def summarize(
        self,
        finalize: bool = False,  # noqa: FBT001, FBT002
    ) -> dict[str, int]:
        """Return a summary of the test tracking queue contents.

        Utility for checking invocation counts where mock.call_count can't be used due
        to a second thread executing the service methods.
        """
        get_another = None if finalize else 10

        while get_another is None or get_another > 0:
            try:
                msg = self.test_queue.popleft()
            except IndexError:
                break

            self.metrics[msg] += 1
            if get_another is not None:
                get_another -= 1

        return self.metrics

    def __enter__(self) -> t.Self:
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


class _SignallingPrinter(PrintingService):
    """A PrintingService that sets an event once its main loop has started.

    The event must not be set before `_on_start` has run: a SIGTERM arriving
    between construction and startup is handled (the handlers are installed in
    `__init__`), and the test must only terminate a service that is running.
    """

    def __init__(self, started: "mp_sync.Event", **kwargs: t.Any) -> None:
        self._started_evt = started
        super().__init__(**kwargs)

    def _on_start(self) -> None:
        super()._on_start()
        self._started_evt.set()


async def _serve_printer(
    started: "mp_sync.Event",
    *,
    fail_on_shutdown: bool,
) -> None:
    """Run a PrintingService until signalled, setting `started` once its
    main loop is running.
    """
    service = _SignallingPrinter(started, as_service=True, hc_freq=1.0, max_duration=10)

    if fail_on_shutdown:
        patcher = mock.patch.object(
            service,
            "_on_shutdown",
            side_effect=RuntimeError("Kaboom!"),
        )
        patcher.start()

    await service.execute()


def run_a_printer(started: "mp_sync.Event") -> None:
    """Run a PrintingService instance in a separate process.

    Utility method for testing signal handling or shutdown behavior.
    """
    asyncio.run(_serve_printer(started, fail_on_shutdown=False))


def run_a_fail_on_shutdown_printer(started: "mp_sync.Event") -> None:
    """Run a PrintingService instance in a separate process.

    Utility method for testing signal handling or shutdown behavior. This service will
    raise an exception on shutdown.
    """
    asyncio.run(_serve_printer(started, fail_on_shutdown=True))


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
@pytest.mark.parametrize("loop_count", [1, 10, 50])
async def test_event_loop_shutdown(loop_count: int) -> None:
    """Verify that _on_iteration repeats until _can_shutdown returns True."""
    service = PrintingService(max_iterations=loop_count)

    # Service should run until `loop_count` is exceeded
    assert not service.can_shutdown

    await service.execute()

    assert service.can_shutdown
    assert service.n_on_iteration >= loop_count - 1


@pytest.mark.asyncio
@pytest.mark.parametrize("loop_count", [0, 10, 50])
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
@pytest.mark.parametrize("loop_count", [10, 30, 50])
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
        mock.patch.object(
            Service,
            "_start_healthcheck",
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
@pytest.mark.parametrize(
    ("max_duration", "delay"),
    [
        (0.5, 0.1),
        (0.5, 0.02),
        (0.25, 0.05),
    ],
)
async def test_event_loop_hc_freq(max_duration: float, delay: float) -> None:
    """Verify that the health check occurs at the correct frequency.

    Confirm that using a frequency ~equal to the loop frequency results
    in the health-check being executed in lockstep with _on_iteration.
    """
    expected_max_hc_calls = ceil(max_duration / delay)

    # Configure the health check at the same rate as the HC
    with PrintingService(
        max_duration=max_duration,
        hc_freq=delay,
        delay=delay,
    ) as service:
        # Complete the service lifecycle
        await service.execute()

        # Collect any leftover call metrics from the HC thread.
        service.summarize(finalize=True)

        assert service.n_on_health_check <= expected_max_hc_calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_duration", "frequency"),
    [
        (1.0, 0.1),
        (0.5, 0.01),
    ],
)
async def test_event_hc_freq(max_duration: float, frequency: float) -> None:
    """Verify that the health check occurs at the correct frequency.

    Confirm a frequency greater than zero is honored.
    """
    # Configure the test service to run for <max_duration> seconds.
    with PrintingService(
        as_service=True,
        hc_freq=frequency,
        max_duration=max_duration,
        delay=frequency,
    ) as service:
        # Complete the service lifecycle
        await service.execute()

        # Collect any leftover call metrics from the HC thread.
        service.summarize(finalize=True)

        # Confirm the hc frequency doesn't exceed maximum count possible (if
        # each HC occurred at exactly the right timestep and takes 0 time).
        # Off by small amount is acceptable.
        max_hc_calls = 1 + (max_duration / frequency)
        lower_bound = (0.9 * max_hc_calls) // max_hc_calls

        assert lower_bound <= service.n_on_health_check <= max_hc_calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("loop_delay", "loop_count"),
    [
        (0.05, 10),
        (0.1, 5),
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
        upper_bound = 1.5 * expected_duration

        assert expected_duration <= elapsed
        assert elapsed <= upper_bound


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_on_shutdown", [False, True])
async def test_signal_handling(fail_on_shutdown: bool) -> None:  # noqa: FBT001
    """Verify that the service shuts down gracefully when signals are sent.

    If the service is configured to fail on shutdown, the signal handler must gracefully
    handle the failure without crashing.

    The child is started with the "spawn" method: forking the multi-threaded
    pytest process can deadlock the child (and has hung CI teardown on Linux,
    where fork is the default start method).
    """
    ctx = mp.get_context("spawn")
    started = ctx.Event()

    printer_fn = run_a_fail_on_shutdown_printer
    if not fail_on_shutdown:
        # If not failing on shutdown, use the regular printer
        printer_fn = run_a_printer

    # The service is configured to run for 10 seconds so a signal will
    # clearly cause it to exit early.
    process = ctx.Process(target=printer_fn, args=(started,))

    try:
        process.start()

        # spawn boots a fresh interpreter, so wait until the service's main
        # loop is running before terminating it.
        assert started.wait(timeout=30), "service process did not start"

        process.terminate()  # Send a signal to terminate the service

        expected_time_to_terminate = 5.0
        process.join(timeout=expected_time_to_terminate)

        assert process.exitcode is not None, "service did not exit after SIGTERM"
        # a clean shutdown exits 0; the fail-on-shutdown service re-raises on
        # the main thread and exits nonzero, but must still exit promptly.
        expected_exitcode = 1 if fail_on_shutdown else 0
        assert process.exitcode == expected_exitcode

    finally:
        if process.is_alive():
            process.kill()
        process.join(timeout=5)
        process.close()


@pytest.mark.skip(
    "At the moment, we _want_ the service to crash if there is an error, because we"
    "rely on that exit code to block dependent tasks from executing. Open to reconsideration"
    "if there's a better way to do this."
)
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
