import pytest

from cstar.orchestration.launch.local import (
    LocalComputeSpec,
    TimeConstrainedRunRequestEnricher,
)
from cstar.orchestration.orchestration import RunRequest


def test_timeconstrainedrunrequestenricher_default_localcomputespec() -> None:
    """Verify that an enriched local run request is configured appropriately using the
    LocalComputeSpec with default values.
    """
    compute = LocalComputeSpec()
    request = RunRequest(command=["python", "-m", "venv", ".venv"])

    enricher = TimeConstrainedRunRequestEnricher(compute)
    enriched_request = enricher.enrich(request)

    # confirm the original command is enriched and not wiped out
    assert " ".join(request.command) in " ".join(enriched_request.command)
    # confirm the command will be time constrained via timeout
    assert f"timeout {compute.walltime_seconds}" in " ".join(enriched_request.command)
    # confirm the force-kill period is specified
    exp_fk = f"{TimeConstrainedRunRequestEnricher.ARG_FORCEKILL_TIMEOUT} {compute.force_kill_seconds}"
    assert exp_fk in " ".join(enriched_request.command)


@pytest.mark.parametrize(
    "compute, exp_match",
    [
        (
            LocalComputeSpec(max_walltime="00:01:40", force_kill_timeout="00:09"),
            "timeout 100s -k 9s",
        ),
        (
            LocalComputeSpec(max_walltime="00:00:10", force_kill_timeout="00:10"),
            "timeout 10s -k 10s",
        ),
        (
            LocalComputeSpec(max_walltime="00:10"),
            "timeout 10s -k 2s",  # expect default fk timeout
        ),
        (
            LocalComputeSpec(force_kill_timeout="00:01:39"),
            "timeout 600s -k 99s",  # expect default overall timeout
        ),
    ],
)
def test_timeconstrainedrunrequestenricher_custom_localcomputespec(
    compute: LocalComputeSpec,
    exp_match: str,
) -> None:
    """Verify that an enriched local run request is configured appropriately using the
    LocalComputeSpec with user-supplied values.
    """
    request = RunRequest(command=["python", "-m", "venv", ".venv"])

    enricher = TimeConstrainedRunRequestEnricher(compute)
    enriched_request = enricher.enrich(request)

    assert enriched_request

    # confirm the command is customized correctly with the user-supplied customization
    assert exp_match in " ".join(enriched_request.command)
    # confirm the original command will be executed with the specified timeout
    assert " ".join(request.command) in " ".join(enriched_request.command)
