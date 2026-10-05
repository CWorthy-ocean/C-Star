import typing as t

import pytest

from cstar.base.adapter import CstarAdaptationError
from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.compute_environment import (
    ComputeEnvironment,
    ComputeEnvironmentAdapter,
    resolve_compute_environment,
)
from cstar.orchestration.launch.slurm import SlurmComputeSpec
from cstar.orchestration.models import COMPUTE_OVERRIDE_NAMESPACES


def test_adapter_empty_not_expected() -> None:
    """Verify an empty mapping is "nothing for this adapter"."""
    with pytest.raises(CstarExpectationFailed):
        ComputeEnvironmentAdapter().adapt({})


def test_adapter_adapts_complete_environment() -> None:
    """Verify every documented key is adapted."""
    env = ComputeEnvironmentAdapter().adapt(
        {
            "launcher": "slurm",
            "system": "anvil",
            "slurm": {
                "account_name": "x-ees250129",
                "queue_name": "wholenode",
                "max_walltime": "04:00:00",
            },
        }
    )

    assert env.launcher == "slurm"
    assert env.system == "anvil"
    assert env.slurm == SlurmComputeSpec(
        account_name="x-ees250129", queue_name="wholenode", max_walltime="04:00:00"
    )


def test_adapter_defaults() -> None:
    """Verify unset keys take their defaults."""
    env = ComputeEnvironmentAdapter().adapt({"system": "anvil"})

    assert env.launcher == ""
    assert env.slurm is None


@pytest.mark.parametrize("launcher", ["local", "slurm", ""])
def test_launcher_names_are_accepted(launcher: str) -> None:
    """Verify the launcher names match those a step's compute overrides use."""
    assert ComputeEnvironment(launcher=launcher).launcher == launcher


def test_launcher_names_are_the_override_namespaces() -> None:
    """Verify every compute-override namespace is a selectable launcher."""
    assert {
        name
        for name in COMPUTE_OVERRIDE_NAMESPACES
        if ComputeEnvironment(launcher=name).launcher
    } == COMPUTE_OVERRIDE_NAMESPACES


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        pytest.param({"bogus": 1}, "bogus", id="unknown-key"),
        pytest.param({"launcher": "pbs"}, "pbs", id="bad-launcher"),
        pytest.param(
            {"slurm": {"max_walltime": "soon"}}, "max_walltime", id="bad-walltime"
        ),
    ],
)
def test_adapter_invalid_content(content: dict[str, t.Any], expected: str) -> None:
    """Verify invalid content raises an adaptation error naming the problem."""
    with pytest.raises(CstarAdaptationError, match=expected):
        ComputeEnvironmentAdapter().adapt(content)


def test_adapter_reports_all_problems() -> None:
    """Verify every problem is listed in the one error."""
    with pytest.raises(CstarAdaptationError) as ex:
        ComputeEnvironmentAdapter().adapt({"launcher": "pbs", "bogus": 1})

    assert "pbs" in str(ex.value)
    assert "bogus" in str(ex.value)


def test_resolve_empty_is_default() -> None:
    """Verify an empty mapping resolves to the default environment."""
    assert resolve_compute_environment({}) == ComputeEnvironment()


def test_resolve_invalid_raises() -> None:
    """Verify an invalid mapping is not defaulted."""
    with pytest.raises(CstarAdaptationError):
        resolve_compute_environment({"bogus": 1})
