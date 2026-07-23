# ruff: noqa: S101

import typing as t
from pathlib import Path

import pytest

from cstar.orchestration.launch.slurm import resolve_cpus
from cstar.orchestration.orchestration import LiveStep


@pytest.fixture
def deferred_live_step(tmp_path: Path) -> LiveStep:
    """Create a LiveStep whose blueprint is deferred to an upstream step.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs

    Returns
    -------
    LiveStep
    """
    return LiveStep.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer"},
            "depends_on": ["producer"],
            "working_dir": tmp_path / "consumer",
        },
    )


@pytest.mark.parametrize(
    ("cpus_override", "expected"),
    [
        (4, 4),
        (4.0, 4),
        ("16", 16),
    ],
)
def test_resolve_cpus_compute_override_wins(
    deferred_live_step: LiveStep,
    cpus_override: t.Any,
    expected: int,
) -> None:
    """Verify an explicit compute_overrides declaration takes precedence.

    Parameters
    ----------
    deferred_live_step : LiveStep
        A step whose blueprint is deferred to an upstream step.
    cpus_override : t.Any
        The declared cpus value from the workplan.
    expected : int
        The expected resolved cpu count.
    """
    step = LiveStep.from_step(
        deferred_live_step,
        update={"compute_overrides": {"cpus": cpus_override}},
    )

    assert resolve_cpus(step) == expected


def test_resolve_cpus_deferred_default(deferred_live_step: LiveStep) -> None:
    """Verify a deferred step with no declaration defaults to a single cpu.

    Parameters
    ----------
    deferred_live_step : LiveStep
        A step whose blueprint is deferred to an upstream step.
    """
    assert deferred_live_step.blueprint is None
    assert resolve_cpus(deferred_live_step) == 1


def test_resolve_cpus_from_blueprint(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify cpus are derived from the blueprint when no override is declared.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="concrete",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "concrete",
    )

    blueprint = step.blueprint
    assert blueprint is not None
    assert resolve_cpus(step) == blueprint.cpus_needed


def test_resolve_cpus_compute_override_beats_blueprint(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify the declared cpus win even when the blueprint is resolvable.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    hello_world_bp_path : Path
        Fixture returning the path to a hello-world blueprint file.
    """
    step = LiveStep(
        name="concrete",
        application="hello_world",
        blueprint=hello_world_bp_path.as_posix(),
        working_dir=tmp_path / "concrete",
        compute_overrides={"cpus": 8},
    )

    assert resolve_cpus(step) == 8
