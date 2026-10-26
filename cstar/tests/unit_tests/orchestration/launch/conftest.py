"""Fixtures shared by the launcher tests."""

from pathlib import Path

import pytest

from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize


@pytest.fixture
def instant_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give a cancelled prior attempt no time to end, and wait for it not at all."""
    monkeypatch.setattr("cstar.orchestration.launch.common.CANCEL_TIMEOUT", 0)
    monkeypatch.setattr("cstar.orchestration.launch.common.CANCEL_POLL_DELAY", 0)


@pytest.fixture
def single_step(wp_templates_dir: Path, read_yaml_intercept: None) -> LiveStep:
    """A step whose blueprint exists."""
    return LiveStep.from_step(
        deserialize(wp_templates_dir / "single_step.yaml", Workplan).steps[0]
    )
