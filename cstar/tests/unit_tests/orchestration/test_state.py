"""Tests for the sentinel repository."""

from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration import state as state_module
from cstar.orchestration.orchestration import ProcessHandle, Status
from cstar.orchestration.serialization import PersistenceMode
from cstar.orchestration.state import StateRepository


@pytest.fixture
def state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the state directory at a scratch location."""
    monkeypatch.setenv("CSTAR_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("CSTAR_RUNID", "atomic-run")
    return tmp_path / "state"


async def test_put_sentinel_never_exposes_a_partial_file(state_home: Path) -> None:
    """Verify a sentinel is written beside its path and renamed into place.

    The local proxy script rewrites the sentinel's status line as soon as it
    starts; if the orchestrator truncated the sentinel in place, that rewrite
    could copy an empty file back over it. The destination must therefore hold
    either the previous complete sentinel or the new one, never a partial one.
    """
    repo = StateRepository()
    handle = ProcessHandle(
        pid="1", name="Step A", run_id="atomic-run", status=Status.Submitted
    )

    first = await repo.put_sentinel(handle)
    assert first is not None
    previous = first.read_text()
    assert "status:" in previous

    observed: list[str] = []
    real_serialize = state_module.serialize

    def spying_serialize(
        path: Path, model: ProcessHandle, mode: PersistenceMode = PersistenceMode.yaml
    ) -> int:
        # the destination is untouched while the new sentinel is being written
        observed.append(first.read_text())
        return real_serialize(path, model, mode=mode)

    handle.status = Status.Running
    with mock.patch.object(state_module, "serialize", spying_serialize):
        second = await repo.put_sentinel(handle)

    assert second == first
    assert observed == [previous]
    assert f"status: {Status.Running.value}" in first.read_text()
    # no temporary files are left behind, and none match the sentinel glob
    assert sorted(p.name for p in first.parent.iterdir()) == sorted(
        [first.name, first.with_suffix(".lock").name]
    )
