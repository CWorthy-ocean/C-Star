import os
from pathlib import Path
from unittest import mock
from unittest.mock import PropertyMock, patch

import pytest

from cstar.base.env import ENV_CSTAR_SCRATCH_DIRS, hpc_data_directory

SCRATCH_ENV_VARS = ("SCRATCH", "SCRATCH_DIR", "LOCAL_SCRATCH")


@pytest.fixture
def clear_scratch_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure no scratch-locating env vars are set, regardless of what the host
    running the test suite happens to have exported.
    """
    for var in (*SCRATCH_ENV_VARS, ENV_CSTAR_SCRATCH_DIRS):
        monkeypatch.delenv(var, raising=False)


def test_hpc_data_directory_uses_scratch_env_var(
    clear_scratch_env_vars: None,
) -> None:
    """Verify that `hpc_data_directory` returns the value of the first populated
    scratch env var named in `CSTAR_SCRATCH_DIRS`, without consulting the system
    context.
    """
    with mock.patch.dict(os.environ, {"SCRATCH": "/mock/scratch/path"}):
        assert hpc_data_directory() == "/mock/scratch/path"


def test_hpc_data_directory_falls_back_to_bouchet_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clear_scratch_env_vars: None,
) -> None:
    """Verify that `hpc_data_directory` falls back to the active system context's
    `scratch_directory()` heuristic when no scratch env var is populated and the
    active context is Bouchet.
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    (fake_home / "scratch_pi_x").mkdir()

    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setenv("USER", "mock_user")

    with patch(
        "cstar.system.manager.HostNameEvaluator.name",
        new_callable=PropertyMock,
        return_value="bouchet",
    ):
        result = hpc_data_directory()

    assert result == (fake_home / "scratch_pi_x" / "mock_user").as_posix()


def test_hpc_data_directory_returns_none_on_context_error(
    clear_scratch_env_vars: None,
) -> None:
    """Verify that `hpc_data_directory` returns `None` (rather than raising) when
    no scratch env var is populated and the active system context cannot be
    determined.
    """
    with patch(
        "cstar.system.manager.HostNameEvaluator.name",
        new_callable=PropertyMock,
        return_value="invalid-name",
    ):
        assert hpc_data_directory() is None


def test_hpc_data_directory_returns_none_when_context_has_no_heuristic(
    clear_scratch_env_vars: None,
) -> None:
    """Verify that `hpc_data_directory` returns `None` when no scratch env var is
    populated and the active system context's `scratch_directory()` returns
    `None` (i.e. the default `SystemContext` behavior for systems without a
    machine-specific heuristic).
    """
    with patch(
        "cstar.system.manager.HostNameEvaluator.name",
        new_callable=PropertyMock,
        return_value="darwin_arm64",
    ):
        assert hpc_data_directory() is None
