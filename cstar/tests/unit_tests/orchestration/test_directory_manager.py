import os
import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import (
    ENV_CSTAR_CACHE_HOME,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_STATE_HOME,
    get_env_item,
)
from cstar.execution.file_system import DirectoryManager as DirMgr


@pytest.mark.parametrize(
    ("fn_under_test", "default_value"),
    [
        (DirMgr.cache_home, get_env_item(ENV_CSTAR_CACHE_HOME).default),
        (DirMgr.config_home, get_env_item(ENV_CSTAR_CONFIG_HOME).default),
        (DirMgr.data_home, get_env_item(ENV_CSTAR_DATA_HOME).default),
        (DirMgr.state_home, get_env_item(ENV_CSTAR_STATE_HOME).default),
    ],
)
def test_directory_mgr_cachedir_no_config(
    fn_under_test: t.Callable[[], Path], default_value: str
) -> None:
    """Verify the default state directory is returned if no environment config is set."""
    expected_path = (Path(default_value) / "cstar").expanduser().resolve()

    with mock.patch.dict(os.environ, {}, clear=True):
        output_dir = fn_under_test()

    assert output_dir == expected_path


@pytest.mark.parametrize(
    ("fn_under_test", "xdg_var", "xdg_value"),
    [
        (DirMgr.cache_home, "XDG_CACHE_HOME", "/foo/bar"),
        (DirMgr.config_home, "XDG_CONFIG_HOME", "~/foo/baz"),
        (DirMgr.data_home, "XDG_DATA_HOME", "/foo/beep"),
        (DirMgr.state_home, "XDG_STATE_HOME", "~/foo/boop"),
    ],
)
def test_directory_mgr_cachedir_xdg_config(
    fn_under_test: t.Callable[[], Path],
    xdg_var: str,
    xdg_value: str,
) -> None:
    """Verify the XDG config is used instead of returning defaults when it is set."""
    # expect the output to be the base path from the XDG setting with a cstar subdir
    xdg_path = Path(xdg_value) / "cstar"
    expected_path = xdg_path.expanduser().resolve()

    with mock.patch.dict(os.environ, {xdg_var: xdg_value}, clear=True):
        output_dir = fn_under_test()

    assert output_dir == expected_path


@pytest.mark.parametrize(
    ("fn_under_test", "xdg_var", "xdg_value", "cstar_var", "cstar_val"),
    [
        (
            DirMgr.cache_home,
            "XDG_CACHE_HOME",
            "/foo/bar",
            ENV_CSTAR_CACHE_HOME,
            "/my-cache/cstar-cache",
        ),
        (
            DirMgr.config_home,
            "XDG_CONFIG_HOME",
            "/foo/baz",
            ENV_CSTAR_CONFIG_HOME,
            "~/my-config/cstar-config",
        ),
        (
            DirMgr.data_home,
            "XDG_DATA_HOME",
            "/foo/beep",
            ENV_CSTAR_DATA_HOME,
            "/my-data/cstar-data",
        ),
        (
            DirMgr.state_home,
            "XDG_STATE_HOME",
            "/foo/boop",
            ENV_CSTAR_STATE_HOME,
            "~/my-state/cstar-state",
        ),
    ],
)
def test_directory_mgr_cachedir_cstar_config(
    fn_under_test: t.Callable[[], Path],
    xdg_var: str,
    xdg_value: str,
    cstar_var: str,
    cstar_val: str,
) -> None:
    """Verify the XDG config is ignored when the CSTAR_XXX_HOME variables are set."""
    cstar_path = Path(cstar_val)

    # the cstar env vars are considered authoritative and no subdir is appended
    expected_path = cstar_path.expanduser().resolve()

    with mock.patch.dict(
        os.environ, {xdg_var: xdg_value, cstar_var: cstar_val}, clear=True
    ):
        output_dir = fn_under_test()

    assert output_dir == expected_path


@pytest.mark.parametrize(
    ("scratch_var", "scratch_val", "xdg_var", "xdg_value"),
    [
        ("SCRATCH", "~/scratch", "XDG_DATA_HOME", "/foo/beep"),
        ("SCRATCH_DIR", "/scratch-x", "XDG_DATA_HOME", "/foo/beep"),
        ("LOCAL_SCRATCH", "/local-scratch", "XDG_DATA_HOME", "/foo/beep"),
    ],
)
def test_directory_mgr_datadir_hpc_override(
    scratch_var: str, scratch_val: str, xdg_var: str, xdg_value: str
) -> None:
    """Verify the XDG data directory is overridden when HPC-specific scratch
    paths are detected.
    """
    hpc_path = Path(scratch_val)

    # expect the output to be the base path from the hpc override with a cstar subdir
    expected_path = (hpc_path / "cstar").expanduser().resolve()

    with mock.patch.dict(
        os.environ, {xdg_var: xdg_value, scratch_var: scratch_val}, clear=True
    ):
        output_dir = DirMgr.data_home()

    assert output_dir == expected_path


def test_directory_mgr_datadir_hpc_override_with_cstar_var(tmp_path: Path) -> None:
    """Verify the data directory from the c-star env var takes precedence over both
    automatic scratch paths and xdg values.
    """
    cstar_val = "/my-data/cstar-data"
    cstar_path = Path(cstar_val)

    # the cstar env vars are considered authoritative and no subdir is appended
    expected_path = cstar_path.expanduser().resolve()

    with mock.patch.dict(
        os.environ,
        {
            "XDG_DATA_HOME": (tmp_path / "xdg-data-home").as_posix(),
            "SCRATCH": "/scratch",
            ENV_CSTAR_DATA_HOME: cstar_val,
        },
        clear=True,
    ):
        output_dir = DirMgr.data_home()

    assert output_dir == expected_path
