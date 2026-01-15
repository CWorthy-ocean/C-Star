# ruff: noqa: S101

import os
from pathlib import Path
from unittest import mock

from cstar.base.utils import (
    DEFAULT_CSTAR_HOME,
    DEFAULT_OUTPUT_DIR,
    ENV_CSTAR_OUTDIR,
    get_output_dir,
)


def test_get_outdir(tmp_path: Path) -> None:
    """Verify the default outdir is returned if no environment configuration
    is available.
    """
    with mock.patch.dict(os.environ, {}, clear=True):
        output_dir = get_output_dir()

    od = (Path(DEFAULT_CSTAR_HOME) / DEFAULT_OUTPUT_DIR).expanduser().resolve()
    assert str(output_dir) == str(od)


def test_get_outdir_with_override(tmp_path: Path) -> None:
    """Verify the default path is not returned when an override is supplied
    to get_outdir.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    with mock.patch.dict(os.environ, {}, clear=True):
        output_dir = get_output_dir(tmp_path)

    assert str(output_dir) == str(tmp_path)


def test_get_outdir_with_env_var(tmp_path: Path) -> None:
    """Verify the default path is not returned when the environment has
    an override specified.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    path = tmp_path / "other-place"

    with mock.patch.dict(os.environ, {ENV_CSTAR_OUTDIR: path.as_posix()}, clear=True):
        output_dir = get_output_dir(path)

    assert str(output_dir) == str(path)


def test_get_outdir_parameter_precedence(tmp_path: Path) -> None:
    """Verify the argument passed to get_cstar_outdir overrides all other values.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    path1 = tmp_path / "place1"
    path2 = tmp_path / "place2"

    with mock.patch.dict(os.environ, {ENV_CSTAR_OUTDIR: path1.as_posix()}, clear=True):
        output_dir = get_output_dir(path2)

    assert str(output_dir) == str(path2)


def test_get_outdir_env_precedence(tmp_path: Path) -> None:
    """Verify the environment variable takes precedence over the default value

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs.
    """
    path1 = tmp_path / "place1"

    with mock.patch.dict(os.environ, {ENV_CSTAR_OUTDIR: path1.as_posix()}, clear=True):
        output_dir = get_output_dir()

    assert str(output_dir) == str(path1)
