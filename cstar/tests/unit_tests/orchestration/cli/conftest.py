"""Shared fixtures for CLI tests."""

import os
import re
from collections.abc import Callable, Generator
from unittest import mock

import pytest

from cstar.base.env import (
    ENV_CSTAR_CLI_DRY_RUN,
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_DISABLE_MIGRATION,
)

_ANSI_CODES = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


@pytest.fixture(autouse=True)
def disable_cli_color() -> Generator[None]:
    """Force plain-text CLI output from typer/rich for every CLI test.

    CI agents enable color output (typer forces a color terminal when
    `GITHUB_ACTIONS`, `FORCE_COLOR`, or `PY_COLORS` is set), which embeds ANSI
    escape codes in captured CLI output and breaks message assertions.
    """
    with mock.patch.dict(os.environ, {"NO_COLOR": "1"}):
        for var in ("FORCE_COLOR", "PY_COLORS", "GITHUB_ACTIONS"):
            os.environ.pop(var, None)
        yield


def _flatten_cli_output(text: str) -> str:
    """Prepare CLI output for phrase matching: remove ANSI color codes (CI
    environments force color even under `color=False`) and rich panel
    decoration, and collapse whitespace to single spaces.
    """
    return " ".join(_ANSI_CODES.sub("", text).replace("│", " ").split())


def _squash_cli_output(text: str) -> str:
    """Prepare CLI output for path matching: additionally remove ALL
    whitespace, since rich wraps long paths at arbitrary points.
    """
    return "".join(_flatten_cli_output(text).split())


@pytest.fixture
def flatten_cli_output() -> Callable[[str], str]:
    """Fixture providing a helper that prepares CLI output for phrase matching.

    `rich` wraps long paths at the terminal width, so normalize whitespace
    before matching
    """
    return _flatten_cli_output


@pytest.fixture
def squash_cli_output() -> Callable[[str], str]:
    """Fixture providing a helper that prepares CLI output for path matching."""
    return _squash_cli_output


@pytest.fixture(autouse=True)
def clean_flag_env() -> Generator[None]:
    """Remove CLI flag environment variables that may leak in from the shell.

    The CLI reads these flags via `envvar=` and the environment, so a value
    exported by a prior CLI run (`set_flag` never unsets them) would silently
    change test behavior. Tests opt back in with their own `mock.patch.dict`.
    """
    with mock.patch.dict(os.environ):
        for flag in (
            ENV_CSTAR_CLI_DRY_RUN,
            ENV_CSTAR_CLI_VERBOSE,
            ENV_CSTAR_CLOBBER_WORKING_DIR,
            ENV_CSTAR_DISABLE_MIGRATION,
        ):
            os.environ.pop(flag, None)
        yield
