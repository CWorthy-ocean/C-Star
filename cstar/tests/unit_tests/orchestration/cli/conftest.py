import os
import re
from collections.abc import Callable, Generator
from unittest import mock

import pytest

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
    """Fixture providing a helper that prepares CLI output for phrase matching."""
    return _flatten_cli_output


@pytest.fixture
def squash_cli_output() -> Callable[[str], str]:
    """Fixture providing a helper that prepares CLI output for path matching."""
    return _squash_cli_output
