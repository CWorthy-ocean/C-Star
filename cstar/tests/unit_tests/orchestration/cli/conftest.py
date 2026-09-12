"""Shared fixtures for CLI tests."""

import os
from collections.abc import Generator
from unittest import mock

import pytest

from cstar.base.env import (
    ENV_CSTAR_CLI_DRY_RUN,
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_DISABLE_MIGRATION,
)


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
