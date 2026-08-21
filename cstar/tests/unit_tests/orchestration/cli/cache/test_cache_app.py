"""Unit tests for the ``cstar cache`` command group itself."""

import importlib
import os
from collections.abc import Generator
from unittest import mock

import pytest
from typer.testing import CliRunner

import cstar.cli.cache
from cstar.base.env import FLAG_OFF, FLAG_ON
from cstar.base.feature import ENV_FF_CLI_MANAGE_CACHE

SUBCOMMANDS = ("clean", "promote", "show", "store")


@pytest.mark.parametrize("subcommand", SUBCOMMANDS)
def test_cli_cache_registers_its_subcommands(subcommand: str) -> None:
    """Every command is reachable once the feature is on.

    The group registers its subcommands at import time behind a feature flag,
    so a missing registration would surface only as a command that silently
    does not exist.

    Parameters
    ----------
    subcommand : str
        Command expected to be registered.
    """
    with mock.patch.dict(os.environ, {ENV_FF_CLI_MANAGE_CACHE: FLAG_ON}):
        module = importlib.reload(cstar.cli.cache)

    result = CliRunner().invoke(module.app, ["--help"], color=False)

    assert subcommand in result.stdout


def test_cli_cache_hides_its_subcommands_when_disabled() -> None:
    """With the flag off the group exists but offers nothing.

    That is the point of the flag: the command tree stays stable while the
    feature is still moving.
    """
    with mock.patch.dict(os.environ, {ENV_FF_CLI_MANAGE_CACHE: FLAG_OFF}):
        module = importlib.reload(cstar.cli.cache)

    # Typer refuses to build a command tree with nothing registered, which is
    # itself the assertion: with the flag off, the group has no subcommands.
    assert not module.app.registered_groups
    assert not module.app.registered_commands


@pytest.fixture(autouse=True)
def _restore_module() -> Generator[None]:
    """Reload the module afterwards so other tests see its real state."""
    yield
    importlib.reload(cstar.cli.cache)
