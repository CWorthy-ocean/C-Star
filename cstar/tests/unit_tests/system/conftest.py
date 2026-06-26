import os
from collections.abc import Generator
from unittest.mock import patch

import pytest

from cstar.system.environment import LmodEnvSettings


@pytest.fixture
def lmod_settings_keys() -> list[str]:
    """Configure environment to include all lmod variables."""
    return [
        "CMD",
        "DIR",
        "PKG",
        "ROOT",
        "SYSHOST",
        "SYSTEM_DEFAULT_MODULES",
        "SYSTEM_NAME",
        "VERSION",
    ]


@pytest.fixture
def env_full_lmod(lmod_settings_keys: list[str]) -> Generator[dict[str, str]]:
    """Configure environment to include all lmod variables."""
    mapping = {LmodEnvSettings.variable(k): f"{k}-value" for k in lmod_settings_keys}

    with patch.dict(os.environ, mapping):
        yield mapping


@pytest.fixture
def env_full_strippable_lmod(
    lmod_settings_keys: list[str],
) -> Generator[dict[str, str]]:
    """Configure environment to include all lmod variables with whitespace that should be stripped."""
    mapping = {LmodEnvSettings.variable(k): f" {k}-value " for k in lmod_settings_keys}

    with patch.dict(os.environ, mapping):
        yield mapping


@pytest.fixture
def env_empty_lmod(lmod_settings_keys: list[str]) -> Generator[dict[str, str]]:
    """Configure environment to include all lmod variables with empty-string values."""
    mapping = {LmodEnvSettings.variable(k): "" for k in lmod_settings_keys}

    with patch.dict(os.environ, mapping):
        yield mapping


@pytest.fixture
def env_clear_lmod(lmod_settings_keys: list[str]) -> Generator[dict[str, str]]:
    """Configure environment to have no lmod variables."""
    keys = {LmodEnvSettings.variable(k) for k in lmod_settings_keys}
    mapping = {k: v for k, v in os.environ.items() if k not in keys}

    with patch.dict(os.environ, mapping, clear=True):
        yield mapping
