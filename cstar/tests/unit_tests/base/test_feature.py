import os
import unittest.mock as mock

import pytest

from cstar.base.env import FLAG_OFF, FLAG_ON, get_env_item
from cstar.base.feature import ENV_FF_DEBUG_BUILD_MODE, ENV_FF_SLURM_DISABLE_MT


def test_feature_debug_build_default() -> None:
    """Verify the default value for a feature flag."""
    key = ENV_FF_DEBUG_BUILD_MODE

    with mock.patch.dict(os.environ, {}, clear=True):
        item = get_env_item(key)

        assert item.default == FLAG_OFF


@pytest.mark.parametrize(
    ("key", "flag_value"),
    [
        (ENV_FF_DEBUG_BUILD_MODE, FLAG_ON),
        (ENV_FF_DEBUG_BUILD_MODE, FLAG_OFF),
        (ENV_FF_SLURM_DISABLE_MT, FLAG_ON),
        (ENV_FF_SLURM_DISABLE_MT, FLAG_OFF),
    ],
)
def test_feature_debug_build_set_externally(key: str, flag_value: bool) -> None:
    """Verify the external value for a feature flag overrides the default."""
    with mock.patch.dict(os.environ, {key: flag_value}, clear=True):
        item = get_env_item(key)

        assert item.value == flag_value
