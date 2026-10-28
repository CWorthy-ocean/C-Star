import os
import unittest.mock as mock

import pytest

from cstar.applications.ucla_roms.transforms import RomsTimeSplitter
from cstar.base.env import FLAG_OFF, FLAG_ON, get_env_item
from cstar.base.feature import (
    ENV_FF_DEVELOPER_MODE,
    ENV_FF_ORCH_TRX_TIMESPLIT,
    ENV_FF_ORCH_TRX_TIMESPLIT_LONGNAME,
    is_feature_enabled,
)


def test_feature_default() -> None:
    """Verify the default value for a feature flag."""
    key = ENV_FF_DEVELOPER_MODE

    with mock.patch.dict(os.environ, {}, clear=True):
        item = get_env_item(key)

        assert item.default == FLAG_OFF


@pytest.mark.parametrize(
    ("key", "flag_value"),
    [
        (ENV_FF_DEVELOPER_MODE, FLAG_ON),
        (ENV_FF_DEVELOPER_MODE, FLAG_OFF),
        (ENV_FF_ORCH_TRX_TIMESPLIT, FLAG_ON),
        (ENV_FF_ORCH_TRX_TIMESPLIT, FLAG_OFF),
    ],
)
def test_feature_set_externally(key: str, flag_value: bool) -> None:
    """Verify the external value for a feature flag overrides the default."""
    with mock.patch.dict(os.environ, {key: flag_value}, clear=True):
        item = get_env_item(key)

        assert item.value == flag_value


@pytest.mark.parametrize(
    ("env", "flag", "expected"),
    [
        ({}, "CSTAR_FF_AREA_TOOL_ACTION", False),
        ({"CSTAR_FF_AREA_TOOL_ACTION": FLAG_ON}, "CSTAR_FF_AREA_TOOL_ACTION", True),
        ({"CSTAR_FF_AREA_TOOL_ACTION": FLAG_OFF}, "CSTAR_FF_AREA_TOOL_ACTION", False),
        # the prefix may be omitted at the call-site
        ({"CSTAR_FF_AREA_TOOL_ACTION": FLAG_ON}, "AREA_TOOL_ACTION", True),
        # an enclosing segment enables every flag beneath it...
        ({"CSTAR_FF_AREA": FLAG_ON}, "CSTAR_FF_AREA_TOOL_ACTION", True),
        ({"CSTAR_FF_AREA_TOOL": FLAG_ON}, "CSTAR_FF_AREA_TOOL_ACTION", True),
        # ...but not a sibling or a longer flag
        ({"CSTAR_FF_AREA_OTHER": FLAG_ON}, "CSTAR_FF_AREA_TOOL_ACTION", False),
        ({"CSTAR_FF_AREA_TOOL_ACTION": FLAG_ON}, "CSTAR_FF_AREA_TOOL", False),
        # developer mode enables only itself
        ({ENV_FF_DEVELOPER_MODE: FLAG_ON}, "CSTAR_FF_AREA_TOOL_ACTION", False),
        ({ENV_FF_DEVELOPER_MODE: FLAG_ON}, ENV_FF_DEVELOPER_MODE, True),
        ({ENV_FF_DEVELOPER_MODE: FLAG_OFF}, ENV_FF_DEVELOPER_MODE, False),
    ],
)
def test_is_feature_enabled(env: dict[str, str], flag: str, expected: bool) -> None:
    """Verify feature flags honour the prefix, hierarchical segments and developer mode."""
    with mock.patch.dict(os.environ, env, clear=True):
        assert is_feature_enabled(flag) is expected


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, False),
        ({ENV_FF_DEVELOPER_MODE: FLAG_ON}, False),
        ({ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_ON}, True),
        ({ENV_FF_DEVELOPER_MODE: FLAG_ON, ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_ON}, True),
    ],
)
def test_time_splitting_needs_its_own_flag(env: dict[str, str], expected: bool) -> None:
    """Verify developer mode does not turn on time-splitting or its long names;
    only the time-splitting flag does (it encloses the long-name flag).
    """
    with mock.patch.dict(os.environ, env, clear=True):
        assert RomsTimeSplitter.is_active() is expected
        assert is_feature_enabled(ENV_FF_ORCH_TRX_TIMESPLIT_LONGNAME) is expected
