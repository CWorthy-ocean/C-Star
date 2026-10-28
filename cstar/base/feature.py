import os
import typing as t

from cstar.base.env import FLAG_OFF, FLAG_ON, GROUP_FF, EnvVar

FF_PREFIX: t.Literal["CSTAR_FF_"] = "CSTAR_FF_"
"""Conventional prefix of environment variables for feature flags."""

ENV_FF_DEVELOPER_MODE: t.Annotated[
    t.Literal["CSTAR_FF_DEVELOPER_MODE"],
    EnvVar(
        "Enable developer-only commands, such as the schema generators. It does "
        "not enable any other feature flag.",
        GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_DEVELOPER_MODE"
"""Enable developer-only commands, such as the schema generators. It does not
enable any other feature flag."""


ENV_FF_ORCH_TRX_TIMESPLIT: t.Annotated[
    t.Literal["CSTAR_FF_ORCH_TRX_TIMESPLIT"],
    EnvVar(
        "Enable automatic time-splitting of simulations.",
        GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_ORCH_TRX_TIMESPLIT"
"""Enable automatic time-splitting of simulations."""

ENV_FF_ORCH_TRX_TIMESPLIT_LONGNAME: t.Annotated[
    t.Literal["CSTAR_FF_ORCH_TRX_TIMESPLIT_LONGNAME"],
    EnvVar(
        "Enable long name generation during time-splitting.",
        GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_ORCH_TRX_TIMESPLIT_LONGNAME"
"""Enable long name generation during time-splitting."""


ENV_FF_ORCH_TRX_TIMESPLIT_FREQ: t.Annotated[
    t.Literal["CSTAR_FF_ORCH_TRX_TIMESPLIT_FREQ"],
    EnvVar(
        "Time-splitting frequency (e.g. Daily, Weekly, Monthly). Read only when "
        "CSTAR_FF_ORCH_TRX_TIMESPLIT is on; developer mode does not set it.",
        GROUP_FF,
        default="Monthly",
    ),
] = "CSTAR_FF_ORCH_TRX_TIMESPLIT_FREQ"
"""Time-splitting frequency (e.g. Daily, Weekly, Monthly), read only when time-splitting is on."""


def is_flag_enabled(flag: str) -> bool:
    """Determine if a boolean environment varible is enabled.

    Parameters
    ----------
    flag : str
        The name of an environment variable to check.

    Returns
    -------
    bool
    """
    return os.getenv(flag, FLAG_OFF) == FLAG_ON


def is_feature_enabled(flag: str) -> bool:
    """Determine if an environment variable for a feature is set.

    Each feature is enabled by its own flag or by an enclosing segment of it;
    `CSTAR_FF_DEVELOPER_MODE` does not enable other features.

    Parameters
    ----------
    flag : str
        The name of the feature flag to inspect.

        May take the full feature-flag form, such as `CSTAR_FF_<FLAG_NAME>` or
        pass only the <FLAG_NAME> and exclude the `CSTAR_FF_` prefix.
    """
    # enable omitting the CSTAR_FF_ prefix at the call-site
    if not flag.startswith(FF_PREFIX):
        flag = f"{FF_PREFIX}{flag}"

    # Enable hierarchical feature flag segments - Given a flag:
    #   CSTAR_FF_ORCH_TRX_TIMESPLIT
    # inspect environment for the segments:
    # - CSTAR_FF_ORCH
    # - CSTAR_FF_ORCH_TRX
    # - CSTAR_FF_ORCH_TRX_TIMESPLIT
    flag_parts = flag.split("_")
    if len(flag_parts) > 2:
        for i in range(2, len(flag_parts)):
            segment_id = "_".join(flag_parts[:i])

            if is_flag_enabled(segment_id):
                return True

    return is_flag_enabled(flag)
