import os
import typing as t

from cstar.base.env import FLAG_OFF, FLAG_ON, EnvVar

FF_PREFIX: t.Literal["CSTAR_FF_"] = "CSTAR_FF_"
"""Conventional prefix of environment variables for feature flags."""

_GROUP_FF: t.Final[str] = "Feature Flags"
"""Group name for feature flag environment variables in documentation."""

ENV_FF_DEVELOPER_MODE: t.Annotated[
    t.Literal["CSTAR_FF_DEVELOPER_MODE"],
    EnvVar(
        "Enable developer mode to enable all feature flags (not recommended).",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_DEVELOPER_MODE"
"""Enable developer mode to enable all feature flags (not recommended)."""

ENV_FF_CLI_ENV_SHOW: t.Annotated[
    t.Literal["CSTAR_FF_CLI_ENV_SHOW"],
    EnvVar(
        "Enable CLI for displaying environment configuration.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_CLI_ENV_SHOW"
"""Enable CLI for displaying environment configuration."""

ENV_FF_CLI_TEMPLATE_CREATE: t.Annotated[
    t.Literal["CSTAR_FF_CLI_TEMPLATE_CREATE"],
    EnvVar(
        "Enable CLI for creating blueprints and workplans from standard templates.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_CLI_TEMPLATE_CREATE"
"""Enable CLI for creating blueprints and workplans from standard templates."""

ENV_FF_CLI_WORKPLAN_COMPOSE: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_COMPOSE"],
    EnvVar(
        "Enable CLI for composing a workplan from pre-existing blueprints.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_COMPOSE"
"""Enable CLI for composing a workplan from pre-existing blueprints."""

ENV_FF_CLI_WORKPLAN_GEN: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_GEN"],
    EnvVar(
        "Enable CLI for generating a workplan from a directory of blueprints.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_GEN"
"""Enable CLI for generating a workplan from a directory of blueprints."""

ENV_FF_CLI_WORKPLAN_PLAN: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_PLAN"],
    EnvVar(
        "Enable CLI for generating the execution plan of a workplan.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_PLAN"
"""Enable CLI for generating the execution plan of a workplan."""

ENV_FF_CLI_WORKPLAN_STATUS: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_STATUS"],
    EnvVar(
        "Enable CLI for retrieving status about a workplan run.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_STATUS"
"""Enable CLI for retrieving status about a workplan run."""

ENV_FF_ORCH_TRX_TIMESPLIT: t.Annotated[
    t.Literal["CSTAR_FF_ORCH_TRX_TIMESPLIT"],
    EnvVar(
        "Enable automatic time-splitting of simulations.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_ORCH_TRX_TIMESPLIT"
"""Enable automatic time-splitting of simulations."""

ENV_FF_ORCH_TRX_OVERRIDE: t.Annotated[
    t.Literal["CSTAR_FF_ORCH_TRX_OVERRIDE"],
    EnvVar(
        "Enable automatic overrides to blueprints contained in a workplan.",
        _GROUP_FF,
        default=FLAG_OFF,
    ),
] = "CSTAR_FF_ORCH_TRX_OVERRIDE"
"""Enable automatic overrides to blueprints contained in a workplan."""


def is_feature_enabled(flag: str) -> bool:
    """Determine if an environment variable for a feature is set.

    Enable all development features via the `CSTAR_FF_DEVELOPER_MODE` flag.

    Parameters
    ----------
    flag : str
        The name of the feature flag to inspect.

        May take the full feature-flag form, such as `CSTAR_FF_<FLAG_NAME>` or
        pass only the <FLAG_NAME> and exclude the `CSTAR_FF_` prefix.
    """
    # developer mode enables all feature flags
    if os.getenv(ENV_FF_DEVELOPER_MODE, FLAG_OFF) == FLAG_ON:
        return True

    # enable omitting the CSTAR_FF_ prefix at the call-site
    if not flag.startswith(FF_PREFIX):
        flag = f"{FF_PREFIX}{flag}"

    # Enable hierarchical feature flag segments - Given a flag:
    #   CSTAR_FF_CLI_BLUEPRINT_CHECK
    # inspect environment for the segments:
    # - CSTAR_FF_CLI
    # - CSTAR_FF_CLI_BLUEPRINT
    # - CSTAR_FF_CLI_BLUEPRINT_CHECK
    flag_parts = flag.split("_")
    if len(flag_parts) > 2:
        for i in range(2, len(flag_parts)):
            segment_id = "_".join(flag_parts[:i])

            if os.getenv(segment_id, FLAG_OFF) == FLAG_ON:
                return True

    return os.getenv(flag, FLAG_OFF) == FLAG_ON
