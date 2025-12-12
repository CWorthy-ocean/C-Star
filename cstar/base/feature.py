import os
from typing import Literal

FLAG_DEVELOPER_MODE: Literal["CSTAR_FF_DEVELOPER_MODE"] = "CSTAR_FF_DEVELOPER_MODE"
FLAG_PREFIX: Literal["CSTAR_FF_"] = "CSTAR_FF_"
FF_ON: Literal["1"] = "1"
FF_OFF: Literal["0"] = "0"


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
    if os.getenv(FLAG_DEVELOPER_MODE, FF_OFF) == FF_ON:
        return True

    # enable omitting the CSTAR_FF_ prefix at the call-site
    if not flag.startswith(FLAG_PREFIX):
        flag = f"{FLAG_PREFIX}{flag}"

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

            if os.getenv(segment_id, FF_OFF) == FF_ON:
                return True

    return os.getenv(flag, FF_OFF) == FF_ON
