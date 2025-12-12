import os


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
    developer_mode = os.getenv("CSTAR_FF_DEVELOPER", "0")
    if developer_mode == "1":
        return True

    # enable omitting the CSTAR_FF_ prefix at the call-site
    if not flag.startswith("CSTAR_FF_"):
        flag = f"CSTAR_FF_{flag}"

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

            if os.getenv(segment_id, "0") == "1":
                return True

    return os.getenv(flag, "0") == "1"
