import typing as t

ARG_CLOBBER: t.Final[str] = "--clobber"
ARG_CLOBBER_HELP: t.Final[str] = (
    "Set this flag to remove any pre-existing files in the working directory."
)
ARG_CLOBBER_WORKPLAN_HELP: t.Final[str] = (
    "Name of a step whose prior state should be cleared and re-executed on "
    "rerun, or 'all' to clobber every step. Repeatable. Accepts the step "
    "name or its slugified form. This option is the only workplan-level "
    "clobber control; the CSTAR_CLOBBER_WORKING_DIR environment variable is "
    "ignored (with a warning) by workplan runs."
)
OPT_CLOBBER_ALL: t.Final[str] = "all"
"""Reserved `--clobber` value that selects every step in the workplan. It
shadows any step literally named `all`, which can therefore never be targeted
individually by name."""

ARG_DIRECTIVES_URI_LONG: t.Final[str] = "--directives"
ARG_DIRECTIVES_URI_SHORT: t.Final[str] = "-d"

ARG_DRY_RUN: t.Final[str] = "--dry-run"

ARG_LOGLEVEL_LONG: t.Final[str] = "--log-level"
ARG_LOGLEVEL_SHORT: t.Final[str] = "-l"
ARG_LOGLEVEL_HELP: t.Final[str] = "Set the logging level for C-Star."

ARG_NO_CACHE: t.Final[str] = "--no-cache"
ARG_NO_CACHE_HELP: t.Final[str] = (
    "Set this flag to force C-Star to acquire and replace any cached assets (e.g. datasets)"
)

ARG_OUTPUT_LONG: t.Final[str] = "--output"
ARG_OUTPUT_SHORT: t.Final[str] = "-o"

ARG_URI_LONG: t.Final[str] = "--blueprint-uri"
ARG_URI_SHORT: t.Final[str] = "-b"

ARG_VERBOSE: t.Final[str] = "--verbose"
ARG_VERBOSE_HELP: t.Final[str] = "Set this flag to print verbose CLI outputs."
