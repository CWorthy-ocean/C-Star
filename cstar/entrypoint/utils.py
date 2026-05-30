import typing as t

ARG_CLOBBER: t.Final[str] = "--clobber"
ARG_CLOBBER_HELP: t.Final[str] = (
    "Set this flag to remove any pre-existing files in the working directory."
)
ARG_DRY_RUN: t.Final[str] = "--dry-run"

ARG_DIRECTIVES_URI_LONG: t.Final[str] = "--directives"
ARG_DIRECTIVES_URI_SHORT: t.Final[str] = "-d"

ARG_LOGLEVEL_LONG: t.Final[str] = "--log-level"
ARG_LOGLEVEL_SHORT: t.Final[str] = "-l"
ARG_LOGLEVEL_HELP: t.Final[str] = "Set the logging level for C-Star."

ARG_OUTPUT_LONG: t.Final[str] = "--output"
ARG_OUTPUT_SHORT: t.Final[str] = "-o"

ARG_URI_LONG: t.Final[str] = "--blueprint-uri"
ARG_URI_SHORT: t.Final[str] = "-b"

ARG_VERBOSE: t.Final[str] = "--verbose"
