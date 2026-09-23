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
ARG_RESUME: t.Final[str] = "--resume"
ARG_RESUME_HELP: t.Final[str] = (
    "Resume the prior attempt found in the blueprint's working directory instead "
    "of starting fresh: previously staged inputs, cloned codebases and the compiled "
    "executable are reused and the run continues from the last usable restart. "
    "Only applications that declare themselves resumable accept this flag; it "
    "cannot be combined with --clobber."
)
ARG_RESUME_WORKPLAN_HELP: t.Final[str] = (
    "Re-enter a prior run and resume its failed steps in place. Identify the run "
    "with --run-id, or with the workplan path it was started from (the run-id is "
    "derived from the workplan name as on the first run; the file must be "
    "unchanged since). Failed steps whose application supports resume continue "
    "from their last usable restart; failed steps of other applications are re-run "
    "from scratch with a warning; completed steps are reused. Cannot be combined "
    "with --clobber, --var or --varfile."
)
OPT_CLOBBER_ALL: t.Final[str] = "all"
"""Reserved `--clobber` value that selects every step in the workplan. It
shadows any step literally named `all`, which can therefore never be targeted
individually by name."""

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

ARG_VAR_LONG: t.Final[str] = "--var"
ARG_VAR_SHORT: t.Final[str] = "-v"
ARG_VARFILE_LONG: t.Final[str] = "--varfile"
ARG_VARFILE_SHORT: t.Final[str] = "-f"

ARG_VERBOSE: t.Final[str] = "--verbose"
ARG_VERBOSE_HELP: t.Final[str] = "Set this flag to print verbose CLI outputs."

ARG_SIZE: t.Final[str] = "--size"
ARG_SIZE_HELP: t.Final[str] = "Set this flag to refresh disk-usage statistics."
