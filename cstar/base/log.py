import enum
import logging
import sys
import time
import typing as t
from pathlib import Path

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] - %(filename)s:%(lineno)d - %(message)s"
)
TRACE_LOG_LEVEL: t.Final[int] = 5
TRACE_LOG_NAME: t.Final[str] = "TRACE"


class LogLevelChoices(enum.StrEnum):
    """Log levels for C-Star. Used by CLI and entrypoints to display and validate
    a set of fixed choices.
    """

    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @classmethod
    def _missing_(cls, value: object) -> "LogLevelChoices | None":
        """Match a level name in any case, ignoring surrounding whitespace."""
        if isinstance(value, str):
            return cls.__members__.get(value.strip().upper())
        return None


class TraceLogger(logging.Logger):
    """A customized logger type that offers a `trace` method used to emit
    log entries that occur too frequently for debug level.
    """

    def trace(
        self: logging.Logger, message: str, *args: t.Any, **kwargs: t.Any
    ) -> None:
        """Emit a log entry at the `TRACE` log level."""
        if self.isEnabledFor(TRACE_LOG_LEVEL):
            self._log(TRACE_LOG_LEVEL, message, args, **kwargs)


logging.addLevelName(TRACE_LOG_LEVEL, TRACE_LOG_NAME)
logging.setLoggerClass(TraceLogger)


def make_formatter(fmt: str = DEFAULT_LOG_FORMAT) -> logging.Formatter:
    """Create a formatter that stamps log entries in local time.

    Some dependencies (copernicusmarine, arcosparse) set the class-wide
    `logging.Formatter.converter` to `time.gmtime` when imported, which would
    switch every C-Star timestamp to UTC partway through a process. Pinning the
    converter on the instance keeps C-Star's own entries in local time.

    Parameters
    ----------
    fmt : str
        The log format string.

    Returns
    -------
    logging.Formatter
    """
    formatter = logging.Formatter(fmt)
    formatter.converter = time.localtime
    return formatter


def register_file_handler(
    logger: logging.Logger,
    filename: str | Path | None,
    level: int = DEFAULT_LOG_LEVEL,
    fmt: str = DEFAULT_LOG_FORMAT,
) -> logging.FileHandler:
    """Register a file handler on the logger.

    Requests to add duplicate file handlers are ignored and the level will
    not be adjusted for a pre-existing handler.

    Parameters
    ----------
    logger : logging.Logger
        The logger to modify
    filename : str or Path or None
        The desired log file path
    level : int
        The log level for a new handler
    fmt : str
        The log format to apply to the handler.

    Returns
    -------
    logging.FileHandler
        The registered file handler instance or None if unable to find or
        register a handler.

    Raises
    ------
    ValueError
        If the file handler cannot be registered due to a malformed input
    """
    file_path: Path | None = None

    if isinstance(filename, Path):
        file_path = filename.resolve()
    elif isinstance(filename, str) and filename.strip():
        file_path = Path(filename).resolve()

    if not file_path:
        raise ValueError("No log filehandler was added.")

    file_path.parent.mkdir(parents=True, exist_ok=True)

    existing_fh = next(
        (
            h
            for h in logger.handlers
            if isinstance(h, logging.FileHandler)
            and Path(h.baseFilename).resolve() == file_path
        ),
        None,
    )
    if not existing_fh:
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(level)
        file_handler.setFormatter(make_formatter(fmt))
        logger.addHandler(file_handler)
        logger.info(f"FileHandler registered to write to: {file_path}")
        existing_fh = file_handler
    elif existing_fh.level != level:
        logger.debug(f"FileHandler is already set to level: {existing_fh.level}")

    return existing_fh


class _StdoutHandler(logging.StreamHandler[t.TextIO]):
    """The STDOUT handler `get_logger` installs for records below WARNING."""


def _reaches_stdout_handler(logger: logging.Logger) -> bool:
    """Return whether records from the logger reach a C-Star STDOUT handler.

    Walks the logger and its non-root ancestors along the propagation chain.
    The logger's own `propagate` is ignored because `get_logger` disables it
    temporarily while it sets up handlers.
    """
    node: logging.Logger | None = logger
    while node is not None and node is not logging.root:
        if any(isinstance(h, _StdoutHandler) for h in node.handlers):
            return True
        if node is not logger and not node.propagate:
            return False
        node = node.parent
    return False


def get_logger(
    name: str | None = None,
    level: int | None = None,
    fmt: str | None = None,
    filename: str | None | Path = None,
) -> TraceLogger:
    """Get a logger instance with the specified name.

    Parameters
    ----------
    name: str
        The name of the logger.
    level: int
        The minimum log level to write.
    fmt: str
        The log format string.
    filename: str | None
        A file path where logs will be written.

    Returns
    -------
    logging.Logger
        A logger instance with the specified name.
    """
    fmt = fmt or DEFAULT_LOG_FORMAT

    level = level or parse_log_level_name(get_env_item(ENV_CSTAR_LOG_LEVEL).value)

    logger = logging.getLogger(name)

    # Prevent propagation during setting up handlers
    logger.propagate = False

    logger.setLevel(level)
    formatter = make_formatter(fmt)

    # Set up root logger if not already configured
    root = logging.getLogger()

    if not root.hasHandlers():
        stderr_handler = logging.StreamHandler()
        stderr_handler.setFormatter(make_formatter())
        logging.basicConfig(level=logging.WARNING, handlers=[stderr_handler])

    # Ensure root handlers only handle WARNING and higher
    for handler in root.handlers:
        handler.setLevel(logging.WARNING)

    # Create specific STDOUT handler for INFO and lower, unless an ancestor's
    # handler already prints this logger's records (e.g. a LoggingMixin class
    # logger under its module's logger):
    if logger is not root and not _reaches_stdout_handler(logger):
        # Root logger defaults to STDERR, we want anything at INFO or lower in STDOUT
        stdout_handler = _StdoutHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)

        # Filter out any WARNING or higher (goes to STDERR):
        stdout_handler.addFilter(lambda record: record.levelno < logging.WARNING)
        stdout_handler.setFormatter(formatter)

        logger.addHandler(stdout_handler)

    if filename:
        register_file_handler(logger, filename, level, fmt)

    # Re-enable propagation on final logger
    logger.propagate = True
    return t.cast("TraceLogger", logger)


class LoggingMixin:
    """A mixin class that provides a logger instance for use in other classes."""

    @property
    def log(self) -> TraceLogger:
        """Return the logger instance for this class."""
        if not hasattr(self, "_log"):
            name = f"{self.__class__.__module__}.{self.__class__.__name__}"
            self._log = get_logger(name)
        return self._log


_WARNED_LOG_LEVELS: set[str] = set()
"""The unrecognised log level names `parse_log_level_name` has already reported:
every logger created at import asks for the level, and the user needs to hear
about a bad one once per process."""


def parse_log_level_name(log_level: int | str) -> int:
    """Return the numeric value of a log level.

    A name is matched in any case against `LogLevelChoices`. An unrecognised name
    is reported once per distinct value on stderr and answered with
    `DEFAULT_LOG_LEVEL`: the loggers created at import ask for the level, so
    raising would break every command. It prints rather than logs because it
    runs inside `get_logger`.

    Parameters
    ----------
    log_level : int | str
        A numeric level, returned as is, or a level name.

    Returns
    -------
    int
        The numeric level.
    """
    if isinstance(log_level, int):
        return log_level

    try:
        choice = LogLevelChoices(log_level)
    except ValueError:
        if log_level not in _WARNED_LOG_LEVELS:
            _WARNED_LOG_LEVELS.add(log_level)
            accepted = ", ".join(LogLevelChoices)
            fallback = logging.getLevelName(DEFAULT_LOG_LEVEL)
            print(
                f"Ignoring log level {log_level!r}: use one of {accepted}; "
                f"logging at {fallback}.",
                file=sys.stderr,
            )
        return DEFAULT_LOG_LEVEL
    return logging.getLevelNamesMapping()[choice.value]


def reset_log_level(level: LogLevelChoices) -> None:
    # set the level on the root logger
    logging.getLogger().setLevel(parse_log_level_name(level))

    # force loggers to delegate level to root logger
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        logger.setLevel(logging.NOTSET)
