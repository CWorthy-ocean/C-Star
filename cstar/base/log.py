import enum
import logging
import sys
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
        file_handler.setFormatter(logging.Formatter(fmt=fmt))
        logger.addHandler(file_handler)
        logger.info(f"FileHandler registered to write to: {file_path}")
        existing_fh = file_handler
    elif existing_fh.level != level:
        logger.debug(f"FileHandler is already set to level: {existing_fh.level}")

    return existing_fh


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
    formatter = logging.Formatter(fmt)

    # Set up root logger if not already configured
    root = logging.getLogger()

    if not root.hasHandlers():
        logging.basicConfig(level=logging.WARNING, format=DEFAULT_LOG_FORMAT)

    # Ensure root handlers only handle WARNING and higher
    for handler in root.handlers:
        handler.setLevel(logging.WARNING)

    # Create specific STDOUT handler for INFO and lower:
    if not logger.hasHandlers():
        # Root logger defaults to STDERR, we want anything at INFO or lower in STDOUT
        stdout_handler = logging.StreamHandler(sys.stdout)
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


def parse_log_level_name(log_level: int | str) -> int:
    level = (
        logging.getLevelNamesMapping()[log_level.upper()]
        if isinstance(log_level, str)
        else log_level
    )
    return level
