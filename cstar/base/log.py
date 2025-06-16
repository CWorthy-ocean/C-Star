import logging
import sys
from pathlib import Path

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "[%(levelname)s] %(message)s"


def get_logger(
    name: str | None = None,
    level: int = DEFAULT_LOG_LEVEL,
    fmt: str | None = None,
    filename: str | None | Path = None,
) -> logging.Logger:
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
        if isinstance(filename, Path):
            filename.parent.mkdir(parents=True, exist_ok=True)
            filename = str(filename)

        existing_fh = [
            h
            for h in logger.handlers
            if isinstance(h, logging.FileHandler)
            and Path(h.baseFilename).resolve() == Path(filename).resolve()
        ]
        if not existing_fh:
            file_handler = logging.FileHandler(filename)
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(fmt=fmt))
            logger.addHandler(file_handler)

    # Re-enable propagation on final logger
    logger.propagate = True
    return logger


class LoggingMixin:
    """A mixin class that provides a logger instance for use in other classes."""

    @property
    def log(self) -> logging.Logger:
        if not hasattr(self, "_log"):
            name = f"{self.__class__.__module__}.{self.__class__.__name__}"
            self._log = get_logger(name)
        return self._log
