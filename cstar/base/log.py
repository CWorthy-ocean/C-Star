import logging


DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s::%(funcName)s:%(lineno)d: %(message)s"
)


def get_logger(
    name: str | None = None, level: int = logging.INFO, fmt: str | None = None
) -> logging.Logger:
    """Get a logger instance with the specified name.

    Parameters
    ----------
    name : str
        The name of the logger.
    level: int
        The minimum log level to write.
    fmt : str
        The log format string.

    Returns
    -------
    logging.Logger
        A logger instance with the specified name.
    """

    fmt = fmt or DEFAULT_LOG_FORMAT
    root = logging.getLogger()
    if not root.hasHandlers():
        logging.basicConfig(level=DEFAULT_LOG_LEVEL, format=DEFAULT_LOG_FORMAT)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        # Ensure handlers use specified format
        for handler in logger.handlers:
            handler.setFormatter(logging.Formatter(fmt))

    return logger


class LoggingMixin:
    """A mixin class that provides a logger instance for use in other classes."""

    @property
    def log(self) -> logging.Logger:
        if not hasattr(self, "_log"):
            name = f"{self.__class__.__module__}.{self.__class__.__name__}"
            self._log = get_logger(name)
        return self._log
