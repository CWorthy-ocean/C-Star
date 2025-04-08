import logging


DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def get_logger(
    name: str, level: int = logging.INFO, format: str | None = None
) -> logging.Logger:
    """Get a logger instance with the specified name.

    Parameters
    ----------
    name : str
        The name of the logger.

    Returns
    -------
    logging.Logger
        A logger instance with the specified name.
    """
    logger = logging.getLogger(name)

    # Ensure handlers are not duplicated
    if len(logger.handlers):
        return logger

    fmt = format or DEFAULT_LOG_FORMAT
    logging.basicConfig(level=level, format=fmt)

    return logger


class LoggingMixin:
    """A mixin class that provides a logger instance for use in other classes."""

    @property
    def log(self) -> logging.Logger:
        if not hasattr(self, "_log"):
            self._log = get_logger(self.__class__.__name__)
        return self._log
