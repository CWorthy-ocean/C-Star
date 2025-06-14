import logging
import logging.handlers
import pathlib
import typing as t
import uuid

import pytest

from cstar.base.log import get_logger


@pytest.fixture
def all_levels() -> list[int]:
    """Return all logging levels."""
    return [
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
    ]


@pytest.fixture
def levels_fn(all_levels: list[int]) -> t.Callable[[int], tuple[list[int], list[int]]]:
    """Return a factory for creating a split of log levels above/below a fixed level.

    Returns
        fn : t.Callable[list[int], list[int]]
    """

    def _inner(level: int) -> tuple[list[int], list[int]]:
        """Return a tuple containing lists of the levels below and above a level.

        1. list of levels less than the supplied level
        2. list of levels greater than or equal to the supplied level

        Parameters
        ----------
        level : int
            The log level to split above/below on.

        Returns
        -------
            tuple[list[int], list[int]] : the lists of log levels
        """
        lt, gte = [], []

        for level_ in all_levels:
            if level_ < level:
                lt.append(level_)
            elif level_ >= level:
                gte.append(level_)

        return lt, gte

    return _inner


@pytest.mark.parametrize(
    "level",
    [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
)
def test_loglevel_fh(
    request: pytest.FixtureRequest,
    level: int,  # noqa: ARG001
    levels_fn: t.Callable[[int], tuple[list[int], list[int]]],
    all_levels: list[int],
    tmp_path: pathlib.Path,
) -> None:
    """Verify logger behavior when the optional file handler is requested.

    Tests that the loggers are configured properly to output the desired log levels to
    the file.
    """
    for level_ in all_levels:
        logger_name = f"{request.function.__name__}-{level_}"
        filename = tmp_path / f"{logger_name}.log"

        log = get_logger(logger_name, level_, filename=str(filename))

        lt_levels, gt_eq_levels = levels_fn(level_)

        msg = str(uuid.uuid4())
        funcs = [log.debug, log.info, log.warning, log.error, log.critical]

        for msg_level, log_fn in zip(all_levels, funcs, strict=False):
            log_fn(msg)
            log_content = filename.read_text()

            # confirm all messages >= level are in the file
            if msg_level in list(gt_eq_levels):
                expected_log_entry = f"[{logging.getLevelName(msg_level)}] {msg}"
                assert expected_log_entry in log_content

            # confirm messages < level are not in file
            if msg_level in list(lt_levels):
                assert msg not in log_content


def test_filehandler_no_dupes(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path
) -> None:
    """Verify the loggers are configured properly.

    Ensure loggers output messages at the desired log levels when the optional file
    handler is requested.
    """
    level = logging.INFO
    logger_name = f"{request.function.__name__}-{level}"
    filename = tmp_path / f"{logger_name}.log"

    # ask for log repeatedly, fh should only be added once.
    log = get_logger(logger_name, level, filename=filename)
    log = get_logger(logger_name, level, filename=filename)
    log = get_logger(logger_name, level, filename=filename)
    log = get_logger(logger_name, level, filename=filename)

    msg = str(uuid.uuid4())
    log.info(msg)

    log_content = filename.read_text()
    expected_log_entry = f"[{logging.getLevelName(level)}] {msg}"

    # confirm entry is found...
    found_at = log_content.find(expected_log_entry)
    assert found_at > -1

    # ... and isn't written more than once
    found_at = log_content.find(expected_log_entry, len(expected_log_entry))
    assert found_at == -1
