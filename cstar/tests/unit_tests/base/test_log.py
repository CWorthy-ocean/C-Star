import logging
import pathlib
import re
import uuid
from collections.abc import Callable
from time import strftime
from unittest import mock

import pytest

from cstar.base.log import TRACE_LOG_LEVEL, get_logger, register_file_handler

LoggingFn: t.TypeAlias = Callable[[str], None]


@pytest.fixture
def all_levels() -> list[int]:
    return [
        TRACE_LOG_LEVEL,
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.CRITICAL,
        logging.ERROR,
    ]


@pytest.fixture
def levels_fn(
    all_levels: list[int],
) -> Callable[[int], tuple[list[int], list[int]]]:
    """Return a function that returns a tuple of lists containing all log levels below
    and above the specified log level.

    Returns:
        Callable[list[int], list[int]]: the function
    """

    def _inner(level: int) -> tuple[list[int], list[int]]:
        """Return a tuple containing:
        1. list of levels less than the supplied level
        2. list of levels greater than or equal to the supplied level

        Args:
            level (int): the target log level

        Returns:
            tuple[list[int], list[int]]: the lists of log levels
        """
        lt: list[int] = []
        gte: list[int] = []

        for level_ in all_levels:
            if level_ < level:
                lt.append(level_)
            elif level_ >= level:
                gte.append(level_)

        return lt, gte

    return _inner


@pytest.mark.parametrize(
    "level",
    [
        TRACE_LOG_LEVEL,
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.CRITICAL,
        logging.ERROR,
    ],
)
def test_loglevel_fh(
    request: pytest.FixtureRequest,
    level: int,
    levels_fn: Callable[[int], tuple[list[int], list[int]]],
    all_levels: list[int],
    tmp_path: pathlib.Path,
) -> None:
    """Verify the loggers are configured properly to output the desired log levels when
    the optional file handler is requested.
    """
    logger_name = f"{request.function.__name__}-{level}"
    filename = tmp_path / f"{logger_name}.log"

    log = get_logger(logger_name, level, filename=str(filename))
    lt_levels, gt_eq_levels = levels_fn(level)

    funcs: list[LoggingFn] = [
        log.trace,
        log.debug,
        log.info,
        log.warning,
        log.critical,
        log.error,
    ]
    names = {ll: logging.getLevelName(ll) for ll in all_levels}

    for actual, log_fn in zip(all_levels, funcs):
        msg = f"This is a {names[actual]} message"
        log_fn(msg)

    today = strftime("%Y\\-%m\\-%d")
    log_content = filename.read_text()
    log_fmt_pattern = r"{0} \d*:\d*:\d*,\d* \[{1}\] - .*\.py:\d* - This is a .*"

    for actual in gt_eq_levels:
        # confirm all messages >= level are in the file
        pattern = log_fmt_pattern.format(today, names[actual])
        matches = re.findall(pattern, log_content)
        assert matches, f"Did not find a match for pattern: {pattern}"

    for actual in lt_levels:
        # confirm messages < level are not in file
        pattern = log_fmt_pattern.format(today, names[actual])
        matches = re.findall(pattern, log_content)
        assert not matches, f"Unexpected match for pattern: {pattern}"


def test_filehandler_no_dupes(
    request: pytest.FixtureRequest,
    tmp_path: pathlib.Path,
) -> None:
    """Verify the loggers are configured properly to output the desired log levels when
    the optional file handler is requested.
    """
    level = logging.INFO
    logger_name = f"{request.function.__name__}-{level}"
    filename = tmp_path / f"{logger_name}.log"

    # ask for log repeatedly, fh should only be added once.
    log = get_logger(logger_name, level, filename=filename)
    log = get_logger(logger_name, level, filename=filename)
    log = get_logger(logger_name, level, filename=filename)
    log = get_logger(logger_name, level, filename=filename)

    msg = str(uuid.uuid4()).replace("-", "")
    log.info(msg)

    log_content = filename.read_text()

    today = strftime("%Y\\-%m\\-%d")
    pattern = rf"{today} \d*:\d*:\d*,\d* \[INFO\] - .*\.py:\d* - {msg}"
    matches = re.findall(pattern, log_content)

    # confirm entry is found...
    assert matches

    # ... and isn't written more than once
    assert len(matches) == 1


@pytest.mark.parametrize(
    "filepath",
    [
        ("",),
        (" ",),
        (None,),
    ],
)
def test_register_fh_no_filename(
    log: logging.Logger,
    filepath: str | pathlib.Path | None,
) -> None:
    """Verify that no handlers are added if a bad filename is passed.

    Parameters
    ----------
    log: logging.Logger
        A logger to modify
    filepath: pathlib.Path
        A path to test
    """
    initial_count = len(log.handlers)

    with pytest.raises(ValueError):
        _ = register_file_handler(log, filepath, logging.DEBUG, "%(msg)s")

    assert len(log.handlers) == initial_count


def test_register_fh_dir_exists(
    log: logging.Logger,
    tmp_path: pathlib.Path,
) -> None:
    """Verify that a handler is added if a valid, existing path is passed.

    Parameters
    ----------
    log: logging.Logger
        A logger to modify
    tmp_path: pathlib.Path
        A temp path for writing test outputs
    """
    filepath = tmp_path / "foo.log"
    initial_count = len(log.handlers)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with mock.patch("pathlib.Path.mkdir", new_callable=mock.Mock) as mock_mkdir:
        fh = register_file_handler(log, filepath, logging.DEBUG, "%(msg)s")

    assert fh
    assert mock_mkdir.called
    assert len(log.handlers) == initial_count + 1, "Handler not registered."


def test_register_fh_dir_not_exist(
    log: logging.Logger,
    tmp_path: pathlib.Path,
) -> None:
    """Verify that a handler is added if the path contains a new directory.

    Parameters
    ----------
    log: logging.Logger
        A logger to modify
    tmp_path: pathlib.Path
        A temp path for writing test outputs
    """
    filepath = tmp_path / "subdir" / "foo.log"
    initial_count = len(log.handlers)

    fh = register_file_handler(log, filepath, logging.DEBUG, "%(msg)s")

    assert fh
    assert filepath.parent.exists()
    assert len(log.handlers) == initial_count + 1, "Handler not registered."


def test_register_fh_resolution(
    log: logging.Logger,
    tmp_path: pathlib.Path,
) -> None:
    """Verify that paths are resolved and avoid logger duplication.

    Parameters
    ----------
    log: logging.Logger
        A logger to modify
    tmp_path: pathlib.Path
        A temp path for writing test outputs
    """
    filepath = tmp_path / "subdir" / "foo.log"
    filepath_sym = tmp_path / "subdir2" / "foo.log"
    filepath_sym.parent.symlink_to(filepath.parent)

    initial_count = len(log.handlers)

    fh = register_file_handler(log, filepath, logging.DEBUG, "%(msg)s")

    assert fh
    assert filepath.parent.exists()
    assert len(log.handlers) == initial_count + 1

    # the symlinked path must be resolved to avoid this dupe
    fh = register_file_handler(log, str(filepath_sym), logging.DEBUG, "%(msg)s")

    assert fh
    assert filepath_sym.parent.exists()
    assert len(log.handlers) == initial_count + 1, "Handler duplicated"


def test_register_fh_dupe_level(
    log: logging.Logger,
    tmp_path: pathlib.Path,
) -> None:
    """Verify that dupe registrations do not change the log level.

    Parameters
    ----------
    log: logging.Logger
        A logger to modify
    tmp_path: pathlib.Path
        A temp path for writing test outputs
    """
    filepath = tmp_path / "subdir" / "foo.log"
    filepath_sym = tmp_path / "subdir2" / "foo.log"
    filepath_sym.parent.symlink_to(filepath.parent)

    initial_level = logging.DEBUG
    fh0 = register_file_handler(log, filepath, initial_level, "%(msg)s")
    assert fh0.level == initial_level

    # confirm a dupe doesn't change the log level
    update_level = logging.INFO
    fh1 = register_file_handler(log, filepath_sym, update_level, "%(msg)s")
    assert fh1.level != update_level
