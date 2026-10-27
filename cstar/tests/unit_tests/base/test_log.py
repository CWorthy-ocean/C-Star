import logging
import pathlib
import re
import time
import typing as t
import uuid
from collections.abc import Callable
from time import strftime
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.base.log import (
    DEFAULT_LOG_LEVEL,
    TRACE_LOG_LEVEL,
    LoggingMixin,
    LogLevelChoices,
    get_logger,
    parse_log_level_name,
    register_file_handler,
    reset_log_level,
)

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


@pytest.mark.parametrize("with_parent", [True, False])
def test_stdout_no_dupes_under_parent_logger(
    capsys: pytest.CaptureFixture[str],
    with_parent: bool,
) -> None:
    """Verify a logger whose ancestor already prints to STDOUT (e.g. a LoggingMixin
    class logger under its module's logger) writes each entry to STDOUT once.
    """
    parent_name = f"test_stdout_no_dupes_{uuid.uuid4().hex}"
    if with_parent:
        get_logger(parent_name, logging.INFO)
    child = get_logger(f"{parent_name}.Child", logging.INFO)

    msg = uuid.uuid4().hex
    child.info(msg)

    assert capsys.readouterr().out.count(msg) == 1


def test_filehandler_keeps_local_time_when_converter_is_replaced(
    request: pytest.FixtureRequest,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify C-Star's entries stay in local time after a dependency replaces the
    class-wide `logging.Formatter.converter`, as copernicusmarine does on import.
    """
    monkeypatch.setattr(
        logging.Formatter, "converter", staticmethod(lambda _: time.gmtime(0))
    )
    filename = tmp_path / f"{request.function.__name__}.log"
    log = get_logger(request.function.__name__, logging.INFO, filename=filename)

    log.info("stamped")

    today = strftime("%Y\\-%m\\-%d")
    assert re.search(rf"^{today} .* - stamped$", filename.read_text(), re.MULTILINE)


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


@pytest.mark.parametrize("spelling", ["debug", "Debug", "dEBUG", " debug\n", "DEBUG"])
def test_log_level_choice_matches_any_case(spelling: str) -> None:
    """Verify a level name matches its member in any case, ignoring surrounding
    whitespace.
    """
    assert LogLevelChoices(spelling) is LogLevelChoices.DEBUG


@pytest.mark.parametrize("value", ["loud", "WARN", "FATAL", "NOTSET", "", 10])
def test_log_level_choice_rejects_other_values(value: str | int) -> None:
    """Verify a value that names no member, a stdlib alias or a number included,
    is refused.
    """
    with pytest.raises(ValueError, match="is not a valid LogLevelChoices"):
        LogLevelChoices(t.cast("str", value))


@pytest.mark.parametrize(
    ("choice", "expected"),
    [
        (LogLevelChoices.TRACE, TRACE_LOG_LEVEL),
        (LogLevelChoices.DEBUG, logging.DEBUG),
        (LogLevelChoices.INFO, logging.INFO),
        (LogLevelChoices.WARNING, logging.WARNING),
        (LogLevelChoices.ERROR, logging.ERROR),
        (LogLevelChoices.CRITICAL, logging.CRITICAL),
    ],
)
def test_parse_log_level_name_maps_every_choice(
    choice: LogLevelChoices, expected: int
) -> None:
    """Verify every member of `LogLevelChoices` maps to its numeric level."""
    assert parse_log_level_name(choice) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [("debug", logging.DEBUG), ("TRACE", TRACE_LOG_LEVEL), ("Error", logging.ERROR)],
)
def test_parse_log_level_name_accepts_any_case(
    name: str, expected: int, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify a level name is matched in any case, without a word on stderr."""
    assert parse_log_level_name(name) == expected
    assert capsys.readouterr().err == ""


def test_parse_log_level_name_passes_numbers_through() -> None:
    """Verify a numeric level is returned as it is."""
    assert parse_log_level_name(7) == 7


@pytest.mark.parametrize("name", ["loud", "WARN"])
def test_parse_log_level_name_falls_back_for_unknown_names(
    name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify a name outside `LogLevelChoices`, a stdlib alias included, answers
    with the default level and names the accepted values on stderr.
    """
    assert parse_log_level_name(name) == DEFAULT_LOG_LEVEL == logging.INFO

    assert capsys.readouterr().err == (
        f"Ignoring log level {name!r}: use one of TRACE, DEBUG, INFO, WARNING, "
        "ERROR, CRITICAL; logging at INFO.\n"
    )


def test_parse_log_level_name_reports_each_bad_value_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a bad value is reported once per process, however many loggers ask
    for it, and a different bad value is reported on its own.
    """
    assert parse_log_level_name("loud") == logging.INFO
    assert parse_log_level_name("loud") == logging.INFO

    assert len(capsys.readouterr().err.splitlines()) == 1

    assert parse_log_level_name("louder") == logging.INFO
    assert parse_log_level_name("loud") == logging.INFO

    assert len(capsys.readouterr().err.splitlines()) == 1


def test_get_logger_survives_a_bad_level_in_the_environment(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify a logger created while the environment holds a bad level gets the
    default level instead of failing the import that created it.
    """
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "loud")

    log = get_logger(f"test_bad_level_{uuid.uuid4().hex}")

    assert log.level == logging.INFO
    assert capsys.readouterr().err.count("Ignoring log level 'loud'") == 1


def test_log_level_description_lists_every_choice() -> None:
    """Verify the setting's description names every level the setting accepts."""
    description = get_env_item(ENV_CSTAR_LOG_LEVEL).description

    for choice in LogLevelChoices:
        assert choice.value in description
    assert "any case" in description


def test_reset_log_level_reaches_module_and_class_loggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `reset_log_level` leaves a module logger and a `LoggingMixin` class
    logger under it deferring to the root logger, whatever level they were given.
    """
    monkeypatch.delenv(ENV_CSTAR_LOG_LEVEL, raising=False)
    module_log = get_logger(f"test_reset_{uuid.uuid4().hex}", logging.WARNING)

    class Thing(LoggingMixin):
        pass

    # the mixin names its logger after the class's module
    Thing.__module__ = module_log.name
    thing = Thing()
    assert thing.log.name == f"{module_log.name}.Thing"
    assert module_log.getEffectiveLevel() == logging.WARNING
    assert thing.log.getEffectiveLevel() == DEFAULT_LOG_LEVEL

    reset_log_level(LogLevelChoices.DEBUG)

    assert module_log.getEffectiveLevel() == logging.DEBUG
    assert thing.log.getEffectiveLevel() == logging.DEBUG
    assert logging.getLogger().level == logging.DEBUG
