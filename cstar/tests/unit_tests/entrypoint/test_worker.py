import itertools
import logging
import os
from unittest import mock

import pytest

from cstar.entrypoint.worker.worker import (
    configure_environment,
    create_parser,
    get_request,
    get_service_config,
)

DEFAULT_LOOP_DELAY = 5
DEFAULT_HEALTH_CHECK_FREQUENCY = 10


@pytest.fixture
def valid_args() -> dict[str, str]:
    """Fixture to provide valid arguments for the SimulationRunner."""
    return {
        "--blueprint-uri": "blueprint.yaml",
        "--output-dir": "output",
        "--log-level": "INFO",
        "--start-date": "2012-01-03 12:00:00",
        "--end-date": "2012-01-04 12:00:00",
    }


def test_create_parser_help() -> None:
    """Verify that a help argument is present in the parser."""
    parser = create_parser()

    # no help argument present
    with pytest.raises(ValueError):  # noqa: PT011
        _ = parser.parse_args(["--help"])


def test_create_parser_happy_path() -> None:
    """Verify that a help argument is present in the parser."""
    parser = create_parser()

    # ruff: noqa: SLF001
    assert "--blueprint-uri" in parser._option_string_actions
    assert "--output-dir" in parser._option_string_actions
    assert "--log-level" in parser._option_string_actions
    assert "--start-date" in parser._option_string_actions
    assert "--end-date" in parser._option_string_actions


@pytest.mark.parametrize(
    ("log_level", "expected_level"),
    [
        ("DEBUG", logging.DEBUG),
        ("INFO", logging.INFO),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
    ],
)
def test_parser_good_log_level(
    valid_args: dict[str, str], log_level: str, expected_level: int
) -> None:
    """Verify that a log level is parsed correctly."""
    valid_args = valid_args.copy()
    valid_args["--log-level"] = log_level

    arg_tuples = [(k, v) for k, v in valid_args.items()]
    args = list(itertools.chain.from_iterable(arg_tuples))

    parser = create_parser()
    parsed_args = parser.parse_args(args)

    assert getattr(parsed_args, "log_level", None) == logging.getLevelName(
        expected_level
    )


@pytest.mark.parametrize(
    "log_level",
    [
        ("debug",),
        ("info",),
        ("warning",),
        ("error",),
        ("critical",),
    ],
)
def test_parser_lowercase_log_level(
    valid_args: dict[str, str],
    log_level: str,
) -> None:
    """Verify that lower-case log levels are parsed correctly."""
    valid_args = valid_args.copy()
    valid_args["--log-level"] = log_level

    arg_tuples = [(k, v) for k, v in valid_args.items()]
    args = list(itertools.chain.from_iterable(arg_tuples))

    parser = create_parser()

    # unable to parse the lower-case log levels
    with pytest.raises(SystemExit):
        parser.parse_args(args)


def test_parser_bad_log_level(valid_args: dict[str, str]) -> None:
    """Verify that a bad log level fails to parse."""
    valid_args = valid_args.copy()
    valid_args["--log-level"] = "INVALID"

    valid_args_tuples = ((k, v) for k, v in valid_args.items())
    args = list(itertools.chain.from_iterable(valid_args_tuples))

    parser = create_parser()

    with pytest.raises(SystemExit):
        _ = parser.parse_args(args)


@pytest.mark.parametrize(
    ("blueprint_uri", "output_dir", "start_date", "end_date"),
    [
        (
            "blueprint1.yaml",
            "output1",
            "2012-01-01 12:00:00",
            "2012-02-04 12:00:00",
        ),
        (
            "blueprint2.yaml",
            "output2",
            "2020-02-01 00:00:00",
            "2020-03-02 00:00:00",
        ),
        (
            "blueprint3.yaml",
            "output3",
            "2021-03-01 08:30:00",
            "2021-04-16 09:30:00",
        ),
    ],
)
def test_get_service_config(
    blueprint_uri: str,
    output_dir: str,
    start_date: str,
    end_date: str,
) -> None:
    """Verify that the expected values are set on the service config."""
    parser = create_parser()
    parsed_args = parser.parse_args(
        [
            "--blueprint-uri",
            blueprint_uri,
            "--output-dir",
            output_dir,
            "--log-level",
            "INFO",
            "--start-date",
            start_date,
            "--end-date",
            end_date,
        ]
    )

    config = get_service_config(parsed_args)

    # some values are currently hardcoded for the worker service
    assert config.as_service
    assert config.loop_delay == DEFAULT_LOOP_DELAY
    assert config.health_check_frequency == DEFAULT_HEALTH_CHECK_FREQUENCY

    # log level is dynamic. verify.
    assert logging._levelToName[config.log_level] == parsed_args.log_level


@pytest.mark.parametrize(
    ("blueprint_uri", "output_dir", "start_date", "end_date"),
    [
        (
            "blueprint1.yaml",
            "output1",
            "2012-01-01 12:00:00",
            "2012-02-04 12:00:00",
        ),
        (
            "blueprint2.yaml",
            "output2",
            "2020-02-01 00:00:00",
            "2020-03-02 00:00:00",
        ),
        (
            "blueprint3.yaml",
            "output3",
            "2021-03-01 08:30:00",
            "2021-04-16 09:30:00",
        ),
    ],
)
def test_get_request(
    blueprint_uri: str,
    output_dir: str,
    start_date: str,
    end_date: str,
) -> None:
    """Verify that the expected values are set on the blueprint request."""
    parser = create_parser()
    parsed_args = parser.parse_args(
        [
            "--blueprint-uri",
            blueprint_uri,
            "--output-dir",
            output_dir,
            "--log-level",
            "INFO",
            "--start-date",
            start_date,
            "--end-date",
            end_date,
        ]
    )

    config = get_request(parsed_args)

    assert config.blueprint_uri == blueprint_uri
    assert str(config.output_dir) == output_dir
    assert str(config.start_date) == start_date
    assert str(config.end_date) == end_date


def test_configure_environment() -> None:
    """Verify the environment side-effects are as expected."""
    log = logging.getLogger()
    logging.basicConfig(level=logging.DEBUG)

    with mock.patch.dict(
        os.environ,
        {},
        clear=True,
    ):
        configure_environment(log)

        assert "CSTAR_INTERACTIVE" in os.environ
        assert os.environ["CSTAR_INTERACTIVE"] == "0"

        assert "GIT_DISCOVERY_ACROSS_FILESYSTEM" in os.environ
        assert os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] == "1"


def test_configure_environment_prebuilt() -> None:
    """Verify the environment side-effects when in a prebuilt environment.

    There shouldn't be behavioral changes compared to non-prebuilt environment.
    """
    log = logging.getLogger()
    logging.basicConfig(level=logging.DEBUG)

    with mock.patch.dict(
        os.environ,
        {
            "CSTAR_ROMS_PREBUILT": "1",
            "CSTAR_MARBL_PREBUILT": "1",
        },
        clear=True,
    ):
        configure_environment(log)

        assert "CSTAR_INTERACTIVE" in os.environ
        assert os.environ["CSTAR_INTERACTIVE"] == "0"

        assert "GIT_DISCOVERY_ACROSS_FILESYSTEM" in os.environ
        assert os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] == "1"
