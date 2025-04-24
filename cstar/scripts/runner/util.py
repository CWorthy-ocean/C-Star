import argparse
import dataclasses as dc
import logging
import pathlib
import typing as t
import functools as ft
import uuid


@ft.wraps
def autolog(func) -> t.Callable[[t.Any], t.Any]:
    """Decorator to automatically log the start and end of a function call."""

    def wrapper(*args, **kwargs) -> t.Any:
        try:
            log = logging.getLogger()
            log.debug(f"Calling {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
        except Exception as ex:
            log.debug(f"Completed {func.__name__} with errors: {ex}")
            raise
        else:
            log.debug(f"Completed {func.__name__}")

        return result

    return wrapper


def create_parser() -> argparse.ArgumentParser:
    """Creates a parser for command line arguments expected by the c-star Worker."""
    parser = argparse.ArgumentParser(
        description="Run a c-star simulation.",
    )
    parser.add_argument(
        "--blueprint-uri",
        type=str,
        required=True,
        help="The URI of a blueprint.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str,
        required=False,
        help="Logging level for the simulation.",
        choices=[
            logging._levelToName[i]
            for i in [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
        ],
    )
    parser.add_argument(
        "--output-dir",
        default="~/code/cstar/examples/",
        type=str,
        required=False,
        help="Local path to write simulation outputs to",
    )
    return parser


@dc.dataclass(frozen=True)
class RunRequest:
    """Represents a request to run a c-star simulation."""

    blueprint_uri: str = dc.field(
        metadata={"description": "The path to the blueprint."},
    )
    output_dir: pathlib.Path = dc.field(
        metadata={"description": "The directory to write simulation outputs to"},
    )
    log_level: str = dc.field(
        default="INFO",
        metadata={"description": "The log level for the simulation."},
    )
    request_id: str = dc.field(
        default_factory=lambda: str(uuid.uuid4()),
        metadata={"description": "A unique identifier for the request."},
    )

    @staticmethod
    def from_args(args: argparse.Namespace) -> "RunRequest":
        """Creates a RunRequest from command line arguments."""

        return RunRequest(
            blueprint_uri=args.blueprint_uri,
            log_level=args.log_level,
            output_dir=pathlib.Path(args.output_dir),
        )
