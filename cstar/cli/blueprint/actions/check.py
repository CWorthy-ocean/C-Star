import argparse

from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize


async def handle(ns: argparse.Namespace) -> None:
    """The action handler for the blueprint-check action.

    Perform content validation on the blueprint supplied by the user.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    try:
        model = deserialize(ns.path, RomsMarblBlueprint)
        assert model, "Blueprint was not deserialized"
        print(f"{ns.command.capitalize()} is valid")
    except ValueError as ex:
        print(f"Error occurred: {ex}")
