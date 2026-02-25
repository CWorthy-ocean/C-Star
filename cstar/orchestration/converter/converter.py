import os
import random
import sys
import textwrap
import typing as t
from collections import defaultdict

from cstar.base.log import get_logger
from cstar.orchestration.models import Application, Step
from cstar.orchestration.orchestration import Launcher
from cstar.orchestration.utils import ENV_CSTAR_CMD_CONVERTER_OVERRIDE

log = get_logger(__name__)

StepToCommandConversionFn: t.TypeAlias = t.Callable[[Step], str]
"""Convert a `Step` into a command to be executed.

Parameters
----------
step : Step
    The step to be converted.

Returns
-------
str
    The complete CLI command.
"""


def convert_roms_step_to_command(step: Step) -> str:
    """Convert a `Step` into a command to be executed.

    This function converts ROMS/ROMS-MARBL applications into a command triggering
    a C-Star worker to run a simulation.

    Parameters
    ----------
    step : Step
        The step to be converted.

    Returns
    -------
    str
        The complete CLI command.
    """
    worker_module = "cstar.entrypoint.worker.worker"
    return f"{sys.executable} -m {worker_module}  -b {step.blueprint_path}"


def convert_step_to_placeholder(step: Step) -> str:
    """Convert a `Step` into a command to be executed.

    This function converts applications into mocks by starting a process that
    executes a blocking sleep.

    Parameters
    ----------
    step : Step
        The step to be converted.

    Returns
    -------
    str
        The complete CLI command.
    """
    sleep_time = random.randint(1, 10)
    return textwrap.dedent(f"""\
        # this is a mock application script that produces verifiable output
        echo "{step.name} started at $(date "+%Y-%m-%d %H:%M:%S")";
        sleep {sleep_time};
        echo "{step.name} completed at $(date "+%Y-%m-%d %H:%M:%S")";
        """)


launcher_aware_app_to_cmd_map: dict[
    type[Launcher],
    dict[str, StepToCommandConversionFn],
] = defaultdict(
    lambda: {
        Application.SLEEP.value: convert_step_to_placeholder,
        Application.ROMS_MARBL.value: convert_roms_step_to_command,
    },
)
"""Map application types to a function that converts a step to a CLI command."""


def register_command_mapping(
    application: Application,
    launcher: type[Launcher],
    mapping_func: StepToCommandConversionFn,
) -> None:
    launcher_map = launcher_aware_app_to_cmd_map[launcher]
    launcher_map[application] = mapping_func


def get_command_mapping(
    application: Application,
    launcher: type[Launcher],
) -> StepToCommandConversionFn:
    launcher_map = launcher_aware_app_to_cmd_map[launcher]
    step_converter = launcher_map[application.value]

    if converter_override := os.getenv(ENV_CSTAR_CMD_CONVERTER_OVERRIDE, ""):
        if converter_override not in launcher_map:
            msg = f"Override in env var `{ENV_CSTAR_CMD_CONVERTER_OVERRIDE}` has invalid value: {converter_override}"
            raise ValueError(msg)

        converter = launcher_map[converter_override]
        msg = f"Overriding step converter `{step_converter}` with `{converter}` for `{application}` commands."
        step_converter = converter
    else:
        msg = f"Using `{step_converter}` for `{application}` commands."

    log.debug(msg)
    return step_converter
