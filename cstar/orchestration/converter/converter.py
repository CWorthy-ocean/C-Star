import os
import random
import textwrap
import typing as t
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from cstar.base.env import ENV_CSTAR_CLOBBER_WORKING_DIR
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_DIRECTIVES_URI_LONG
from cstar.orchestration.models import Application
from cstar.orchestration.utils import (
    ENV_CSTAR_CMD_CONVERTER_OVERRIDE,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
)

if t.TYPE_CHECKING:
    from collections.abc import Callable

    from cstar.orchestration.orchestration import LiveStep

log = get_logger(__name__)


@dataclass
class RunRequest:
    command: list[str] = field(default_factory=list[str])
    environment: dict[str, str] = field(default_factory=dict[str, str])

    def as_command(self) -> str:
        variables = " ".join(
            f"{key}='{value}'" for key, value in self.environment.items()
        )
        cmd = " ".join(self.command)

        return f"{variables} {cmd}"

    def as_script(self) -> str:
        command = " ".join(self.command)
        exports = ";".join(
            f"export {key}='{value}'" for key, value in self.environment.items()
        )
        return f"{exports}; {command};"


StepToCommandConversionFn: t.TypeAlias = "Callable[[LiveStep], RunRequest]"
"""Convert a `Step` into a command to be executed.

Parameters
----------
step : "Step"
    The step to be converted.

Returns
-------
str
    The complete CLI command.
"""


def prepare_directive_file(step: "LiveStep") -> Path:
    """Create a directives file in the step work directory.

    Parameters
    ----------
    step : LiveStep
        The step to prepare a directive file for.

    Returns
    -------
    str
        The path to the directive file.
    """
    directives_path = step.fsm.run_dir / "directives.yaml"
    if not step.fsm.run_dir.exists():
        step.fsm.run_dir.mkdir(parents=True)
    with directives_path.open("w") as fp:
        model = step.model_dump(include={"directives"})
        content = yaml.dump(model, sort_keys=False)
        fp.write(content)
    return directives_path


def create_inline_compute_overrides(step: "LiveStep") -> dict[str, str]:
    """Generate a dictionary containing environment variables that must be
    passed to the new process.
    """
    variables: dict[str, str] = {}
    for key, env_var in [("walltime", ENV_CSTAR_SLURM_MAX_WALLTIME)]:
        if key in step.compute_overrides:
            variables[env_var] = str(step.compute_overrides[key])
    return variables


def convert_step_to_blueprint_run_command_request(step: "LiveStep") -> RunRequest:
    """Convert a generic blueprint execution step to a CLI command.

    Parameters
    ----------
    step : LiveStep
        The step to be converted.

    Returns
    -------
    str
        The complete CLI command.
    """
    cmd_array = [
        "cstar",
        "blueprint",
        "run",
        str(step.blueprint_path),
    ]

    if is_flag_enabled(ENV_CSTAR_CLOBBER_WORKING_DIR):
        cmd_array.append(ARG_CLOBBER)

    if step.directives:
        directives_path = prepare_directive_file(step)
        cmd_array.extend([ARG_DIRECTIVES_URI_LONG, str(directives_path)])

    return RunRequest(cmd_array, create_inline_compute_overrides(step))


def convert_step_to_placeholder_request(step: "LiveStep") -> RunRequest:
    """Convert a `Step` into a command to be executed.

    This function converts applications into mocks by starting a process that
    executes a blocking sleep.

    Parameters
    ----------
    step : LiveStep
        The step to be converted.

    Returns
    -------
    str
        The complete CLI command.
    """
    if not step.fsm.run_dir.exists():
        step.fsm.run_dir.mkdir(parents=True)

    sleep_time = random.random()
    variables = create_inline_compute_overrides(step)
    script = textwrap.dedent(f"""\
        # this is a mock application script that produces verifiable output
        echo "{step.name} started at $(date "+%Y-%m-%d %H:%M:%S")";
        sleep {sleep_time};
        echo "{step.name} completed at $(date "+%Y-%m-%d %H:%M:%S")";
        """)

    # write it to a script asset
    script_path = step.fsm.run_dir / "placeholder_script.sh"
    script_path.write_text(script)

    return RunRequest(["sh", str(script_path)], variables)


app_to_cmd_map: dict[str, StepToCommandConversionFn] = defaultdict(
    lambda: convert_step_to_blueprint_run_command_request,
    {
        Application.SLEEP.value: convert_step_to_placeholder_request,
        Application.ROMS_MARBL.value: convert_step_to_blueprint_run_command_request,
    },
)
"""Map application types to a function that converts a step to a CLI command."""


def register_command_mapping(
    application: Application | str,
    mapping_func: StepToCommandConversionFn,
) -> None:
    if isinstance(application, Application):
        application = application.value
    app_to_cmd_map[application] = mapping_func


def get_command_mapping(
    application: str,
) -> StepToCommandConversionFn:
    if isinstance(application, Application):
        application = application.value

    step_converter = app_to_cmd_map[application]
    if step_converter is None:
        msg = f"No command converter found for application: {application!r}"
        raise CstarExpectationFailed(msg)

    if converter_override := os.getenv(ENV_CSTAR_CMD_CONVERTER_OVERRIDE, ""):
        if converter_override not in app_to_cmd_map:
            msg = f"Override in env var {ENV_CSTAR_CMD_CONVERTER_OVERRIDE!r} has invalid value: {converter_override!r}"
            raise ValueError(msg)

        converter = app_to_cmd_map[converter_override]
        msg = f"Overriding step converter `{step_converter}` with `{converter}` for `{application}` commands."
        step_converter = converter
    else:
        msg = f"Using `{step_converter}` for `{application}` commands."

    log.trace(msg)
    return step_converter
