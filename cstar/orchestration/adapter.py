import random
import textwrap
import typing as t
from pathlib import Path

import yaml

from cstar.base.adapter import ModelAdapter
from cstar.base.env import ENV_CSTAR_CLOBBER_WORKING_DIR
from cstar.base.feature import is_flag_enabled
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_DIRECTIVES_URI_LONG
from cstar.orchestration import orchestration
from cstar.orchestration.formatting import RunRequestCommandFormatter


def prepare_directive_file(step: "orchestration.LiveStep") -> Path:
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


class StepToRunRequestAdapter(
    ModelAdapter["orchestration.LiveStep", orchestration.RunRequest]
):
    """Convert a `LiveStep` into a `RunRequest`."""

    def __init__(self, model: "orchestration.LiveStep") -> None:
        """Initialize the adapter.

        Parameters
        ----------
        model : LiveStep
            The step to convert.
        """
        self.model = model

    def adapt(self) -> orchestration.RunRequest:
        """Convert a `Step` into a request for blueprint execution via the C-Star CLI.

        Returns
        -------
        RunRequest
            The instance converted from the source model.
        """
        cmd_array = [
            "cstar",
            "blueprint",
            "run",
            str(self.model.blueprint_path),
        ]

        if is_flag_enabled(ENV_CSTAR_CLOBBER_WORKING_DIR):
            cmd_array.append(ARG_CLOBBER)

        if self.model.directives:
            directives_path = prepare_directive_file(self.model)
            cmd_array.extend([ARG_DIRECTIVES_URI_LONG, str(directives_path)])

        return orchestration.RunRequest(command=cmd_array)


class StepToPlaceholderAdapter(StepToRunRequestAdapter):
    """Convert a `LiveStep` into a `RunRequest` with the original command
    replaced with a placeholder script.
    """

    SCRIPTFILE_NAME: t.Final[str] = "placeholder_script.sh"

    def adapt(self) -> orchestration.RunRequest:
        """Convert a `Step` into a placeholder request instead of the
        originally requested blueprint execution.

        Returns
        -------
        RunRequest
            The instance converted from the source model.
        """
        actual = super().adapt()

        if not self.model.fsm.run_dir.exists():
            self.model.fsm.run_dir.mkdir(parents=True)

        original_cmd = RunRequestCommandFormatter().format(actual)
        sleep_time = random.random()

        script = textwrap.dedent(f"""\
            # this is a mock application script that produces verifiable output
            echo "{self.model.name} started at $(date "+%Y-%m-%d %H:%M:%S")";
            echo "replacing: {original_cmd}";
            sleep {sleep_time};
            echo "{self.model.name} completed at $(date "+%Y-%m-%d %H:%M:%S")";
            """)

        # write it to a script asset
        script_path = self.model.fsm.run_dir / self.SCRIPTFILE_NAME
        script_path.write_text(script)

        return orchestration.RunRequest(command=["sh", str(script_path)])
