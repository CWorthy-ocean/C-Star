import random
import textwrap
import typing as t
from pathlib import Path

import yaml

from cstar.base.adapter import ConfiguredModelAdapter, ModelEnricher
from cstar.base.env import ENV_CSTAR_CLOBBER_WORKING_DIR
from cstar.base.feature import is_flag_enabled
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_DIRECTIVES_URI_LONG
from cstar.orchestration.orchestration import RunRequest, RunRequestCommandFormatter

if t.TYPE_CHECKING:
    from cstar.orchestration.orchestration import LiveStep


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


class StepToRunRequestAdapter(ConfiguredModelAdapter["LiveStep", "RunRequest"]):
    """Convert a `LiveStep` into a `RunRequest`."""

    _enricher: ModelEnricher["RunRequest"] | None = None
    """A model enricher that modifies the run request."""

    def __init__(self, enricher: ModelEnricher["RunRequest"] | None = None) -> None:
        """Initialize the adapter instance.

        Parameters
        ----------
        enricher : ModelEnricher[RunRequest | None]
            An enricher to be applied after the default adaptation is performed.
        """
        self._enricher = enricher

    def adapt(
        self,
        model: "LiveStep",
    ) -> "RunRequest":
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
            str(model.blueprint_path),
        ]

        if is_flag_enabled(ENV_CSTAR_CLOBBER_WORKING_DIR):
            cmd_array.append(ARG_CLOBBER)

        if model.directives:
            directives_path = prepare_directive_file(model)
            cmd_array.extend([ARG_DIRECTIVES_URI_LONG, str(directives_path)])

        request = RunRequest(command=cmd_array)
        if self._enricher and (enriched_request := self._enricher.enrich(request)):
            request = enriched_request
        return request


class StepToPlaceholderAdapter(StepToRunRequestAdapter):
    """Convert a `LiveStep` into a `RunRequest` with the original command
    replaced with a placeholder script.
    """

    SCRIPTFILE_NAME: t.Final[str] = "placeholder_script.sh"

    def adapt(self, model: "LiveStep") -> RunRequest:
        """Convert a `Step` into a placeholder request instead of the
        originally requested blueprint execution.

        Returns
        -------
        RunRequest
            The instance converted from the source model.
        """
        request = super().adapt(model)

        if not model.fsm.run_dir.exists():
            model.fsm.run_dir.mkdir(parents=True)

        original_cmd = RunRequestCommandFormatter().format(request)
        sleep_time = random.random()

        script = textwrap.dedent(f"""\
            # this is a mock application script that produces verifiable output
            echo "{model.name} started at $(date "+%Y-%m-%d %H:%M:%S")";
            echo "replacing: {original_cmd}";
            sleep {sleep_time};
            echo "{model.name} completed at $(date "+%Y-%m-%d %H:%M:%S")";
            """)

        # write it to a script asset
        script_path = model.fsm.run_dir / self.SCRIPTFILE_NAME
        script_path.write_text(script)

        return RunRequest(command=["sh", str(script_path)])
