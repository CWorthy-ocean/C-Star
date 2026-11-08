"""The ``compress`` application: compress the data files of a finished run in place.

A compress blueprint is written by ``cstar run compress`` from its questions (or
by hand), and runs like any blueprint: ``cstar blueprint run`` here, or as a
step of a workplan (``cstar workplan submit``) with SLURM resources in the
step's ``compute_overrides``. See `cstar.archive.compress`.
"""

import typing as t

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.archive.compress import APP_NAME, CompressBlueprint, compress_run
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.handler import ExecutionStatus

__all__ = ["APP_NAME", "CompressApplication", "CompressBlueprint", "CompressRunner"]

_APP_NAME_LONG: t.Final[str] = "Compress run output"
"""The long-form application name."""


class CompressRunner(BlueprintRunner[CompressBlueprint]):
    """Worker class that compresses a run's data files."""

    @t.override
    async def run(self) -> RunnerResult[CompressBlueprint]:
        """Compress the files; the step fails when any file could not be compressed.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        outcomes = compress_run(self.blueprint)
        failed = any(o.status == "failed" for o in outcomes)
        self.add_state(ExecutionStatus.FAILED if failed else ExecutionStatus.COMPLETED)
        return self.result


@register_application
class CompressApplication(ApplicationDefinition[CompressBlueprint, CompressRunner]):
    name = APP_NAME
    long_name = _APP_NAME_LONG
    runner = CompressRunner
    blueprint = CompressBlueprint
    applicable_transforms = ()
