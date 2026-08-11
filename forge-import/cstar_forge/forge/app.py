"""The forge C-Star application: wires ``ForgeBlueprint`` into a real,
C-Star-discoverable application via :class:`ForgeRunner` + :class:`ForgeApplication`
(see ``cstar.orchestration.models.Blueprint`` /
https://c-star.readthedocs.io/en/latest/custom_applications.html).

Not part of the "forge application" boundary that ``tests/test_forge_app_boundary.py``
guards -- like ``cstar_forge/run.py``, this module is **host-resolution glue**: it
reaches into ``cstar_forge.config`` (via ``cstar_forge.run.process``) to resolve the
host, which the guarded ``cstar_forge/forge/`` modules must never do. It stays
disposable for the same reason ``run.py`` is: when forge relocates into C-Star
wholesale, C-Star supplies its own host resolution and this module is rewritten, not
carried over as-is.

Discoverable via the ``CSTAR_APP_MODULES`` environment variable
(``cstar.applications.core.get_application``)::

    export CSTAR_APP_MODULES=cstar_forge.forge.app

which lets C-Star's own entrypoint discover and run a forge blueprint directly, e.g.
``cstar blueprint run forge_blueprint.yaml``.

Scope (2026-07, first cut): :meth:`ForgeRunner.run` generates ROMS-MARBL inputs and
emits the downstream ``roms_marbl`` blueprint (``B_{name}.yaml``), then stops -- it
does not chain into actually running the ROMS-MARBL simulation. That is the existing
``roms_marbl`` application's job, consuming the blueprint forge just produced. See
``docs/architecture-details.md`` for the producer/consumer boundary between the two
applications.
"""

from __future__ import annotations

import shutil
import typing as t
from pathlib import Path

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.file_system import JobFileSystemManager
from cstar.execution.handler import ExecutionStatus

from cstar_forge.forge.forge_blueprint import DEFAULT_APPLICATION, ForgeBlueprint

APP_NAME: t.Final[str] = DEFAULT_APPLICATION

_APP_NAME_LONG: t.Final[str] = "C-Star Forge domain generator"


class ForgeRunner(BlueprintRunner[ForgeBlueprint]):
    """Worker class that runs the forge domain-generation pipeline from a blueprint."""

    @t.override
    async def run(self) -> RunnerResult[ForgeBlueprint]:
        """Generate ROMS-MARBL inputs and emit the downstream ``roms_marbl`` blueprint.

        Delegates to ``cstar_forge.run.process`` (host-resolution glue) ->
        ``forge_blueprint_engine.process_forge_blueprint`` -> the ``ForgeExecutor``
        substitution seam (``ensure_source_data`` -> ``generate_inputs`` ->
        ``configure_build``). Synchronous and heavy (network fetches, roms-tools
        NetCDF generation) -- runs inline on the event loop for this first cut;
        ``asyncio.to_thread`` is a candidate refinement if that becomes a problem.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        from cstar_forge import run as forge_run

        try:
            executor = forge_run.process(self.blueprint)
        except Exception as ex:
            msg = "An error occurred while generating forge inputs"
            self.log.exception(msg)
            self.add_state(ExecutionStatus.FAILED, [msg, str(ex)])
            return self.result

        self.log.debug(
            f"Forge blueprint emitted: {executor.path_roms_marbl_blueprint()}"
        )
        published = self._publish_blueprint(executor)
        self.log.info(f"Forge blueprint published for downstream steps: {published}")
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    @staticmethod
    def _publish_blueprint(executor: t.Any) -> Path:
        """Copy the emitted ``roms_marbl`` blueprint into ``<working root>/output/``.

        Under a workplan, C-Star's deferred-blueprint resolution
        (``cstar.orchestration.transforms.resolve_deferred_blueprint``) looks for the
        producer step's artifact in its ``output/`` dir (the step's ``working_dir``
        root, which the scheduler system-override points the forge blueprint at) --
        not in the ``blueprints/`` dir the executor writes to. Only the blueprint is
        copied (not the ``settings_B_{name}.yaml`` sidecar), so a deferred reference
        that omits ``filename`` still resolves to a unique candidate.

        Returns
        -------
        Path
            The published blueprint path.
        """
        src = executor.path_roms_marbl_blueprint()
        out_dir = JobFileSystemManager(src.parent.parent).output_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        dest = out_dir / src.name
        shutil.copy2(src, dest)
        return dest


@register_application
class ForgeApplication(ApplicationDefinition[ForgeBlueprint, ForgeRunner]):
    """Registers ``forge`` with C-Star's application registry (see module docstring
    for the ``CSTAR_APP_MODULES`` discovery mechanism).
    """

    name = APP_NAME
    long_name = _APP_NAME_LONG
    runner = ForgeRunner
    blueprint = ForgeBlueprint
    applicable_transforms = ()
    migrations = ()
