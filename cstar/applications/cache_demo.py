"""A demonstration application for the artifact cache.

The three cached functions below stand in for real dataset-generation steps
that can take hours and produce terabytes (e.g. roms-tools forcing
generation): they sleep briefly and write small files instead. Together they
exercise the cache's single-file, multi-file, and value-returning shapes.

Run it end-to-end with a blueprint based on the shipped template::

    cstar blueprint run cstar/additional_files/templates/bp/cache_demo/blueprint.1.0.0.yaml

The first run executes every function (writing into the personal cache and
symlinking results into `<working_dir>/output`); a second run completes
almost instantly with every function served from cache. Manage entries with
`cstar cache list/show/promote/clear` (behind the `CSTAR_FF_CACHE` feature
flag), and bypass with `cstar blueprint run --no-cache`.
"""

import time
import typing as t
from pathlib import Path

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.base.adapter import SchemaAdapter
from cstar.caching import CacheHandle, cached_artifact
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.file_system import JobFileSystemManager
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint

APP_NAME: t.Final[str] = "cache_demo"


class CacheDemoBlueprint(Blueprint):
    """Configuration for the artifact-cache demonstration application."""

    application: str = APP_NAME
    """The application identifier."""
    dataset_name: str
    """Name of the (pretend) dataset to generate; part of every cache key."""
    num_files: int = 3
    """Number of tile files produced by the multi-file step."""
    sleep_seconds: float = 2.0
    """Simulated cost of each generation step, in seconds."""


@cached_artifact(version="1", label="demo-summary", key_exclude=("sleep_seconds",))
def generate_summary(
    dataset_name: str,
    sleep_seconds: float,
    output_dir: Path,
) -> Path:
    """Produce a single summary file; stands in for a single-output generator.

    Returns
    -------
    Path
        The generated file, restored on cache hits.
    """
    time.sleep(sleep_seconds)
    path = output_dir / f"{dataset_name}_summary.txt"
    path.write_text(f"summary of {dataset_name}\n")
    return path


@cached_artifact(version="1", label="demo-tiles", key_exclude=("sleep_seconds",))
def generate_tiles(
    dataset_name: str,
    num_files: int,
    sleep_seconds: float,
    output_dir: Path,
) -> list[Path]:
    """Produce several tile files; stands in for a multi-file generator
    (e.g. partitioned NetCDF output).

    Returns
    -------
    list[Path]
        The generated files, restored on cache hits.
    """
    paths: list[Path] = []
    for index in range(num_files):
        time.sleep(sleep_seconds / max(num_files, 1))
        path = output_dir / "tiles" / f"{dataset_name}_tile_{index:03d}.dat"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"tile {index} of {dataset_name}\n")
        paths.append(path)
    return paths


@cached_artifact(version="1", label="demo-stats", key_exclude=("sleep_seconds",))
def compute_stats(
    dataset_name: str,
    sleep_seconds: float,
    output_dir: Path,
) -> dict[str, t.Any]:
    """Produce a stats file and return a lightweight summary value; stands in
    for a generator whose Python return value matters downstream.

    Returns
    -------
    dict[str, t.Any]
        JSON-serializable statistics, restored verbatim on cache hits.
    """
    time.sleep(sleep_seconds)
    stats = {"dataset": dataset_name, "mean": 0.5, "count": 42}
    path = output_dir / f"{dataset_name}_stats.json"
    path.write_text(str(stats))
    return stats


class CacheDemoRunner(BlueprintRunner[CacheDemoBlueprint]):
    """Worker class executing the cache demonstration blueprint."""

    def _log_handle(self, handle: CacheHandle) -> None:
        source = (
            f"cache hit ({handle.tier})"
            if handle.hit
            else (
                "regenerated (cache bypassed)"
                if handle.tier is None
                else f"generated into {handle.tier} cache"
            )
        )
        msg = (
            f"{handle.function.rsplit('.', 1)[-1]}: {source} "
            f"[key {handle.key[:12]}] -> {len(handle.paths)} file(s)"
        )
        self.log.info(msg)
        print(msg)

    @t.override
    async def run(self) -> RunnerResult[CacheDemoBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        blueprint = self.blueprint
        fsm = JobFileSystemManager(blueprint.working_dir)
        fsm.prepare()

        started = time.monotonic()

        summary = generate_summary(
            blueprint.dataset_name,
            blueprint.sleep_seconds,
            output_dir=fsm.output_dir,
        )
        self._log_handle(summary)

        tiles = generate_tiles(
            blueprint.dataset_name,
            blueprint.num_files,
            blueprint.sleep_seconds,
            output_dir=fsm.output_dir,
        )
        self._log_handle(tiles)

        stats = compute_stats(
            blueprint.dataset_name,
            blueprint.sleep_seconds,
            output_dir=fsm.output_dir,
        )
        self._log_handle(stats)

        elapsed = time.monotonic() - started
        msg = (
            f"cache_demo completed in {elapsed:.2f}s; stats: {stats.result}; "
            f"outputs in {fsm.output_dir}"
        )
        self.log.info(msg)
        print(msg)

        self.add_state(ExecutionStatus.COMPLETED)
        return self.result


APP_CD_SCHEMA_1_0_0: t.Final[str] = "1.0.0"


cd_bounds = {
    "min": APP_CD_SCHEMA_1_0_0,
    "max": APP_CD_SCHEMA_1_0_0,
}
"""Schema bounds for the cache_demo blueprint schema."""


class CacheDemoSchemaAdapterV1V1(SchemaAdapter):
    """No-op schema migration anchoring the 1.0.0 schema version."""

    @classmethod
    def application(cls) -> str:
        return APP_NAME

    @classmethod
    def source(cls) -> str:
        return APP_CD_SCHEMA_1_0_0

    @classmethod
    def target(cls) -> str:
        # no migration is performed when source == target
        return APP_CD_SCHEMA_1_0_0

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        return {**model}


@register_application
class CacheDemoApplication(
    ApplicationDefinition[CacheDemoBlueprint, CacheDemoRunner],
):
    name = APP_NAME
    long_name = "Artifact cache demonstration"
    runner = CacheDemoRunner
    blueprint = CacheDemoBlueprint
    applicable_transforms = ()
    migrations = (CacheDemoSchemaAdapterV1V1,)
