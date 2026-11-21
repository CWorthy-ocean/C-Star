"""Shared fixtures for CLI tests."""

import os
import re
import typing as t
from collections.abc import Callable, Generator
from pathlib import Path
from unittest import mock

import pytest
import yaml

import cstar
import cstar.catalog
from cstar.base.env import (
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_PROJECT_HOME,
    REMOVED_SETTINGS,
)
from cstar.execution.file_system import StateDirectoryManager
from cstar.tests.unit_tests.orchestration.manifest_runs import (
    BuiltRun,
    build_manifest_run,
)

_ANSI_CODES = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

_DOCS = Path(cstar.__file__).parent.parent / "docs"
_BUNDLED_FORGE = (
    Path(cstar.catalog.__file__).parent / "bundled" / "blueprints" / "forge"
)

_SHIPPED_FORGE_BLUEPRINTS = sorted(
    [
        *_BUNDLED_FORGE.glob("*.yaml"),
        *_DOCS.rglob("forge_*.yaml"),
        *_DOCS.rglob("forge-blueprint-*.yaml"),
    ]
)
"""The forge blueprints C-Star ships for users to check and run."""


@pytest.fixture(
    params=_SHIPPED_FORGE_BLUEPRINTS,
    ids=[path.name for path in _SHIPPED_FORGE_BLUEPRINTS],
)
def shipped_forge_blueprint(request: pytest.FixtureRequest) -> Path:
    """Each forge blueprint C-Star ships, bundled or in the docs."""
    return t.cast("Path", request.param)


def _misspelled_section(data: dict[str, t.Any]) -> None:
    data["model_settings"]["botom_drag"] = {"rdrg2": 1.0e-3}


def _cdr_window_ends_early(data: dict[str, t.Any]) -> None:
    data["cdr"]["cdr_forcing"]["end_time"] = "2012-01-04T00:00:00"


FORGE_REFUSALS: dict[str, tuple[Path, Callable[[dict[str, t.Any]], None], str]] = {
    "misspelled section": (
        _BUNDLED_FORGE / "wio-toy-example.yaml",
        _misspelled_section,
        "botom_drag (did you mean bottom_drag?)",
    ),
    "CDR window ends before the run": (
        _DOCS / "tutorials" / "forge_blueprint_wio_release.yaml",
        _cdr_window_ends_early,
        "the CDR window ends at 2012-01-04 00:00:00 (cdr_forcing.end_time), before "
        "the run ends",
    ),
}
"""A shipped forge blueprint, an edit Forge refuses, and the text naming it."""


@pytest.fixture(params=sorted(FORGE_REFUSALS))
def refused_forge_blueprint(
    request: pytest.FixtureRequest, tmp_path: Path
) -> tuple[Path, str]:
    """Write a schema-valid forge blueprint that Forge refuses before any data.

    Returns
    -------
    tuple[Path, str]
        The blueprint file and the text the refusal must contain.
    """
    source, edit, expected = FORGE_REFUSALS[request.param]
    data = yaml.safe_load(source.read_text())
    edit(data)
    path = tmp_path / source.name
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path, expected


class FakeEntryPoint:
    """Stand-in for importlib.metadata.EntryPoint with a controllable load()."""

    def __init__(self, name, loaded=None, error=None):
        self.name = name
        self.value = f"fake_pkg.cli:{name}"
        self._loaded = loaded
        self._error = error

    def load(self):
        if self._error is not None:
            raise self._error
        return self._loaded


@pytest.fixture(autouse=True)
def disable_cli_color() -> Generator[None]:
    """Force plain-text CLI output from typer/rich for every CLI test.

    CI agents enable color output (typer forces a color terminal when
    `GITHUB_ACTIONS`, `FORCE_COLOR`, or `PY_COLORS` is set), which embeds ANSI
    escape codes in captured CLI output and breaks message assertions.
    """
    # typer reads GITHUB_ACTIONS/FORCE_COLOR once, when typer.rich_utils is first
    # imported, and bakes the result into FORCE_TERMINAL. A test outside this
    # directory can trigger that import with colour still forced, so pin the
    # module constant as well as the environment.
    import typer.rich_utils

    with (
        mock.patch.dict(os.environ, {"NO_COLOR": "1"}),
        mock.patch.object(typer.rich_utils, "FORCE_TERMINAL", None),
    ):
        for var in ("FORCE_COLOR", "PY_COLORS", "GITHUB_ACTIONS"):
            os.environ.pop(var, None)
        yield


def _flatten_cli_output(text: str) -> str:
    """Prepare CLI output for phrase matching: remove ANSI color codes (CI
    environments force color even under `color=False`) and rich panel
    decoration, and collapse whitespace to single spaces.
    """
    return " ".join(_ANSI_CODES.sub("", text).replace("│", " ").split())


def _squash_cli_output(text: str) -> str:
    """Prepare CLI output for path matching: additionally remove ALL
    whitespace, since rich wraps long paths at arbitrary points.
    """
    return "".join(_flatten_cli_output(text).split())


@pytest.fixture
def flatten_cli_output() -> Callable[[str], str]:
    """Fixture providing a helper that prepares CLI output for phrase matching.

    `rich` wraps long paths at the terminal width, so normalize whitespace
    before matching
    """
    return _flatten_cli_output


@pytest.fixture
def squash_cli_output() -> Callable[[str], str]:
    """Fixture providing a helper that prepares CLI output for path matching."""
    return _squash_cli_output


@pytest.fixture(autouse=True)
def clean_flag_env() -> Generator[None]:
    """Remove settings exported in the shell that would change CLI test behavior.

    A value of `CSTAR_DISABLE_MIGRATION` changes what the commands do, and an
    exported removed setting adds a warning to stderr. Tests opt back in with
    their own `mock.patch.dict`.
    """
    with mock.patch.dict(os.environ):
        for flag in (ENV_CSTAR_DISABLE_MIGRATION, *REMOVED_SETTINGS):
            os.environ.pop(flag, None)
        yield


@pytest.fixture
def readonly_blueprint(tmp_path: Path, hello_world_bp_content: str) -> Generator[Path]:
    """A hello-world blueprint in a directory its user cannot write to.

    Like a blueprint in a shared catalog, the directory takes no new file; it is
    made writable again when the test ends.
    """
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    blueprint = catalog / "helloworld.yaml"
    blueprint.write_text(hello_world_bp_content)
    catalog.chmod(0o555)
    try:
        yield blueprint
    finally:
        catalog.chmod(0o755)


@pytest.fixture
def project_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the project home at a temporary directory; return its runs area.

    The area does not exist until a test puts a run in it.
    """
    project = (tmp_path / "project").resolve()
    monkeypatch.setenv(ENV_CSTAR_PROJECT_HOME, str(project))
    return project / "cstar" / "runs"


@pytest.fixture
def build_run(hello_world_bp_path: Path) -> Callable[..., BuiltRun]:
    """Build a run directory that only its manifest names; see `build_manifest_run`."""

    def build(root: Path, run_id: str, **kwargs: t.Any) -> BuiltRun:
        return build_manifest_run(root, run_id, hello_world_bp_path, **kwargs)

    return build


@pytest.fixture
def local_run(build_run: Callable[..., BuiltRun]) -> Callable[..., BuiltRun]:
    """Build a run only its manifest names in the data home."""

    def build(run_id: str, **kwargs: t.Any) -> BuiltRun:
        root = StateDirectoryManager.workplan_runs_dir() / run_id
        return build_run(root, run_id, **kwargs)

    return build


@pytest.fixture
def project_run(
    build_run: Callable[..., BuiltRun], project_runs: Path
) -> Callable[..., BuiltRun]:
    """Build a run in the project's runs area."""

    def build(run_id: str, **kwargs: t.Any) -> BuiltRun:
        return build_run(project_runs / run_id, run_id, **kwargs)

    return build
