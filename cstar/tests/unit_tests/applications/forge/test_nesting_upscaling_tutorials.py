"""The committed nesting + upscaling tutorials stay loadable and pinned to release refs.

Each example under ``docs/tutorials`` carries three forge blueprints and a workplan
over them. The blueprints are written once and committed, so a schema bump or a ref
that drifts to a commit hash would otherwise show up only when someone runs the example.
"""

from pathlib import Path

import pytest
import yaml

from cstar.applications.forge.blueprint import FORGE_BLUEPRINT_VERSION, ForgeBlueprint
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize
from cstar.roms.namelist import is_commit_hash

EXAMPLES = ("nesting_upscaling_tiny", "nesting_upscaling_ccs")
FORGE_STEPS = ("forge_parent", "forge_child", "forge_parent_up")
PINNED_REPOS = ("roms", "marbl", "pio")
"""The code repositories whose refs must be names; the templates commit comes from the ModelSpec."""


@pytest.fixture
def tutorials(package_path: Path) -> Path:
    return package_path / "docs" / "tutorials"


@pytest.mark.parametrize("example", EXAMPLES)
@pytest.mark.parametrize("step", FORGE_STEPS)
def test_blueprint_loads_at_the_current_schema(
    tutorials: Path, example: str, step: str
) -> None:
    """The blueprint validates, and its file declares the current forge blueprint version.

    ``from_yaml`` migrates an older file silently, so the declared version is read from
    the file: a bump of ``FORGE_BLUEPRINT_VERSION`` then asks for the tutorials to be
    regenerated rather than leaving them to migrate on every load.
    """
    path = tutorials / example / f"{step}.yaml"
    declared = yaml.safe_load(path.read_text())["forge_blueprint_version"]
    assert declared == FORGE_BLUEPRINT_VERSION

    assert ForgeBlueprint.from_yaml(path).application == "forge"


@pytest.mark.parametrize("example", EXAMPLES)
@pytest.mark.parametrize("step", FORGE_STEPS)
@pytest.mark.parametrize("repo", PINNED_REPOS)
def test_code_refs_are_names_not_commit_hashes(
    tutorials: Path, example: str, step: str, repo: str
) -> None:
    """The ucla-roms, MARBL and PIO refs are tags or branches, never a commit hash.

    For ucla-roms a hash makes C-Star clone the repository to find the release the commit
    descends from, and warn that the namelist schema of that release may not match; a tag
    does neither.
    """
    code = ForgeBlueprint.from_yaml(tutorials / example / f"{step}.yaml").code
    section = getattr(code, repo)
    assert section is not None, f"code.{repo} is missing"
    ref = section.commit
    assert ref, f"code.{repo} has no commit"
    assert not is_commit_hash(ref), f"code.{repo}.commit is the hash {ref}"


@pytest.mark.parametrize("example", EXAMPLES)
def test_workplan_parses_and_names_existing_blueprints(
    tutorials: Path, example: str
) -> None:
    """The workplan validates without runtime variables, and its forge steps' files exist.

    The blueprint paths in the workplan are relative to the workplan itself. Nothing is
    filled in or run: the ``{{...}}`` placeholders stay as written.
    """
    workplan_path = tutorials / example / "workplan.yaml"
    workplan = deserialize(workplan_path, Workplan)

    forge = [s for s in workplan.steps if s.application == "forge"]
    assert [s.name for s in forge] == list(FORGE_STEPS)
    missing = [
        s.blueprint_path
        for s in forge
        if not (workplan_path.parent / s.blueprint_path).is_file()
    ]
    assert not missing, missing
