"""The committed nesting + upscaling examples stay loadable and pinned to release refs.

Each example (the tiny tutorial under ``docs/tutorials``, the California Current System
case under ``docs/developers``) carries three forge blueprints and a workplan over them.
The blueprints are written once and committed, so a schema bump or a ref that drifts to
a commit hash would otherwise show up only when someone runs the example.
"""

import re
import typing as t
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from cstar.applications.cdr_metrics import CdrDomain
from cstar.applications.forge.blueprint import FORGE_BLUEPRINT_VERSION, ForgeBlueprint
from cstar.applications.roms_marbl.outputs import RestartFile
from cstar.applications.roms_marbl.transforms import ContinuanceDirective
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize
from cstar.roms.namelist import is_commit_hash

TINY = "tutorials/nesting_upscaling_tiny"
EXAMPLES = (TINY, "developers/nesting_upscaling_ccs")
"""The example directories, relative to ``docs``."""
FORGE_STEPS = ("forge_parent", "forge_child", "forge_parent_up")
PINNED_REPOS = ("roms", "marbl", "pio")
"""The code repositories whose refs must be names; the templates commit comes from the ModelSpec."""


@pytest.fixture
def docs(package_path: Path) -> Path:
    return package_path / "docs"


@pytest.mark.parametrize("example", EXAMPLES)
@pytest.mark.parametrize("step", FORGE_STEPS)
def test_blueprint_loads_at_the_current_schema(
    docs: Path, example: str, step: str
) -> None:
    """The blueprint validates, and its file declares the current forge blueprint version.

    ``from_yaml`` migrates an older file silently, so the declared version is read from
    the file: a bump of ``FORGE_BLUEPRINT_VERSION`` then asks for the tutorials to be
    regenerated rather than leaving them to migrate on every load.
    """
    path = docs / example / f"{step}.yaml"
    declared = yaml.safe_load(path.read_text())["forge_blueprint_version"]
    assert declared == FORGE_BLUEPRINT_VERSION

    assert ForgeBlueprint.from_yaml(path).application == "forge"


@pytest.mark.parametrize("example", EXAMPLES)
@pytest.mark.parametrize("step", FORGE_STEPS)
@pytest.mark.parametrize("repo", PINNED_REPOS)
def test_code_refs_are_names_not_commit_hashes(
    docs: Path, example: str, step: str, repo: str
) -> None:
    """The ucla-roms, MARBL and PIO refs are tags or branches, never a commit hash.

    For ucla-roms a hash makes C-Star clone the repository to find the release the commit
    descends from, and warn that the namelist schema of that release may not match; a tag
    does neither.
    """
    code = ForgeBlueprint.from_yaml(docs / example / f"{step}.yaml").code
    section = getattr(code, repo)
    assert section is not None, f"code.{repo} is missing"
    ref = section.commit
    assert ref, f"code.{repo} has no commit"
    assert not is_commit_hash(ref), f"code.{repo}.commit is the hash {ref}"


@pytest.mark.parametrize("example", EXAMPLES)
def test_workplan_parses_and_names_existing_blueprints(
    docs: Path, example: str
) -> None:
    """The workplan validates without runtime variables, and its forge steps' files exist.

    The blueprint paths in the workplan are relative to the workplan itself. Nothing is
    filled in or run: the ``{{...}}`` placeholders stay as written.
    """
    workplan_path = docs / example / "workplan.yaml"
    workplan = deserialize(workplan_path, Workplan)

    forge = [s for s in workplan.steps if s.application == "forge"]
    assert [s.name for s in forge] == list(FORGE_STEPS)
    missing = [
        s.blueprint_path
        for s in forge
        if not (workplan_path.parent / s.blueprint_path).is_file()
    ]
    assert not missing, missing


@pytest.mark.parametrize("example", EXAMPLES)
def test_cdr_metrics_step_assigns_the_source_roles(docs: Path, example: str) -> None:
    """``child`` is the release and ``parent_up`` only transfers its alkalinity.

    A role swapped or dropped would count the release twice, or not at all, in the
    combined metrics. Each domain is also analysed on the grid its own forge step wrote.
    """
    workplan = deserialize(docs / example / "workplan.yaml", Workplan)
    (step,) = [s for s in workplan.steps if s.name == "cdr_metrics"]
    domains = t.cast("dict[str, dict[str, t.Any]]", step.blueprint_overrides["domains"])

    assert set(domains) == {"child", "parent_up"}
    default_role = CdrDomain.model_fields["source_role"].default
    assert domains["child"].get("source_role", default_role) == "release"
    assert domains["parent_up"]["source_role"] == "transfer"
    assert domains["child"]["grid"]["location"].startswith("{{root_dir: forge_child}}/")
    assert domains["parent_up"]["grid"]["location"].startswith(
        "{{root_dir: forge_parent_up}}/"
    )


def test_tiny_workplan_starts_the_child_where_its_blueprint_does(
    docs: Path,
) -> None:
    """The restart ``nest_ic`` reads and the one ``parent_up`` continues from are the child's start.

    The tiny workplan names that restart by hand, in the file stamp of ``parent_rst`` and
    in the ``timestamp`` of ``parent_up``'s ``continue-from``. If the child's
    ``run.start_date`` moves and these do not, the child starts from the wrong parent
    state, and ``parent_up`` from a different time than the child.
    """
    example = docs / TINY
    child = ForgeBlueprint.from_yaml(example / "forge_child.yaml")
    workplan = deserialize(example / "workplan.yaml", Workplan)
    steps = {s.name: s for s in workplan.steps}

    parent_rst = str(steps["nest_ic"].blueprint_overrides["parent_rst"])
    match = re.search(r"output_rst\.(\d{14})\.nc$", parent_rst)
    assert match, parent_rst
    assert datetime.strptime(match[1], RestartFile.FMT_TS) == child.run.start_date

    continue_from = t.cast(
        "dict[str, t.Any]", steps["parent_up"].directives[ContinuanceDirective.key()]
    )
    timestamp = datetime.fromisoformat(
        str(continue_from[ContinuanceDirective.KEY_TIMESTAMP])
    )
    assert timestamp == child.run.start_date


def test_tiny_parent_up_ends_with_the_child(docs: Path) -> None:
    """``parent_up`` runs to the child's end, so both have a record at the last time.

    The combined metrics' final record is finite only if both domains reach it.
    """
    example = docs / TINY
    child = ForgeBlueprint.from_yaml(example / "forge_child.yaml")
    parent_up = ForgeBlueprint.from_yaml(example / "forge_parent_up.yaml")

    assert parent_up.run.end_date == child.run.end_date


@pytest.mark.parametrize("step", FORGE_STEPS)
def test_tiny_blueprints_leave_the_sources_to_forge(docs: Path, step: str) -> None:
    """No source of the tiny tutorial is a local file: Forge downloads or streams each one.

    Readers run the tutorial from the files they download from its page, so a source
    path would name a file they do not have. Forge stages exactly the datasets that
    ``datasets`` lists, so it must hold every source the resolver snapshotted.
    """
    path = docs / TINY / f"{step}.yaml"
    paths = re.findall(r"^\s+path: (.+)$", path.read_text(), flags=re.M)
    assert paths, "no source paths found"
    assert set(paths) == {"null"}, paths

    blueprint = ForgeBlueprint.from_yaml(path)
    staged = {rd.dataset_key for rd in blueprint.forcing.resolved_datasets.values()}
    assert sorted(staged) == blueprint.datasets


CDR_TUTORIAL = "tutorials"
"""The spin-up, CDR release and metrics tutorial: two forge blueprints and a workplan."""
CDR_FORGE_FILES = {
    "make_spinup": "forge_blueprint_wio_spinup.yaml",
    "make_release": "forge_blueprint_wio_release.yaml",
}


@pytest.mark.parametrize("name", CDR_FORGE_FILES.values())
def test_cdr_tutorial_blueprint_loads_at_the_current_schema(
    docs: Path, name: str
) -> None:
    """Each blueprint validates and declares the current forge blueprint version."""
    path = docs / CDR_TUTORIAL / name
    declared = yaml.safe_load(path.read_text())["forge_blueprint_version"]
    assert declared == FORGE_BLUEPRINT_VERSION

    blueprint = ForgeBlueprint.from_yaml(path)
    assert blueprint.application == "forge"
    for repo in PINNED_REPOS:
        ref = getattr(blueprint.code, repo).commit
        assert ref and not is_commit_hash(ref), f"code.{repo}.commit is {ref!r}"


def test_cdr_tutorial_workplan_names_its_blueprints(docs: Path) -> None:
    """The workplan's forge steps point at the two committed blueprints, by relative path."""
    workplan_path = docs / CDR_TUTORIAL / "workplan_cdr.yaml"
    workplan = deserialize(workplan_path, Workplan)
    forge = {
        s.name: s.blueprint_path for s in workplan.steps if s.application == "forge"
    }
    assert {k: Path(v).name for k, v in forge.items()} == CDR_FORGE_FILES
    assert all((workplan_path.parent / v).is_file() for v in forge.values())


def test_cdr_tutorial_release_continues_where_the_spinup_ends(docs: Path) -> None:
    """``release`` continues from ``spinup``'s last restart, which is the release blueprint's start.

    The directive sets the start date from the restart it finds, so a spin-up that ends
    elsewhere would quietly shift the release run.
    """
    spinup = ForgeBlueprint.from_yaml(
        docs / CDR_TUTORIAL / CDR_FORGE_FILES["make_spinup"]
    )
    release = ForgeBlueprint.from_yaml(
        docs / CDR_TUTORIAL / CDR_FORGE_FILES["make_release"]
    )
    assert spinup.run.end_date == release.run.start_date
    assert spinup.cdr.mode == "none"
    assert release.cdr.mode == "simple"

    workplan = deserialize(docs / CDR_TUTORIAL / "workplan_cdr.yaml", Workplan)
    steps = {s.name: s for s in workplan.steps}
    continue_from = t.cast(
        "dict[str, t.Any]", steps["release"].directives[ContinuanceDirective.key()]
    )
    assert continue_from == {"step": "spinup"}
    domains = t.cast(
        "dict[str, dict[str, t.Any]]", steps["metrics"].blueprint_overrides["domains"]
    )
    (domain,) = domains.values()
    assert domain["grid"]["location"].startswith("{{root_dir: make_release}}/")
    assert domain["sources"] == [{"location": "{{output_dir: release}}"}]
