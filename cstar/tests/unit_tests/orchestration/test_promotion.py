"""Tests for the copy plan `cstar run promote` makes of a run, and its execution."""

import os
import shutil
import typing as t
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from cstar.applications.roms_marbl.file_system import RomsFileSystemManager
from cstar.base.env import ENV_CSTAR_PROJECT_HOME
from cstar.base.exceptions import CstarError
from cstar.orchestration.manifest import (
    ATTEMPTS_DIR_NAME,
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    Product,
    RunManifest,
    StepRecord,
    fold_step_records,
    read_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.promotion import (
    GROUP_ARTIFACTS,
    GROUP_EXTRAS,
    CopyPlan,
    DestinationState,
    Entry,
    Include,
    Mode,
    destination_state,
    execute_plan,
    link_run,
    plan_promotion,
    product_group,
    remove_run,
    staging_dir,
)
from cstar.tests.unit_tests.orchestration.manifest_runs import (
    STARTED,
    BuiltRun,
    build_manifest_run,
)

RUN_ID: t.Final[str] = "promoted"
FIRST: t.Final[str] = "Spin Up"
SECOND: t.Final[str] = "run"
CONTENT: t.Final[str] = "0123456789"


@dataclass(frozen=True)
class Planned:
    """A run with products, and the plan made of it."""

    built: BuiltRun
    manifest: RunManifest

    @property
    def root(self) -> Path:
        return self.built.root

    def plan(self, *include: Include) -> CopyPlan:
        return plan_promotion(self.root, self.manifest, include=include)


@pytest.fixture
def build_run(hello_world_bp_path: Path) -> Callable[..., BuiltRun]:
    """Build a run directory that only its manifest names."""

    def build(root: Path, run_id: str, **kwargs: t.Any) -> BuiltRun:
        return build_manifest_run(root, run_id, hello_world_bp_path, **kwargs)

    return build


def write(path: Path, text: str = CONTENT) -> Path:
    """Write a file, and the directories above it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def record_products(step_root: Path, step: str, products: Sequence[str]) -> None:
    """Write a completed step record that lists `products`."""
    write_step_record(
        step_root,
        StepRecord(
            step=step,
            application="hello_world",
            blueprint=BlueprintIdentity(
                kind="Blueprint",
                application="hello_world",
                name="bp",
                content_hash="abc",
            ),
            outcome="completed",
            finished_at=STARTED,
            products=[Product(kind="output", path=path) for path in products],
        ),
    )


def relatives(entries: Sequence[Entry]) -> set[str]:
    """The posix paths of the entries."""
    return {entry.relative.as_posix() for entry in entries}


@pytest.fixture
def planned(build_run: Callable[..., BuiltRun], tmp_path: Path) -> Planned:
    """A run whose first step has two products and a missing one, and whose second has one."""
    built = build_run(tmp_path / "source" / RUN_ID, RUN_ID)
    root = built.root
    first, second = built.step_dir(FIRST), built.step_dir(SECOND)

    write(first / "output/a.nc", "a" * 5)
    write(first / "output/b.nc", "b" * 7)
    write(first / "output/not_a_product.nc")
    write(first / "logs/x.out")
    write(first / "work/a.ovrd.yaml")
    write(first / "work/notes.txt")
    write(first / "input/data.nc")
    write(second / "output/c.nc", "c" * 3)
    record_products(first, FIRST, ["output/a.nc", "output/b.nc", "output/gone.nc"])
    record_products(second, SECOND, ["output/c.nc"])

    write(root / "attempts/20261015T120000/workplan.yaml")
    write(root / f"{RUN_ID}_original.bak")

    manifest = read_run_manifest(root)
    assert manifest is not None
    fold_step_records(manifest, root)
    return Planned(built, manifest)


def add_roms_step_files(planned: Planned) -> Path:
    """Give the first step the large directories of a ROMS step."""
    step = planned.built.step_dir(FIRST)
    fs = RomsFileSystemManager(step)
    write(fs.compile_time_code_dir / "Makefile")
    write(fs.runtime_code_dir / "namelist.nml")
    write(fs.codebases_dir / "ucla-roms" / "src" / "main.F")
    write(fs.temp_output_dir / "piece.000.nc")
    return step


def test_artifacts_hold_the_small_files_that_describe_the_run(
    planned: Planned,
) -> None:
    """The artifacts are the manifest, workplans, attempts and per-step records, logs and work files."""
    artifacts = relatives(planned.plan().groups[GROUP_ARTIFACTS])
    first, second = (f"tasks/{name}" for name in ("spin-up", "run"))

    assert {
        RUN_MANIFEST_NAME,
        f"{RUN_ID}_transformed.yaml",
        f"{RUN_ID}_original.bak",
        f"{ATTEMPTS_DIR_NAME}/20261015T120000/workplan.yaml",
        f"{first}/{STEP_RECORD_NAME}",
        f"{first}/logs/x.out",
        f"{first}/work/a.ovrd.yaml",
        f"{second}/{STEP_RECORD_NAME}",
    } <= artifacts
    assert not [a for a in artifacts if "/output/" in a or "/input/" in a]
    assert f"{first}/work/notes.txt" not in artifacts


def test_artifacts_lead_with_the_manifest(planned: Planned) -> None:
    """The manifest comes first, so a copy stopped early still names the run."""
    assert planned.plan().groups[GROUP_ARTIFACTS][0].relative == Path(RUN_MANIFEST_NAME)


def test_products_are_grouped_by_step_in_manifest_order(planned: Planned) -> None:
    """Each step with a record has its own group of the products it recorded."""
    plan = planned.plan()

    assert list(plan.groups) == [
        GROUP_ARTIFACTS,
        product_group(FIRST),
        product_group(SECOND),
    ]
    first = plan.groups[product_group(FIRST)]
    assert [(e.relative.name, e.size_bytes, e.kind) for e in first] == [
        ("a.nc", 5, "file"),
        ("b.nc", 7, "file"),
    ]
    assert [e.size_bytes for e in plan.groups[product_group(SECOND)]] == [3]


def test_a_missing_product_is_reported_and_not_planned(planned: Planned) -> None:
    """A recorded product that is not on disk is only listed as missing."""
    plan = planned.plan()
    gone = Path("tasks/spin-up/output/gone.nc")

    assert plan.missing_products == (gone,)
    assert gone not in {e.relative for e in plan.entries}


def test_a_step_without_a_record_has_no_group(
    build_run: Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """Only steps with a record have a product group."""
    built = build_run(tmp_path / RUN_ID, RUN_ID)
    manifest = read_run_manifest(built.root)
    assert manifest is not None

    plan = plan_promotion(built.root, manifest)

    assert list(plan.groups) == [GROUP_ARTIFACTS]


def test_a_product_leaving_the_run_is_refused(planned: Planned) -> None:
    """A recorded path cannot make the plan copy a file outside the run."""
    step = planned.manifest.steps[0]
    assert step.record is not None
    step.record.products[0].path = "../../../../outside.nc"

    with pytest.raises(CstarError, match="leaves the run directory"):
        planned.plan()


def test_extras_are_not_included_by_default(planned: Planned) -> None:
    """Without `include`, the compile-time code and temp output stay behind."""
    add_roms_step_files(planned)

    plan = planned.plan()

    assert GROUP_EXTRAS not in plan.groups
    assert not [e for e in plan.entries if "compile_time_code" in e.relative.parts]


def test_builds_add_each_steps_code_and_codebases(planned: Planned) -> None:
    """`builds` holds the compile-time code, runtime code and codebases, not temp output."""
    step = add_roms_step_files(planned)
    base = step.relative_to(planned.root).as_posix()

    extras = relatives(planned.plan(Include.builds).groups[GROUP_EXTRAS])

    assert {
        f"{base}/input/compile_time_code/Makefile",
        f"{base}/input/runtime_code/namelist.nml",
        f"{base}/input/codebases/ucla-roms/src/main.F",
    } <= extras
    assert not [e for e in extras if "temp_output" in e]
    assert f"{base}/input/data.nc" not in extras


def test_temp_output_adds_only_the_pieces(planned: Planned) -> None:
    """`temp-output` holds the per-rank pieces and none of the builds."""
    step = add_roms_step_files(planned)
    base = step.relative_to(planned.root).as_posix()

    extras = relatives(planned.plan(Include.temp_output).groups[GROUP_EXTRAS])

    assert f"{base}/temp_output/piece.000.nc" in extras
    assert not [e for e in extras if "/input/" in e]


def test_all_adds_the_whole_run(planned: Planned) -> None:
    """`all` includes what no other group planned, such as a stray output file."""
    step = add_roms_step_files(planned)
    write(step / "output/extra.txt")
    base = step.relative_to(planned.root).as_posix()

    extras = relatives(planned.plan(Include.all).groups[GROUP_EXTRAS])

    assert {
        f"{base}/output/extra.txt",
        f"{base}/input/data.nc",
        f"{base}/temp_output/piece.000.nc",
        f"{base}/input/compile_time_code/Makefile",
    } <= extras
    assert RUN_MANIFEST_NAME not in extras


def test_a_product_in_an_extra_is_planned_once(planned: Planned) -> None:
    """A product under `compile_time_code` stays in its product group, not the extras."""
    step = planned.built.step_dir(FIRST)
    exe = RomsFileSystemManager(step).compile_time_code_dir / "roms"
    write(exe)
    write(RomsFileSystemManager(step).compile_time_code_dir / "Makefile")
    product = exe.relative_to(step).as_posix()
    step_entry = planned.manifest.steps[0]
    assert step_entry.record is not None
    step_entry.record.products.append(Product(kind="executable", path=product))

    plan = planned.plan(Include.builds, Include.all)
    target = exe.relative_to(planned.root)

    assert [e.relative for e in plan.entries].count(target) == 1
    assert target in {e.relative for e in plan.groups[product_group(FIRST)]}
    assert target not in {e.relative for e in plan.groups[GROUP_EXTRAS]}


def test_directories_hold_every_step_working_dir(planned: Planned) -> None:
    """Each step's directory is created even when the copy puts nothing in it."""
    assert set(planned.plan().directories) == {
        Path("tasks/spin-up"),
        Path("tasks/run"),
    }


def test_blueprint_run_has_no_directories_to_create(
    build_run: Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """A blueprint run works in its root, which is the copy itself."""
    built = build_run(tmp_path / RUN_ID, RUN_ID, kind="blueprint", steps=("run",))
    manifest = read_run_manifest(built.root)
    assert manifest is not None

    assert plan_promotion(built.root, manifest).directories == ()


def test_totals_add_up(planned: Planned) -> None:
    """`total_bytes` and `file_count` are sums over the entries."""
    plan = planned.plan()

    assert plan.total_bytes == sum(e.size_bytes for e in plan.entries)
    assert plan.file_count == len([e for e in plan.entries if e.kind == "file"])
    assert plan.total_bytes >= 5 + 7 + 3


def test_a_symlinked_root_is_walked_as_a_directory(planned: Planned) -> None:
    """A run reached through a link, as a `--mode link` destination is, lists its files."""
    link = planned.root.parent / "link-to-run"
    link.symlink_to(planned.root, target_is_directory=True)

    plan = plan_promotion(link, planned.manifest, include={Include.all})

    assert "tasks/spin-up/output/not_a_product.nc" in relatives(plan.entries)
    assert RUN_MANIFEST_NAME in relatives(plan.entries)


def test_a_symlink_in_an_extra_is_not_descended(planned: Planned) -> None:
    """A link, to a file or a directory, is one entry of size 0, and nothing under it is planned."""
    step = add_roms_step_files(planned)
    codebases = RomsFileSystemManager(step).codebases_dir
    elsewhere = write(planned.root.parent / "elsewhere" / "big.nc")
    (codebases / "file-link").symlink_to(elsewhere)
    (codebases / "dir-link").symlink_to(elsewhere.parent, target_is_directory=True)

    plan = planned.plan(Include.builds)
    by_name = {e.relative.name: e for e in plan.entries}

    for name in ("file-link", "dir-link"):
        assert (by_name[name].kind, by_name[name].size_bytes) == ("link", 0)
    assert "big.nc" not in by_name


def test_a_symlinked_product_is_planned_as_the_file(planned: Planned) -> None:
    """A product that is a link counts as a file with its target's size."""
    step = planned.built.step_dir(SECOND)
    target = write(planned.root.parent / "elsewhere" / "real.nc", "r" * 11)
    (step / "output/linked.nc").symlink_to(target)
    entry = planned.manifest.steps[1]
    assert entry.record is not None
    entry.record.products.append(Product(kind="output", path="output/linked.nc"))

    found = {e.relative.name: e for e in planned.plan().entries}["linked.nc"]

    assert (found.kind, found.size_bytes) == ("file", 11)


def test_a_dangling_product_link_is_missing(planned: Planned) -> None:
    """A product whose link target is gone is a missing product."""
    step = planned.built.step_dir(SECOND)
    (step / "output/dangling.nc").symlink_to(planned.root / "nowhere.nc")
    entry = planned.manifest.steps[1]
    assert entry.record is not None
    entry.record.products.append(Product(kind="output", path="output/dangling.nc"))

    assert Path("tasks/run/output/dangling.nc") in planned.plan().missing_products


def test_planning_never_reads_file_contents(
    planned: Planned, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plan is made from stat calls alone."""
    add_roms_step_files(planned)

    def refuse(*args: t.Any, **kwargs: t.Any) -> t.NoReturn:
        raise AssertionError("opened a file")

    monkeypatch.setattr("builtins.open", refuse)
    monkeypatch.setattr(Path, "read_bytes", refuse)
    monkeypatch.setattr(Path, "read_text", refuse)

    assert planned.plan(Include.all).file_count


def test_modes_and_includes_have_their_cli_spellings() -> None:
    """The enums carry the values the command line spells."""
    assert [m.value for m in Mode] == ["copy", "link", "move", "materialize"]
    assert [i.value for i in Include] == ["builds", "temp-output", "all"]


# destination_state


def manifest_dir(path: Path, uuid: str, build_run: Callable[..., BuiltRun]) -> Path:
    """Build a run with the given uuid at `path`."""
    return build_run(path, path.name, uuid=uuid).root


def test_destination_free(tmp_path: Path) -> None:
    """Nothing there."""
    assert destination_state(tmp_path / "new", uuid="u") == DestinationState.free


def test_destination_link_to_run(
    tmp_path: Path, build_run: Callable[..., BuiltRun]
) -> None:
    """A symlink to a directory that holds this run."""
    run = manifest_dir(tmp_path / "run", "u-1", build_run)
    (tmp_path / "link").symlink_to(run, target_is_directory=True)

    state = destination_state(tmp_path / "link", uuid="u-1")

    assert state == DestinationState.link_to_run


def test_destination_promoted(
    tmp_path: Path, build_run: Callable[..., BuiltRun]
) -> None:
    """A real directory that holds this run."""
    run = manifest_dir(tmp_path / "run", "u-1", build_run)

    assert destination_state(run, uuid="u-1") == DestinationState.promoted


def test_destination_other_run(
    tmp_path: Path, build_run: Callable[..., BuiltRun]
) -> None:
    """A directory that holds a different run, directly or through a link."""
    run = manifest_dir(tmp_path / "run", "u-1", build_run)
    (tmp_path / "link").symlink_to(run, target_is_directory=True)

    assert destination_state(run, uuid="u-2") == DestinationState.other_run
    assert (
        destination_state(tmp_path / "link", uuid="u-2") == DestinationState.other_run
    )


def test_destination_occupied(tmp_path: Path) -> None:
    """An empty directory, a plain file, a dangling link and an unreadable manifest hold no run."""
    (tmp_path / "empty").mkdir()
    write(tmp_path / "file")
    (tmp_path / "dangling").symlink_to(tmp_path / "nowhere")
    write(tmp_path / "garbled" / RUN_MANIFEST_NAME, ": : not a manifest [")

    for name in ("empty", "file", "dangling", "garbled"):
        state = destination_state(tmp_path / name, uuid="u")
        assert state == DestinationState.occupied, name


# execute_plan


@pytest.fixture
def destination(tmp_path: Path) -> Path:
    """Where the run is promoted to; its parent does not exist yet."""
    return tmp_path / "project" / "runs" / RUN_ID


def test_copy_reproduces_the_plan(planned: Planned, destination: Path) -> None:
    """Every planned entry exists in the copy, with the step directories."""
    add_roms_step_files(planned)
    (planned.built.step_dir(SECOND) / "output/c.nc").parent.mkdir(exist_ok=True)
    plan = planned.plan(Include.all)

    copied = execute_plan(plan, destination)

    assert copied == plan.total_bytes
    for entry in plan.entries:
        assert os.path.lexists(destination / entry.relative), entry.relative
    for directory in plan.directories:
        assert (destination / directory).is_dir()
    assert (destination / "tasks/spin-up/output/a.nc").read_text() == "a" * 5
    assert not staging_dir(destination).exists()


def test_empty_step_directories_are_created(
    build_run: Callable[..., BuiltRun], tmp_path: Path, destination: Path
) -> None:
    """A step that left nothing still has its directory, so `check` finds it."""
    built = build_run(tmp_path / RUN_ID, RUN_ID)
    manifest = read_run_manifest(built.root)
    assert manifest is not None

    execute_plan(plan_promotion(built.root, manifest), destination)

    assert (destination / "tasks/spin-up").is_dir()
    assert (destination / "tasks/run").is_dir()


def test_a_relative_link_keeps_its_text(planned: Planned, destination: Path) -> None:
    """A link inside the run to a relative path is recreated as it was."""
    step = add_roms_step_files(planned)
    codebases = RomsFileSystemManager(step).codebases_dir
    (codebases / "rel").symlink_to(Path("ucla-roms") / "src" / "main.F")

    execute_plan(planned.plan(Include.builds), destination)

    copy = destination / codebases.relative_to(planned.root) / "rel"
    assert os.readlink(copy) == os.path.join("ucla-roms", "src", "main.F")
    assert copy.read_text() == CONTENT


def test_an_absolute_link_into_the_run_is_made_relative(
    planned: Planned, destination: Path
) -> None:
    """The copy stays self-contained once the source is gone."""
    step = add_roms_step_files(planned)
    codebases = RomsFileSystemManager(step).codebases_dir
    target = codebases / "ucla-roms" / "src" / "main.F"
    (codebases / "abs").symlink_to(target.resolve())
    plan = planned.plan(Include.builds)

    execute_plan(plan, destination)
    shutil.rmtree(planned.root)

    copy = destination / codebases.relative_to(planned.root) / "abs"
    assert not os.path.isabs(os.readlink(copy))
    assert copy.read_text() == CONTENT


def test_a_relative_link_climbing_inside_the_run_keeps_its_text(
    planned: Planned, destination: Path
) -> None:
    """A `..` that stays inside the run still resolves in the copy."""
    step = add_roms_step_files(planned)
    codebases = RomsFileSystemManager(step).codebases_dir
    (codebases / "nested").mkdir()
    (codebases / "nested" / "up").symlink_to(
        Path("..") / "ucla-roms" / "src" / "main.F"
    )

    execute_plan(planned.plan(Include.builds), destination)

    copy = destination / codebases.relative_to(planned.root) / "nested" / "up"
    assert os.readlink(copy) == os.path.join("..", "ucla-roms", "src", "main.F")
    assert copy.read_text() == CONTENT


def test_a_relative_link_leaving_the_run_becomes_absolute(
    planned: Planned, destination: Path
) -> None:
    """The copy sits elsewhere, so a link that climbs out names its target outright."""
    step = add_roms_step_files(planned)
    codebases = RomsFileSystemManager(step).codebases_dir
    outside = write(planned.root.parent / "outside.txt", "far")
    climb = Path(os.path.relpath(outside, codebases))
    (codebases / "out").symlink_to(climb)
    assert not climb.is_absolute()

    execute_plan(planned.plan(Include.builds), destination)

    copy = destination / codebases.relative_to(planned.root) / "out"
    assert os.path.isabs(os.readlink(copy))
    assert Path(os.readlink(copy)) == outside
    assert copy.read_text() == "far"


def test_an_absolute_link_leaving_the_run_keeps_its_text(
    planned: Planned, destination: Path
) -> None:
    """A link to somewhere else is still a link to it."""
    step = add_roms_step_files(planned)
    codebases = RomsFileSystemManager(step).codebases_dir
    outside = write(planned.root.parent / "elsewhere" / "far.nc")
    (codebases / "far").symlink_to(outside)

    execute_plan(planned.plan(Include.builds), destination)

    copy = destination / codebases.relative_to(planned.root) / "far"
    assert os.readlink(copy) == str(outside)


def test_a_symlinked_product_is_copied_as_a_regular_file(
    planned: Planned, destination: Path
) -> None:
    """The product is the file, so the copy holds its content, not the link."""
    step = planned.built.step_dir(SECOND)
    target = write(planned.root.parent / "elsewhere" / "real.nc", "r" * 11)
    (step / "output/linked.nc").symlink_to(target)
    entry = planned.manifest.steps[1]
    assert entry.record is not None
    entry.record.products.append(Product(kind="output", path="output/linked.nc"))

    copied = execute_plan(planned.plan(), destination)

    copy = destination / "tasks/run/output/linked.nc"
    assert not copy.is_symlink()
    assert copy.read_text() == "r" * 11
    assert copied == planned.plan().total_bytes


def test_an_existing_destination_is_refused(
    planned: Planned, destination: Path
) -> None:
    """Nothing is overwritten, and what was there is untouched."""
    keep = write(destination / "keep.txt", "mine")

    with pytest.raises(CstarError, match="already exists"):
        execute_plan(planned.plan(), destination)

    assert keep.read_text() == "mine"
    assert not staging_dir(destination).exists()


def test_a_leftover_staging_dir_is_refused_and_kept(
    planned: Planned, destination: Path
) -> None:
    """A staging directory of an earlier promotion is for the user to remove."""
    leftover = write(staging_dir(destination) / "partial.txt", "old")

    with pytest.raises(CstarError, match="Remove it"):
        execute_plan(planned.plan(), destination)

    assert leftover.read_text() == "old"
    assert not destination.exists()


def test_a_failed_copy_leaves_nothing(
    planned: Planned, destination: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The staging directory is removed and the destination never appears."""
    real = shutil.copy2
    calls: list[Path] = []

    def flaky(src: t.Any, dst: t.Any, **kwargs: t.Any) -> t.Any:
        calls.append(Path(src))
        if len(calls) == 2:
            raise OSError("disk full")
        return real(src, dst, **kwargs)

    monkeypatch.setattr(shutil, "copy2", flaky)

    with pytest.raises(OSError, match="disk full"):
        execute_plan(planned.plan(), destination)

    assert len(calls) == 2
    assert not staging_dir(destination).exists()
    assert not os.path.lexists(destination)


def test_a_copy_that_does_not_fit_is_refused_before_anything_is_made(
    planned: Planned, destination: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Too little free space leaves neither a staging directory nor a destination."""
    monkeypatch.setattr(shutil, "disk_usage", lambda _: SimpleNamespace(free=0))

    with pytest.raises(CstarError, match="has 0 free"):
        execute_plan(planned.plan(), destination)

    assert not staging_dir(destination).exists()
    assert not os.path.lexists(destination)


def test_replace_link_replaces_a_symlink(planned: Planned, destination: Path) -> None:
    """The link is swapped for the copy once the copy is complete."""
    destination.parent.mkdir(parents=True)
    destination.symlink_to(planned.root, target_is_directory=True)

    execute_plan(planned.plan(), destination, replace_link=True)

    assert destination.is_dir() and not destination.is_symlink()
    assert (destination / RUN_MANIFEST_NAME).is_file()
    assert (planned.root / RUN_MANIFEST_NAME).is_file()


def test_replace_link_keeps_the_link_when_the_copy_fails(
    planned: Planned, destination: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed copy leaves the link in place."""
    destination.parent.mkdir(parents=True)
    destination.symlink_to(planned.root, target_is_directory=True)

    def fail(*args: t.Any, **kwargs: t.Any) -> t.NoReturn:
        raise OSError("disk full")

    monkeypatch.setattr(shutil, "copy2", fail)

    with pytest.raises(OSError, match="disk full"):
        execute_plan(planned.plan(), destination, replace_link=True)

    assert destination.is_symlink()
    assert destination.resolve() == planned.root.resolve()


def test_replace_link_refuses_a_directory(planned: Planned, destination: Path) -> None:
    """Only a symlink can be replaced."""
    keep = write(destination / "keep.txt", "mine")

    with pytest.raises(CstarError, match="not a symlink"):
        execute_plan(planned.plan(), destination, replace_link=True)

    assert keep.read_text() == "mine"


def test_replace_link_refuses_a_missing_destination(
    planned: Planned, destination: Path
) -> None:
    """There is no link to replace."""
    with pytest.raises(CstarError, match="not a symlink"):
        execute_plan(planned.plan(), destination, replace_link=True)


def test_staging_dir_is_a_hidden_sibling(destination: Path) -> None:
    """The copy is renamed within one directory."""
    assert staging_dir(destination) == destination.parent / f".{RUN_ID}.promoting"


# link_run and remove_run


def test_link_resolves_to_the_run(
    build_run: Callable[..., BuiltRun],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The link names the run, wherever it is."""
    monkeypatch.delenv(ENV_CSTAR_PROJECT_HOME, raising=False)
    monkeypatch.delenv("PROJECT", raising=False)
    run = build_run(tmp_path / "run", "run").root
    link = tmp_path / "areas" / "run"

    link_run(link, run)

    assert link.is_symlink()
    assert link.resolve() == run.resolve()


def test_link_is_relative_inside_the_project_home(
    build_run: Callable[..., BuiltRun],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When both lie in the project home, the link moves with it."""
    project = (tmp_path / "project").resolve()
    monkeypatch.setenv(ENV_CSTAR_PROJECT_HOME, str(project))
    run = build_run(project / "scratch" / "run", "run").root
    link = project / "cstar" / "runs" / "run"

    link_run(link, run)

    assert not os.path.isabs(os.readlink(link))
    assert link.resolve() == run.resolve()


def test_link_is_absolute_when_the_run_is_outside_the_project_home(
    build_run: Callable[..., BuiltRun],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run elsewhere is named where it is."""
    project = (tmp_path / "project").resolve()
    monkeypatch.setenv(ENV_CSTAR_PROJECT_HOME, str(project))
    run = build_run(tmp_path / "scratch" / "run", "run").root
    link = project / "cstar" / "runs" / "run"

    link_run(link, run)

    assert os.path.isabs(os.readlink(link))
    assert link.resolve() == run.resolve()


def test_link_refuses_an_existing_destination(
    build_run: Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """Nothing is overwritten."""
    run = build_run(tmp_path / "run", "run").root
    taken = write(tmp_path / "taken" / "file.txt", "mine")

    dangling = tmp_path / "dangling"
    dangling.symlink_to(tmp_path / "nowhere")

    for destination in (taken.parent, dangling):
        with pytest.raises(CstarError, match="already exists"):
            link_run(destination, run)

    assert taken.read_text() == "mine"


def test_remove_run_removes_the_directory(
    build_run: Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """The run's directory and everything in it is gone."""
    run = build_run(tmp_path / "run", "run").root

    remove_run(run)

    assert not run.exists()


def test_remove_run_refuses_a_symlink(
    build_run: Callable[..., BuiltRun], tmp_path: Path
) -> None:
    """A linked run is unlinked by the caller; the run it names is not touched."""
    run = build_run(tmp_path / "run", "run").root
    link = tmp_path / "link"
    link.symlink_to(run, target_is_directory=True)

    with pytest.raises(CstarError, match="not a run directory"):
        remove_run(link)

    assert (run / RUN_MANIFEST_NAME).is_file()


def test_remove_run_refuses_a_file(tmp_path: Path) -> None:
    """Only a directory is a run."""
    with pytest.raises(CstarError, match="not a run directory"):
        remove_run(write(tmp_path / "file"))
