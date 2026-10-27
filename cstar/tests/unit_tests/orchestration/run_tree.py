"""A fake run directory with one of everything `cstar run check` reports."""

import textwrap
import typing as t
from dataclasses import dataclass
from pathlib import Path

from cstar.base.utils import slugify
from cstar.execution.file_system import DirectoryManager, StateDirectoryManager
from cstar.orchestration.models import Step
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.serialization import serialize
from cstar.orchestration.tracking import WorkplanRun

RUN_ID: t.Final[str] = "my-run"
STEP_NAMES: t.Final[tuple[str, str]] = ("Spin Up", "run")
"""The step that has a directory in the tree, and the one that does not."""


@dataclass(frozen=True)
class FakeRun:
    """A run directory under the data home, its transformed workplan and record."""

    root: Path
    """The run directory (`<data home>/workplan_runs/my-run`), already resolved."""
    workplan: LiveWorkplan
    trx_path: Path
    """The transformed workplan, `<root>/my-workplan_transformed.yaml`."""
    elsewhere: Path
    """A file outside both the run and the data home."""

    @property
    def record(self) -> WorkplanRun:
        """The tracking record of the run."""
        return WorkplanRun(
            output_path=self.root,
            run_id=RUN_ID,
        )

    @property
    def spin_up(self) -> Path:
        """The directory of the step that exists."""
        return self.root / "tasks" / "spin-up"

    def rel(self, path: Path) -> str:
        """`path` below the run directory, as text."""
        return path.relative_to(self.root).as_posix()


def write(path: Path, text: str) -> Path:
    """Write dedented `text` to `path`, creating its parents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path


def link(path: Path, target: str | Path) -> Path:
    """Create the symlink `path` with exactly the link text `target`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(target)
    return path


def build_fake_run(blueprint: Path, scratch: Path) -> FakeRun:
    """Build a run tree that exercises every check.

    Under the run, with `<root>` the run directory, `<home>` the data home and
    `<step>` the `Spin Up` step's directory:

    - `<root>/my-workplan_transformed.yaml` ends with a comment naming `<root>`
    - `<step>/output/result.yaml` names `<root>` (line 2), another run under
      `<home>` (line 3), `<home>-old` (line 4: not under `<home>`), and paths
      outside both (lines 5 and 6)
    - `<step>/blueprints/B_spin-up.yaml` and `<step>/work/cstar_generated_roms.nml`
      name `<root>` on line 2, and so does `<step>/tasks/sub/output/n.yaml`
    - the launch artifacts `work/directives.yaml`, `work/spin-up.ovrd.yaml`,
      `script.sh` and `input/runtime_code/namelist.nml` name `<root>` on line 2
    - `<root>/notes.yaml` names `<root>` but is not a reference-bearing file
    - `<step>/input` holds two absolute links inside the run (`abs_file` and
      `abs_dir`), a dangling absolute link, a dangling relative link, a relative
      link that works, and an absolute link to a file outside the run
    - `<step>/input/codebases/roms/.libs/lib.la` is a dangling relative link,
      as a cloned ucla-roms holds
    - `<root>/gathered_output/a.nc` is an absolute link into the run
    - the `run` step has no directory and `tasks/stale` belongs to no step
    - the run's state directory holds the sentinel of `Spin Up`, one that
      belongs to no step, and a file that is no sentinel
    """
    root = StateDirectoryManager.data_dir(RUN_ID).resolve()
    home = DirectoryManager.data_home()
    elsewhere = write(scratch / "elsewhere" / "data.nc", "data")
    step = root / "tasks" / "spin-up"
    named = f"name: x\nwhere: {root}/tasks/spin-up/input/grid.nc\n"

    steps = [
        LiveStep.from_step(
            Step(name=name, application="hello_world", blueprint=blueprint.as_posix()),
            update={"working_dir": root / "tasks" / slugify(name)},
        )
        for name in STEP_NAMES
    ]
    workplan = LiveWorkplan(name="fake", description="a fake run", steps=steps)
    trx_path = root / "my-workplan_transformed.yaml"
    trx_path.parent.mkdir(parents=True, exist_ok=True)
    assert serialize(trx_path, workplan)
    with trx_path.open("a") as stream:
        stream.write(f"\n# transformed in {root}/tasks\n")

    write(root / "notes.yaml", named)
    write(step / "output" / "a.nc", "data")
    write(
        step / "output" / "result.yaml",
        f"""\
        name: result
        source: {root}/tasks/spin-up/output/a.nc
        other: {home}/workplan_runs/other-run/tasks/x/output/b.nc
        near: {home}-old/x
        elsewhere: /usr/local/lib/x.so
        relative: ../input/b.nc
        """,
    )
    write(step / "blueprints" / "B_spin-up.yaml", named)
    write(step / "work" / "cstar_generated_roms.nml", named)
    write(step / "tasks" / "sub" / "output" / "n.yaml", named)
    for launch in (
        "work/directives.yaml",
        "work/spin-up.ovrd.yaml",
        "script.sh",
        "input/runtime_code/namelist.nml",
    ):
        write(step / launch, named)

    link(step / "input" / "abs_file", step / "output" / "a.nc")
    link(step / "input" / "abs_dir", step / "output")
    link(step / "input" / "abs_dangling", root / "tasks" / "gone")
    link(step / "input" / "rel_dangling", "../nothing")
    link(step / "input" / "rel_ok", "../output/a.nc")
    link(step / "input" / "abs_outside", elsewhere)
    link(step / "input" / "codebases" / "roms" / ".libs" / "lib.la", "../lib.la")
    link(root / "gathered_output" / "a.nc", step / "output" / "a.nc")
    (root / "tasks" / "stale").mkdir()

    state = StateDirectoryManager.run_state_dir(run_id=RUN_ID)
    write(state / "spin-up.sentinel.yaml", "name: spin-up")
    write(state / "retired.sentinel.yaml", "name: retired")
    write(state / "lock.txt", "")

    return FakeRun(root, workplan, trx_path, elsewhere)
