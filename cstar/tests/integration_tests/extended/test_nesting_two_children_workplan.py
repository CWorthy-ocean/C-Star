"""Tier 3: a parent with two child grids, built without ParallelIO, run end to end.

The parent of ``docs/tutorials/nesting_upscaling_tiny`` gets two children: the
example's child, named ``faroe-shetland`` here (the Faroe-Shetland Channel), and a
second one, ``wyville-thomson`` (the Wyville Thomson Ridge). The parent and the
``faroe-shetland`` child are built without ParallelIO, so the parent writes per-rank
``_ext`` files that ``extract_data_join`` splits into one ``<name>_bry`` file per
child. The ``faroe-shetland`` child then runs from the parent's restart (``nest_ic``)
with ``nest-from`` picking its files by ``child_name``.
"""

import time
import typing as t
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import f90nml
import pytest
import yaml

from cstar.roms.file_system import GENERATED_NAMELIST_NAME
from cstar.tests.integration_tests.cases import ROMS_REF
from cstar.tests.integration_tests.cli_harness import (
    DONE,
    kill_run,
    make_cli_env,
    run_cstar,
    run_log_tails,
    step_root,
    wait_for_terminal,
)
from cstar.tests.integration_tests.extended.test_nesting_upscaling_workplan import (
    EXAMPLE_DIR,
    WORKPLAN,
    with_registry_sources,
    without_v_sponge_trc_before_0_13,
)

RUN_ID = "two-children-it"
CHILD = "faroe-shetland"
OTHER_CHILD = "wyville-thomson"
STEPS = ("forge_parent", "forge_child", "parent", "nest_ic", "child")
ALL_DONE = dict.fromkeys(STEPS, DONE)
RUN_TIMEOUT = 60 * 60
# Inside the parent (400 km square at 7.5W 60N), clear of the example's child; the same
# N as that child, as ucla-roms needs without ParallelIO.
OTHER_CHILD_GRID = {
    "nx": 24,
    "ny": 24,
    "size_x": 100.0,
    "size_y": 100.0,
    "center_lon": -8.5,
    "center_lat": 59.6,
    "N": 10,
    "rot": 0.0,
    "theta_s": 5.0,
    "theta_b": 2.0,
    "hc": 300.0,
    "hmin": 5.0,
}

pytestmark = pytest.mark.requires_tools(
    "nccopy", "mpirun", "mpifort or gfortran", "GNU timeout or gtimeout"
)


@dataclass(frozen=True)
class TwoChildRun:
    data_home: Path
    statuses: dict[str, int | None]

    def outputs(self, step: str, pattern: str) -> list[Path]:
        return sorted(
            (step_root(self.data_home, RUN_ID, step) / "output").glob(pattern)
        )


def _forge_blueprint(name: str, data: dict[str, Path]) -> dict[str, t.Any]:
    blueprint = yaml.safe_load((EXAMPLE_DIR / name).read_text())
    blueprint["model_settings"]["cppdefs"]["use_pio"] = False
    blueprint["code"]["pio"] = None
    return without_v_sponge_trc_before_0_13(
        with_registry_sources(blueprint, data), ROMS_REF
    )


def write_workplan(root: Path, data: dict[str, Path]) -> Path:
    parent = _forge_blueprint("forge_parent.yaml", data)
    (example_child,) = parent["domain"]["children"].values()
    parent["domain"]["children"] = {
        CHILD: example_child,
        OTHER_CHILD: {"grid_kwargs": dict(OTHER_CHILD_GRID)},
    }
    blueprints = {
        "forge_parent": parent,
        "forge_child": _forge_blueprint("forge_child.yaml", data),
    }
    for name, blueprint in blueprints.items():
        (root / f"{name}.yaml").write_text(yaml.safe_dump(blueprint, sort_keys=False))

    workplan = yaml.safe_load(WORKPLAN.read_text())
    workplan["name"] = RUN_ID
    workplan["steps"] = [s for s in workplan["steps"] if s["name"] in STEPS]
    for step in workplan["steps"]:
        if step["name"] in blueprints:
            step["blueprint"] = str(root / f"{step['name']}.yaml")
            step["blueprint_overrides"] = {"code": {"roms": {"commit": ROMS_REF}}}
        elif step["name"] == "nest_ic":
            step["blueprint_overrides"]["pio"] = False
        elif step["name"] == "child":
            step["directives"]["nest-from"]["child_name"] = CHILD
    path = root / "workplan.yaml"
    path.write_text(yaml.safe_dump(workplan, sort_keys=False))
    return path


@pytest.fixture(scope="module")
def two_child_run(
    integration_test_data: dict[str, Path],
    cstar_shim: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[TwoChildRun]:
    root = tmp_path_factory.mktemp("two_children")
    state_home = root / "state"
    try:
        workplan = write_workplan(root, integration_test_data)
        env = make_cli_env(root, cstar_shim)
        started = time.monotonic()
        proc = run_cstar(env, "workplan", "submit", str(workplan), "--run-id", RUN_ID)
        assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"
        statuses = wait_for_terminal(
            state_home, RUN_ID, STEPS, timeout=RUN_TIMEOUT, poll_interval=5.0
        )
    except BaseException:
        kill_run(state_home, RUN_ID, STEPS)
        raise
    print(f"\ntwo-child workplan: {(time.monotonic() - started) / 60:.1f} min")
    yield TwoChildRun(data_home=root / "data", statuses=statuses)
    kill_run(state_home, RUN_ID, STEPS)


def test_all_steps_done(two_child_run: TwoChildRun) -> None:
    assert two_child_run.statuses == ALL_DONE, run_log_tails(
        two_child_run.data_home, RUN_ID
    )


def test_parent_wrote_each_childs_boundaries(two_child_run: TwoChildRun) -> None:
    assert two_child_run.outputs("parent", f"{CHILD}_bry.*.nc")
    assert two_child_run.outputs("parent", f"{OTHER_CHILD}_bry.*.nc")
    assert not two_child_run.outputs("parent", "*_ext.*.nc")


def test_child_reads_only_its_own_boundaries(two_child_run: TwoChildRun) -> None:
    root = step_root(two_child_run.data_home, RUN_ID, "child")
    (namelist,) = root.rglob(GENERATED_NAMELIST_NAME)
    frcfiles = f90nml.read(namelist)["forcing_files"]["frcfiles"]
    names = [
        Path(f).name for f in ([frcfiles] if isinstance(frcfiles, str) else frcfiles)
    ]
    assert any(n.startswith(f"{CHILD}_bry.") for n in names), names
    assert not any(n.startswith(f"{OTHER_CHILD}_bry.") for n in names), names
    assert two_child_run.outputs("child", "output_rst.*.nc")
