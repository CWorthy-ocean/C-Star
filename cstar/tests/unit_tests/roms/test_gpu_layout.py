"""Tests for `cstar.roms.gpu_layout`: the GPU job's rank layout and writer sizing.

The expected writer counts in `test_suggested_writers_per_node_matches_the_script`
were produced by running roms-marbl-gpu's ``tools/gpu_layout.py --per-node`` (v0.1.2-beta; unchanged in v0.1.3-beta)
on the namelist each case writes with C-Star's own writer, with ``--gpus-per-node``
set to tiles / nodes; the column ``NWPN`` of its output is the number hard-coded here.
"""

from pathlib import Path

import pytest

from cstar.roms.gpu_layout import (
    GpuRankLayout,
    suggested_writers_per_node,
    writers_per_file,
)
from cstar.roms.namelist import RomsNamelistV0_11_0

V0_11_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_11_0.nml"


@pytest.mark.parametrize(
    (
        "tiles",
        "nodes",
        "writers_per_node",
        "per_node",
        "writers",
        "tasks",
        "tasks_per_node",
    ),
    [
        (4, 1, 1, 4, 1, 5, 5),  # 1 node, 2 x 2 tiles + 1 writer
        (8, 2, 1, 4, 2, 10, 5),  # 2 nodes of 4 GPUs, 1 writer each
        (8, 2, 0, 4, 0, 8, 4),  # no writers
        (6, 3, 3, 2, 9, 15, 5),
        (1, 1, 0, 1, 0, 1, 1),
    ],
)
def test_rank_layout_arithmetic(
    tiles, nodes, writers_per_node, per_node, writers, tasks, tasks_per_node
):
    layout = GpuRankLayout(tiles, nodes, writers_per_node)
    assert layout.tiles_per_node == per_node
    assert layout.writers == writers
    assert layout.tasks == tasks
    assert layout.tasks_per_node == tasks_per_node


def test_rank_layout_rejects_tiles_that_do_not_divide_over_the_nodes():
    with pytest.raises(ValueError) as raised:
        GpuRankLayout(tiles=6, nodes=4, writers_per_node=1)
    assert str(raised.value) == (
        "6 GPU tiles (np_xi x np_eta) cannot be spread evenly over 4 nodes; "
        "choose a tile count that is a multiple of the node count"
    )


@pytest.mark.parametrize(
    ("tiles", "nodes", "writers_per_node", "message"),
    [
        (0, 1, 0, "0 GPU tiles"),
        (4, 0, 0, "0 nodes"),
        (4, 1, -1, "-1 writers per node"),
        (2, 4, 0, "cannot be spread evenly"),  # no compute rank on two nodes
    ],
)
def test_rank_layout_rejects_an_invalid_layout(tiles, nodes, writers_per_node, message):
    with pytest.raises(ValueError, match=message):
        GpuRankLayout(tiles, nodes, writers_per_node)


def test_rank_layout_reports_every_problem_at_once():
    with pytest.raises(ValueError) as raised:
        GpuRankLayout(tiles=0, nodes=0, writers_per_node=-2)
    assert len(str(raised.value).splitlines()) == 3


def test_rank_layout_is_frozen():
    layout = GpuRankLayout(4, 1, 1)
    with pytest.raises(AttributeError):
        layout.tiles = 8  # type: ignore[misc]


def _namelist(**settings) -> RomsNamelistV0_11_0:
    """The 0.11.0 fixture (16 x 16 tiles, 512 x 512 x 60, 32 BGC tracers, history
    off, restarts on) with ``<group>__<key>`` overrides applied.
    """
    nml = RomsNamelistV0_11_0.read(V0_11_0_NAMELIST)
    for name, value in settings.items():
        group, key = name.split("__")
        setattr(getattr(nml, group), key, value)
    return nml


HIS = {"basic_output_settings__wrt_file_his": True}
NO_RST = {
    "basic_output_settings__wrt_file_rst": False,
    "basic_output_settings__nrpf_his": 1000,
}
SMALL = {  # a 2 x 2 domain without MARBL: a record of a few MB
    "param_settings__np_xi": 2,
    "param_settings__np_eta": 2,
    "param_settings__llm": 100,
    "param_settings__mmm": 100,
    "param_settings__nz": 20,
    "param_settings__nt_bgc": 0,
}


def _grid(np_xi: int, np_eta: int, size: int) -> dict:
    """``np_xi`` x ``np_eta`` tiles of ``size`` x ``size`` x 60 cells, MARBL on."""
    return {
        "param_settings__np_xi": np_xi,
        "param_settings__np_eta": np_eta,
        "param_settings__llm": size,
        "param_settings__mmm": size,
        "param_settings__nz": 60,
        "param_settings__nt_bgc": 32,
    }


@pytest.mark.parametrize(
    ("settings", "nodes", "expected"),
    [
        # history off: one writer for the restart
        ({}, 1, 1),
        # history + restart: two files at once, one writer each (record of MB)
        ({**HIS, **SMALL}, 1, 2),
        # history only: one file
        ({**HIS, **NO_RST, **SMALL}, 1, 1),
        # history split over files (5 records per file < the run's 31), restart off
        ({**HIS, **SMALL, **NO_RST, "basic_output_settings__nrpf_his": 5}, 1, 2),
        # 2.4 GB record: still one writer per file; 2 writers over 8 nodes -> 1 each
        (HIS, 8, 1),
        # 9.2 GB record: two writers per file, two files: 4 writers on one node
        ({**HIS, **_grid(2, 2, 1000)}, 1, 4),
        # 9.2 GB record, history only: 2 writers over 2 nodes -> 1 each
        ({**HIS, **NO_RST, **_grid(4, 2, 1000)}, 2, 1),
        # 20.7 GB record, history only: 3 writers over 2 nodes round up to 2 each
        ({**HIS, **NO_RST, **_grid(4, 2, 1500)}, 2, 2),
        # 36.9 GB+ record: capped at 4 per file; 2 files -> 8 writers over 3 nodes -> 3
        ({**HIS, **_grid(3, 2, 2000)}, 3, 3),
    ],
)
def test_suggested_writers_per_node_matches_the_script(settings, nodes, expected):
    assert suggested_writers_per_node(_namelist(**settings), nodes) == expected


def test_a_history_period_longer_than_the_run_is_one_record():
    """A period beyond the run is one record, which a 5-record file does not split."""
    settings = {
        **HIS,
        **SMALL,
        **NO_RST,
        "basic_output_settings__nrpf_his": 5,
        "basic_output_settings__output_period_his": 1.0e9,
    }
    assert suggested_writers_per_node(_namelist(**settings), 1) == 1


def test_a_zero_history_period_counts_as_one_record():
    """The script falls back to one record when the period is not positive."""
    settings = {
        **HIS,
        **SMALL,
        **NO_RST,
        "basic_output_settings__nrpf_his": 5,
        "basic_output_settings__output_period_his": 0.0,
    }
    assert suggested_writers_per_node(_namelist(**settings), 1) == 1


@pytest.mark.parametrize(
    ("writers", "per_file"),
    [(0, 1), (1, 1), (2, 1), (3, 1), (4, 2), (5, 1), (6, 3), (8, 4), (10, 2), (16, 4)],
)
def test_writers_per_file_follows_the_model_s_writer_groups(
    writers: int, per_file: int
) -> None:
    """``min(4, max(1, writers // 2))``, lowered until it divides the writers
    (roms-marbl-gpu ``io_server.F90`` ``writer_groups``).
    """
    assert writers_per_file(writers) == per_file
