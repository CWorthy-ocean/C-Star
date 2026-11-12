"""Rank layout of a GPU ROMS job: how many MPI ranks roms-marbl-gpu needs, and how
many of them write output.

The GPU model runs one compute rank per tile (``np_xi * np_eta`` tiles, one GPU
each). Output is written by separate writer (I/O server) ranks: with
``ROMS_GPU_NWRITERS=w`` in the job's environment, the last ``w`` ranks of every
node are writers and the rest compute. Every node carries the same number of
ranks (what a SLURM ``--ntasks-per-node`` can express), which makes the tiles
divide evenly over the nodes. The model itself checks only that the compute ranks
number ``np_xi * np_eta`` and that every node keeps a compute rank, and stops at
start-up printing the layout it expected otherwise (roms-marbl-gpu
``src/shared/comm.F90``, v0.1.3-beta).

:class:`GpuRankLayout` holds that arithmetic. :func:`suggested_writers_per_node`
sizes the writers from the history record, as roms-marbl-gpu'
``tools/gpu_layout.py --per-node`` (v0.1.3-beta) does.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable

    from cstar.roms.namelist import RomsNamelistBase

GPU_WRITER_RATE_GBPS: Final = 0.6
"""GB/s one writer streams (measured on Derecho Lustre). Mirrors ``--rate-gbps`` in
roms-marbl-gpu ``tools/gpu_layout.py`` at v0.1.3-beta."""

GPU_WRITER_TARGET_SECONDS: Final = 15.0
"""Seconds one history record should take to write. Mirrors ``--target-seconds`` in
roms-marbl-gpu ``tools/gpu_layout.py`` at v0.1.3-beta."""

GPU_MAX_WRITERS_PER_GROUP: Final = 4
"""Most writers on one file: one file's bandwidth is limited, and more writers on
it were slower. Mirrors the ``min(4, ...)`` in roms-marbl-gpu ``tools/gpu_layout.py``
at v0.1.3-beta."""

GPU_MAX_WRITERS: Final = 16
"""Most writer ranks in total. Mirrors ``--max-writers`` in roms-marbl-gpu
``tools/gpu_layout.py`` at v0.1.3-beta."""

_RECORD_2D_KEYS: Final = ("wrt_z", "wrt_ub", "wrt_vb", "wrt_hbls", "wrt_hbbl")
"""The 2-D history switches ``tools/gpu_layout.py`` counts in a record. A key the
namelist does not have counts as off."""

_RECORD_3D_KEYS: Final = (
    "wrt_u",
    "wrt_v",
    "wrt_temp",
    "wrt_salt",
    "wrt_akv",
    "wrt_akt",
    "wrt_r",
)
"""The 3-D history switches ``tools/gpu_layout.py`` counts in a record
(``wrt_temp`` and ``wrt_salt`` are in ``&ts_output_settings``). A key the
namelist does not have counts as off."""

_MARBL_EXTRA_2D: Final = 8
"""2-D MARBL surface diagnostics (PH, pCO2, FG_CO2, zsat, ...) per history record
when ``nt_bgc > 0``, as ``tools/gpu_layout.py`` counts them."""

_MARBL_EXTRA_3D: Final = 4
"""3-D MARBL carbonate diagnostics per history record when ``nt_bgc > 0``, as
``tools/gpu_layout.py`` counts them."""


@dataclass(frozen=True)
class GpuRankLayout:
    """The MPI ranks of a GPU job: compute ranks, writer ranks, and their nodes.

    The one place the rank arithmetic lives (see the module docstring and
    roms-marbl-gpu ``src/shared/comm.F90``, v0.1.3-beta). Every node carries
    ``tiles // nodes`` compute ranks and ``writers_per_node`` writer ranks, as a
    uniform ranks-per-node launch gives, so the tiles must divide evenly over the
    nodes. The model itself checks only the compute-rank count and that every node
    keeps a compute rank (which ``tiles >= nodes`` gives here), not that the node
    sizes are equal.

    Parameters
    ----------
    tiles
        Compute ranks, ``np_xi * np_eta``: one per GPU.
    nodes
        Nodes in the job.
    writers_per_node
        Writer ranks on every node (``ROMS_GPU_NWRITERS``); ``0`` for none.

    Raises
    ------
    ValueError
        If `tiles` or `nodes` is below 1, `writers_per_node` is negative, or
        `tiles` does not divide evenly over `nodes`. One error lists every
        violation.
    """

    tiles: int
    nodes: int
    writers_per_node: int

    def __post_init__(self) -> None:
        """Reject a layout the GPU model would stop on."""
        problems: list[str] = []
        if self.tiles < 1:
            problems.append(f"{self.tiles} GPU tiles (np_xi x np_eta); need at least 1")
        if self.nodes < 1:
            problems.append(f"{self.nodes} nodes; need at least 1")
        if self.writers_per_node < 0:
            problems.append(f"{self.writers_per_node} writers per node; need 0 or more")
        if self.tiles >= 1 and self.nodes >= 1 and self.tiles % self.nodes:
            problems.append(
                f"{self.tiles} GPU tiles (np_xi x np_eta) cannot be spread evenly "
                f"over {self.nodes} nodes; choose a tile count that is a multiple "
                "of the node count"
            )
        if problems:
            raise ValueError("\n".join(problems))

    @property
    def tiles_per_node(self) -> int:
        """Compute ranks on each node."""
        return self.tiles // self.nodes

    @property
    def writers(self) -> int:
        """Writer ranks in the whole job."""
        return self.writers_per_node * self.nodes

    @property
    def tasks(self) -> int:
        """MPI ranks in the whole job: compute plus writer."""
        return self.tiles + self.writers

    @property
    def tasks_per_node(self) -> int:
        """MPI ranks on each node: compute plus writer."""
        return self.tiles_per_node + self.writers_per_node


def _namelist_lookup(settings: RomsNamelistBase) -> Callable[[str, Any], Any]:
    """Look keys up in a namelist as ``tools/gpu_layout.py`` does.

    The script scans the namelist text, so a key is found in the first group of
    ``settings.model_dump()`` that holds it; the lookup returns `default` when no
    group does.
    """
    groups = settings.model_dump().values()

    def value(key: str, default: Any) -> Any:
        return next(
            (g[key] for g in groups if g.get(key) is not None),
            default,
        )

    return value


def history_record_gb(settings: RomsNamelistBase) -> float:
    """Size, in GB, of one history record of a GPU run of this namelist.

    The estimate of roms-marbl-gpu ``tools/gpu_layout.py`` (v0.1.3-beta): four
    bytes per point of each 2-D and 3-D field the ``wrt_*`` switches write, plus
    the MARBL tracers and diagnostics when ``nt_bgc > 0``. A key C-Star's namelist
    does not have counts as the script counts a key the namelist lacks.

    Parameters
    ----------
    settings
        The namelist the GPU model will read.

    Returns
    -------
    float
    """
    value = _namelist_lookup(settings)
    nt_bgc = int(value("nt_bgc", 0))
    nz = int(value("nz", 0))
    cells = (int(value("llm", 0)) + 2) * (int(value("mmm", 0)) + 2)
    n2d = sum(1 for k in _RECORD_2D_KEYS if value(k, False))
    n3d = sum(1 for k in _RECORD_3D_KEYS if value(k, False))
    if value("write_passive", True) and nt_bgc > 0:
        n3d += nt_bgc
    if nt_bgc > 0:
        n2d += _MARBL_EXTRA_2D
        n3d += _MARBL_EXTRA_3D
    return 4.0 * cells * (n3d * nz + n2d) / 1e9


def writers_per_file(writers: int) -> int:
    """Writer ranks that write one output file together.

    The model splits its writers into groups, one file per group at a time:
    ``min(4, max(1, writers // 2))`` per group, lowered until it divides the
    writers (``writer_groups`` in roms-marbl-gpu ``src/host/io_server.F90``,
    v0.1.3-beta, without the ``ROMS_GPU_IO_GROUP`` override).

    Parameters
    ----------
    writers
        Writer ranks in the whole job; ``0`` is taken as one, as the model does.

    Returns
    -------
    int
    """
    writers = max(writers, 1)
    size = min(GPU_MAX_WRITERS_PER_GROUP, max(1, writers // 2))
    while writers % size:
        size -= 1
    return size


def suggested_writers_per_node(settings: RomsNamelistBase, nodes: int) -> int:
    """Writer ranks per node for a GPU run of this namelist.

    A port of roms-marbl-gpu ``tools/gpu_layout.py --per-node`` (v0.1.3-beta). It
    sizes the history record from the grid and the ``wrt_*`` switches that write
    a field, takes the writers one file needs to write a record in
    :data:`GPU_WRITER_TARGET_SECONDS` at :data:`GPU_WRITER_RATE_GBPS` each (at
    most :data:`GPU_MAX_WRITERS_PER_GROUP`), doubles that when two files are
    written at once (history and restart, or a history split over several
    files), caps the total at :data:`GPU_MAX_WRITERS`, and spreads it over
    `nodes`, rounding up so that every node carries the same number.

    The script scans the namelist text, so this finds each key in the first group
    of ``settings.model_dump()`` that holds it, with the script's default when it
    holds none: ``write_passive``, ``wrt_file_his`` and ``wrt_file_rst`` default
    on, ``nrpf_his`` to 20. A key C-Star's namelist does not have
    (``write_passive``, which the GPU reads from its own ``&passive_tracers``
    group) is read as the script reads a key the namelist lacks: off, or its
    default.

    Parameters
    ----------
    settings
        The namelist the GPU model will read.
    nodes
        Nodes in the job, at least 1.

    Returns
    -------
    int
        Writer ranks to put on each node (``ROMS_GPU_NWRITERS``), at least 1.
    """
    value = _namelist_lookup(settings)
    record_gb = history_record_gb(settings)

    his = value("wrt_file_his", True)
    rst = value("wrt_file_rst", True)
    per_group = 1
    if his:
        per_group = max(
            1,
            min(
                GPU_MAX_WRITERS_PER_GROUP,
                math.ceil(
                    record_gb / (GPU_WRITER_RATE_GBPS * GPU_WRITER_TARGET_SECONDS)
                ),
            ),
        )

    # Files in flight at once: history plus restart, or several history files.
    dt = float(value("dt", 1.0))
    ntimes = int(value("ntimes", 1))
    period = float(value("output_period_his", 0.0) or 0.0)
    nrec_his = (
        int(ntimes * dt / period) + 1 if (his and period > 0) else (1 if his else 0)
    )
    split_his = his and int(value("nrpf_his", 20)) < nrec_his
    n_groups = 2 if (his and (rst or split_his)) else 1

    writers = max(1, min(GPU_MAX_WRITERS, n_groups * per_group))
    return math.ceil(writers / nodes)
