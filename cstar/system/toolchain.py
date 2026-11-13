"""Toolchain profiles: the compiler, MPI and module environment a system offers.

A toolchain is the compiler/MPI/module profile a system offers; applications
declare which one they need (``ApplicationDefinition.toolchain``). This module
holds only the types, so the application and orchestration layers can name a
toolchain without importing the system manager.
"""

import typing as t
from collections.abc import Mapping
from dataclasses import dataclass, field

ToolchainName = t.Literal["cpu", "gpu"]
"""The names of the toolchain profiles an application can ask a system for."""


@dataclass(frozen=True)
class Toolchain:
    """The compiler, MPI and module profile a system provides for one kind of run."""

    compiler: str
    """The compiler value passed to the ROMS build, e.g. ``gnu``, ``intel`` or ``nvfortran``."""
    mpi_prefix: str
    """The prefix of the MPI launcher used to run the model."""
    profile: str = ""
    """The infix in the system's module and env file names: ``""`` selects
    ``<system>.lmod`` and ``<system>.env``, ``"gpu"`` selects
    ``<system>.gpu.lmod`` and ``<system>.gpu.env``."""
    gpu_arch: str = ""
    """The NVIDIA compute capability flag value, e.g. ``cc80``; empty for CPU."""
    link_flags: str = ""
    """Extra linker flags a GPU build appends after the MPI libraries, e.g. Cray
    MPICH's GPU transport layer. ``$VAR`` references are expanded when the build
    runs, so they may name variables the profile's modules set."""


@dataclass(frozen=True)
class GpuNodes:
    """The GPU nodes a system offers, and how a job reaches them."""

    gpus_per_node: int
    """The number of GPUs on each GPU node."""
    queue: str
    """The partition or QOS GPU jobs go to by default."""
    directives: Mapping[str, str] = field(default_factory=dict)
    """Extra ``#SBATCH key value`` pairs for GPU jobs, e.g. ``{"-C": "gpu"}``.
    These replace the scheduler's CPU ``other_scheduler_directives`` for GPU jobs."""
    cores_per_node: int | None = None
    """The CPU cores on each GPU node, for PBS ``ncpus=``; ``None`` when not needed."""
    account_required: bool = False
    """Whether the scheduler refuses a GPU job that names no account, so C-Star
    must insist on one."""
