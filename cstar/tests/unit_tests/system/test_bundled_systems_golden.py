"""The bundled systems, pinned: what each one gives C-Star, observed from outside.

The fixture was captured from the Python system contexts before they became the
YAML files under ``cstar/catalog/bundled/systems/``, so it is the evidence that
the conversion changed no system's behaviour. Each system is observed only
through what consumers use: the registry, `CStarSystemManager` (which loads the
modules and env of a toolchain), the scheduler, the toolchains, the settings a
job requires, detection and the scratch convention. Shelling out (Lmod, sinfo,
sacctmgr) is replaced by a recorder and a fixed environment.

An intended change to a bundled system regenerates the fixture: rerun with
``UPDATE_GOLDEN=1`` set, review the diff, and commit it.
"""

import dataclasses
import json
import os
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_SYSTEM
from cstar.base.exceptions import CstarError
from cstar.system.environment import CStarEnvironment, SlurmSettingsBase
from cstar.system.manager import (
    CStarSystemManager,
    registered_context,
    registered_names,
)
from cstar.system.scheduler import PBSQueue, Scheduler

GOLDEN_PATH = Path(__file__).parent / "fixtures" / "bundled_systems_golden.json"

BUNDLED_SYSTEMS = (
    "anvil",
    "bouchet",
    "darwin_arm64",
    "derecho",
    "elja",
    "expanse",
    "linux_aarch64",
    "linux_x86_64",
    "oscar",
    "perlmutter",
)

MODULE_ENVIRONMENT = {
    # what the modules (or conda) would set; each value names its variable
    **{
        name: f"/mod/{name.lower()}"
        for name in (
            "CONDA_PREFIX",
            "CRAY_MPICH_PREFIX",
            "CRAY_NETCDF_HDF5PARALLEL_PREFIX",
            "CRAY_NETCDF_PREFIX",
            "CRAY_PARALLEL_NETCDF_PREFIX",
            "EBROOTGOMPI",
            "EBROOTNETCDF",
            "EBROOTNETCDFMINFORTRAN",
            "EBROOTOPENMPI",
            "MPI_DIR",
            "MVAPICH2HOME",
            "NETCDF",
            "NETCDF_FORTRANHOME",
            "NETCDF_FORTRAN_HOME",
            "OPAL_PREFIX",
            "OPENMPI_HOME",
        )
    },
    # PNETCDF is deliberately unset: a reference to it must expand to ""
    "PATH": "/usr/bin:/bin",
    "LIBRARY_PATH": "/usr/lib",
    "LD_LIBRARY_PATH": "/opt/ld",
    # decoys: a file's own earlier entry must win over the process environment
    "MPIHOME": "/decoy/mpihome",
    "NETCDFHOME": "/decoy/netcdfhome",
    "LMOD_CMD": "/fake/lmod",
}
"""The fixed environment each system's modules and env are loaded in."""

DETECTION_ENVIRONMENTS = {
    "none": {},
    "anvil": {"RCAC_CLUSTER": "anvil"},
    "bouchet-login": {"CLUSTER": "bouchet"},
    "bouchet-job": {"SLURM_CLUSTER_NAME": "bouchet"},
    "elja-login": {"HOSTNAME": "elja-irhpc"},
    "elja-job": {"SLURM_CLUSTER_NAME": "elja"},
    "other-cluster": {"SLURM_CLUSTER_NAME": "slurmctld", "CLUSTER": "x"},
}
"""Environments each system's `is_match` is asked about."""


def _scheduler(scheduler: Scheduler | None) -> dict[str, Any] | None:
    """Describe a scheduler without querying the machine."""
    if scheduler is None:
        return None
    queues = []
    for q in scheduler.queues:
        if isinstance(q, PBSQueue):
            walltime = f"fixed {q.max_walltime}"
        else:
            method = q._max_walltime_method or q._default_max_walltime_method
            walltime = method.__name__
        queues.append(
            {
                "type": type(q).__name__,
                "name": q.name,
                "query_name": q.query_name,
                "walltime": walltime,
            }
        )
    return {
        "type": type(scheduler).__name__,
        "primary_queue": scheduler.primary_queue_name,
        "queues": queues,
        "directives": dict(scheduler.other_scheduler_directives),
        "requires_task_distribution": scheduler.requires_task_distribution,
        "max_cpus_per_node": scheduler._max_cpus_per_node,
        "documentation": scheduler.documentation,
        "gpu": None if scheduler.gpu is None else dataclasses.asdict(scheduler.gpu),
    }


def _toolchains(name: str) -> dict[str, Any]:
    """Describe the system's toolchains, or why it has none under a name."""
    found: dict[str, Any] = {}
    for profile in ("cpu", "gpu"):
        try:
            tc = registered_context(name).toolchain(profile)
        except CstarError as ex:
            found[profile] = f"error: {ex}"
            continue
        found[profile] = {
            "compiler": tc.compiler,
            "mpi_prefix": tc.mpi_prefix,
            "profile": tc.profile,
            "gpu_arch": tc.gpu_arch,
            "link_flags": tc.link_flags,
        }
    return found


def _environments(name: str, profiles: list[str]) -> dict[str, Any]:
    """Load each toolchain's modules and env, recording the Lmod calls and the
    variables set, in the fixed module environment.
    """
    calls: list[str] = []
    found: dict[str, Any] = {}
    with (
        mock.patch.dict(
            os.environ, {**MODULE_ENVIRONMENT, ENV_CSTAR_SYSTEM: name}, clear=True
        ),
        mock.patch("cstar.system.environment.platform.system", return_value="Linux"),
        mock.patch.object(
            CStarEnvironment,
            "_call_lmod",
            lambda _self, *args: calls.append(" ".join(args)),
        ),
    ):
        mgr = CStarSystemManager()  # loads the cpu toolchain
        for profile in profiles:
            if profile != "cpu":
                calls.clear()
            env = mgr.environment_for(profile)  # type: ignore[arg-type]
            found[profile] = {
                "lmod_calls": list(calls),
                "env": env.environment_variables,
            }
    return found


def _required_settings(name: str) -> list[str]:
    """The settings a job on the system must name."""
    klass = registered_context(name).settings_klass()
    if klass is None or not issubclass(klass, SlurmSettingsBase):
        return []
    return sorted(klass.required_settings())


def _detection(name: str) -> dict[str, bool]:
    """Whether the system recognises the machine, per environment."""
    found = {}
    for label, environ in DETECTION_ENVIRONMENTS.items():
        with mock.patch.dict(os.environ, environ, clear=True):
            found[label] = registered_context(name).is_match()
    return found


def _scratch(name: str, home: Path) -> str | None:
    """The scratch root the system finds in a home holding several candidates."""
    with mock.patch.dict(os.environ, {"HOME": str(home), "USER": "testuser"}):
        root = registered_context(name).scratch_root()
    return None if root is None else root.relative_to(home).as_posix()


def observe(name: str, home: Path) -> dict[str, Any]:
    """Everything the golden pins about one system."""
    ctx = registered_context(name)
    toolchains = _toolchains(name)
    return {
        "name": ctx.name,
        "compiler": ctx.compiler,
        "mpi_prefix": ctx.mpi_prefix,
        "scheduler": _scheduler(ctx.create_scheduler()),
        "toolchains": toolchains,
        "required_settings": _required_settings(name),
        "environments": _environments(
            name, [p for p, tc in toolchains.items() if isinstance(tc, dict)]
        ),
        "detection": _detection(name),
        "scratch": _scratch(name, home),
    }


@pytest.fixture
def scratch_home(tmp_path: Path) -> Path:
    """A home holding Bouchet's and Oscar's scratch candidates, and a decoy file."""
    home = tmp_path / "home"
    for directory in ("scratch_pi_beta", "scratch_pi_alpha", "scratch"):
        (home / directory).mkdir(parents=True)
    (home / "scratch_pi_000").touch()  # a file, not a directory: never chosen
    return home


def test_bundled_system_names() -> None:
    """Verify exactly the bundled systems are registered."""
    assert registered_names() == sorted(BUNDLED_SYSTEMS)


def test_bundled_systems_match_golden(scratch_home: Path) -> None:
    """Verify each bundled system gives C-Star what the golden fixture records."""
    observed = {name: observe(name, scratch_home) for name in BUNDLED_SYSTEMS}

    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        pytest.skip(
            f"UPDATE_GOLDEN=1: wrote {GOLDEN_PATH}. Review the diff and commit it, "
            "then rerun without UPDATE_GOLDEN to confirm the test passes."
        )

    golden = json.loads(GOLDEN_PATH.read_text())
    for name in BUNDLED_SYSTEMS:
        assert observed[name] == golden[name], f"{name} differs from the golden"
