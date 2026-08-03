"""Pre- and post-build checks that keep a ROMS build from mixing toolchains.

On systems that use Linux Environment Modules (Lmod), an active conda
environment can shadow the module stack's `mpifort`/`nf-config`/`nc-config`
on `PATH`, silently producing a binary that is linked against a mix of
conda and module libraries. On conda-based systems, the opposite mistake
(a stray system tool leaking onto `PATH`) causes the same problem.

This module provides:

- `explicit_mpi_wrapper` : resolve an unambiguous path to the MPI Fortran
  wrapper to hand to `make`, defeating `PATH` shadowing, when it is safe
  to do so.
- `assert_single_toolchain_stack` : a pre-build check that every tool that
  will participate in the build (NetCDF, PnetCDF, MPI) resolves to a
  single, consistent toolchain.
- `verify_roms_linkage` : a post-build check that the produced `roms`
  binary is not linked against a mismatched set of libraries, and (where
  relevant) embeds a RUNPATH so it does not depend on `LD_LIBRARY_PATH`
  at runtime.
"""

import os
import shutil
import sys
from pathlib import Path

from cstar.base.log import get_logger
from cstar.base.utils import _run_cmd
from cstar.system.manager import get_sysmgr

log = get_logger(__name__)

_DECLARED_PREFIX_VARS: tuple[str, ...] = (
    "MPIHOME",
    "NETCDFHOME",
    "NETCDFFHOME",
    "PNETCDFHOME",
)
"""System-env-file variables that must not be declared-but-empty."""

_MPI_WRAPPER_CANDIDATES: dict[str, tuple[str, ...]] = {
    "gnu": ("mpifort",),
    "intel": ("mpiifx", "mpifort"),
    "ifort": ("mpiifort", "mpifort"),
}
"""Candidate MPI Fortran wrapper names to search `PATH` for, by compiler."""

_LINKAGE_LIB_PATTERNS: tuple[str, ...] = (
    "libnetcdf",
    "libnetcdff",
    "libpnetcdf",
    "libhdf5",
    "libmpi",
)
"""Library name fragments checked by `verify_roms_linkage` on Linux."""


def _get_dependency_root(env_var: str) -> str | None:
    """Look up a dependency location from the C-Star environment or the process
    environment.

    Parameters
    ----------
    env_var : str
        The name of the environment variable to look up (e.g. `MPIHOME`).

    Returns
    -------
    str | None
        The value of the variable, or `None` if not set anywhere.
    """
    return get_sysmgr().environment.environment_variables.get(
        env_var
    ) or os.environ.get(env_var)


def explicit_mpi_wrapper() -> str | None:
    """Resolve an explicit path to the MPI Fortran wrapper to pass to `make`.

    A full, explicit path defeats `PATH` shadowing (e.g. by an active conda
    environment on an Lmod system), whereas a bare command name like
    `mpifort` would be resolved by the shell/Makefile against whatever
    happens to be first on `PATH` at build time.

    This is only done for the `gnu` compiler: on `intel` systems the correct
    wrapper name varies (`mpiifx`, `mpiifort`, etc.), and the ROMS Makefile's
    own autodetection plus its compiler-compatibility check already handle
    that case correctly.

    Returns
    -------
    str | None
        The absolute path to `mpifort` under `MPIHOME`, if the system
        compiler is `gnu` and the file exists there. Otherwise `None`, in
        which case the caller should fall back to the Makefile's own
        autodetection.
    """
    cstar_sysmgr = get_sysmgr()
    if cstar_sysmgr.environment.compiler != "gnu":
        return None

    mpi_home = _get_dependency_root("MPIHOME")
    if not mpi_home:
        return None

    candidate = Path(mpi_home) / "bin/mpifort"
    if candidate.exists():
        return str(candidate)

    return None


def rpath_link_flags() -> str | None:
    """Build explicit `-Wl,-rpath` link flags from the declared `LD_RUN_PATH`.

    Setting `LD_RUN_PATH` alone is not sufficient to embed a RUNPATH: the
    linker only consults it when no `-rpath` option is passed, and MPI
    wrappers frequently inject their own `-rpath` flags (e.g. spack-built
    OpenMPI embeds its gcc/openmpi/hwloc lib dirs). Explicit `-Wl,-rpath`
    flags merge with the wrapper's instead of being ignored, so every
    directory in `LD_RUN_PATH` ends up in the binary's RUNPATH.

    Returns
    -------
    str | None
        Space-separated `-Wl,-rpath,<dir>` flags for each directory declared
        in the system environment file's `LD_RUN_PATH`, or `None` on
        non-Linux platforms or when `LD_RUN_PATH` is not declared/empty.
    """
    if sys.platform != "linux":
        return None

    ld_run_path = get_sysmgr().environment.environment_variables.get("LD_RUN_PATH")
    if not ld_run_path:
        return None

    dirs = [d for d in ld_run_path.split(":") if d.strip()]
    if not dirs:
        return None

    return " ".join(f"-Wl,-rpath,{d}" for d in dirs)


def assert_single_toolchain_stack(mpi_wrapper: str | None) -> None:
    """Assert that the tools that will participate in a ROMS build resolve to a
    single, consistent toolchain stack.

    This performs two kinds of check:

    1. Every declared prefix variable (`MPIHOME`, `NETCDFHOME`, `NETCDFFHOME`,
       `PNETCDFHOME`) that is present in the system environment file must not
       have expanded to an empty string (which usually means the module that
       provides it was not loaded).
    2. The NetCDF (`nf-config`, `nc-config`) and MPI tools that will be used
       for the build must all come from the same place: on an Lmod
       (module-stack) system, none of them may resolve into an active conda
       environment; on a conda-based system, all of them must resolve into
       the active conda environment (if one is active).

    Parameters
    ----------
    mpi_wrapper : str | None
        An explicit path to the MPI Fortran wrapper that will be used for
        the build (as returned by `explicit_mpi_wrapper`), or `None` if the
        Makefile's autodetection will be relied upon instead.

    Raises
    ------
    OSError
        If a declared prefix variable is empty, if the NetCDF tooling
        cannot be found on `PATH`, or if the resolved tools do not form a
        single consistent toolchain stack for the current system.
    """
    cstar_sysmgr = get_sysmgr()
    env_vars = cstar_sysmgr.environment.environment_variables

    for var in _DECLARED_PREFIX_VARS:
        if var in env_vars and not (env_vars[var] or "").strip():
            raise OSError(
                f"Cannot verify ROMS build toolchain: {var} is declared in the "
                f"system environment file ({cstar_sysmgr.environment.system_env_path}) "
                "but expanded to an empty string. This usually means the module "
                f"that provides it is not loaded (e.g. for NETCDFFHOME on Anvil, "
                "the `netcdf-fortran` module sets NETCDF_FORTRAN_HOME)."
            )

    tools: dict[str, str] = {}

    nf_config = shutil.which("nf-config")
    nc_config = shutil.which("nc-config")
    missing = [
        name
        for name, path in (("nf-config", nf_config), ("nc-config", nc_config))
        if not path
    ]
    if missing:
        raise OSError(
            f"Cannot verify ROMS build toolchain: {' and '.join(missing)} not found "
            "on PATH. On an HPC system the netcdf module is probably not loaded; "
            "on a laptop the conda environment is probably not active."
        )
    tools["nf-config"] = str(nf_config)
    tools["nc-config"] = str(nc_config)

    if mpi_wrapper is not None:
        if not Path(mpi_wrapper).exists():
            raise OSError(
                f"Cannot verify ROMS build toolchain: MPI wrapper {mpi_wrapper} "
                "does not exist."
            )
        tools["MPI wrapper"] = mpi_wrapper
    else:
        candidates = _MPI_WRAPPER_CANDIDATES.get(
            cstar_sysmgr.environment.compiler, ("mpifort",)
        )
        for name in candidates:
            found = shutil.which(name)
            if found:
                tools["MPI wrapper"] = found
                break

    resolved: dict[str, Path] = {
        name: Path(path).resolve() for name, path in tools.items()
    }

    conda_prefix = os.environ.get("CONDA_PREFIX")
    resolved_conda_prefix = Path(conda_prefix).resolve() if conda_prefix else None

    if cstar_sysmgr.environment.uses_lmod:
        if resolved_conda_prefix is not None:
            for name, path in resolved.items():
                if path.is_relative_to(resolved_conda_prefix):
                    raise OSError(
                        f"Cannot verify ROMS build toolchain: {name} resolves to "
                        f"{path}, which is inside the active conda environment "
                        f"({resolved_conda_prefix}). An active conda environment "
                        "is shadowing the module stack's tools; run `conda "
                        "deactivate` before building."
                    )
    elif resolved_conda_prefix is not None:
        for name, path in resolved.items():
            if not path.is_relative_to(resolved_conda_prefix):
                raise OSError(
                    f"Cannot verify ROMS build toolchain: {name} resolves to "
                    f"{path}, which is outside the active conda environment "
                    f"({resolved_conda_prefix}). A tool from outside the active "
                    "conda environment would produce a mixed-linkage binary."
                )


def _resolved_conda_prefix() -> Path | None:
    """Return the resolved path of the active conda environment, if any."""
    conda_prefix = os.environ.get("CONDA_PREFIX")
    return Path(conda_prefix).resolve() if conda_prefix else None


def _verify_linkage_linux(exe_path: Path) -> None:
    """Verify linkage of the ROMS binary on Linux, via `ldd` and `readelf`.

    Parameters
    ----------
    exe_path : Path
        The path to the compiled `roms` binary.

    Raises
    ------
    RuntimeError
        If a checked library is unresolved, resolves into an active conda
        environment on a module-stack (Lmod) system, or (when `LD_RUN_PATH`
        is declared for this system) the binary lacks the expected RUNPATH
        entry.
    """
    if not shutil.which("ldd"):
        log.warning(
            f"`ldd` not available; skipping linkage verification for {exe_path}."
        )
    else:
        stdout = _run_cmd(f"ldd {exe_path}", raise_on_error=False)
        # Only reject conda-resolved libraries on module-stack (Lmod) systems:
        # on conda-based Linux the build *should* link the active env's libraries.
        resolved_conda_prefix = (
            _resolved_conda_prefix() if get_sysmgr().environment.uses_lmod else None
        )

        for line in stdout.splitlines():
            stripped = line.strip()
            if not any(pattern in stripped for pattern in _LINKAGE_LIB_PATTERNS):
                continue

            if "not found" in stripped:
                raise RuntimeError(
                    f"ROMS binary {exe_path} has unresolved linkage: {stripped!r}. "
                    "The library could not be located at runtime; check that the "
                    "module/environment providing it is loaded when running ROMS."
                )

            if "=>" in stripped and resolved_conda_prefix is not None:
                target = stripped.split("=>", 1)[1].strip().split(" ")[0]
                if target:
                    try:
                        resolved_target = Path(target).resolve()
                    except OSError:
                        resolved_target = None
                    if resolved_target is not None and resolved_target.is_relative_to(
                        resolved_conda_prefix
                    ):
                        raise RuntimeError(
                            f"ROMS binary {exe_path} links against {stripped!r}, "
                            f"which resolves inside the active conda environment "
                            f"({resolved_conda_prefix}). This indicates the build "
                            "picked up a conda library instead of the module "
                            "stack's."
                        )

    env_vars = get_sysmgr().environment.environment_variables
    ld_run_path = env_vars.get("LD_RUN_PATH")
    if not ld_run_path:
        return

    if not shutil.which("readelf"):
        log.warning(
            f"`readelf` not available; skipping RUNPATH verification for {exe_path}."
        )
        return

    stdout = _run_cmd(f"readelf -d {exe_path}", raise_on_error=False)
    rpath_entries: list[str] = []
    for line in stdout.splitlines():
        if ("RPATH" in line or "RUNPATH" in line) and "[" in line and "]" in line:
            inner = line.split("[", 1)[1].rsplit("]", 1)[0]
            rpath_entries = inner.split(":")
            break

    netcdff_home = env_vars.get("NETCDFFHOME") or env_vars.get("NETCDFHOME")
    expected_dir = str(Path(netcdff_home) / "lib") if netcdff_home else None

    if not expected_dir or expected_dir not in rpath_entries:
        raise RuntimeError(
            f"ROMS binary {exe_path} is not self-locating: RUNPATH missing "
            f"expected directory {expected_dir}. Found RUNPATH entries: "
            f"{rpath_entries}. The binary would rely on LD_LIBRARY_PATH at "
            "runtime."
        )


def _verify_linkage_darwin(exe_path: Path) -> None:
    """Verify linkage of the ROMS binary on macOS, via `otool -L`.

    Parameters
    ----------
    exe_path : Path
        The path to the compiled `roms` binary.

    Raises
    ------
    RuntimeError
        If a linked dependency does not resolve under the active conda
        environment, `/usr/lib`, `/System/`, or one of `@rpath`,
        `@loader_path`, `@executable_path`.
    """
    resolved_conda_prefix = _resolved_conda_prefix()
    if resolved_conda_prefix is None or not shutil.which("otool"):
        log.warning(
            "CONDA_PREFIX not set or `otool` not available; skipping linkage "
            f"verification for {exe_path}."
        )
        return

    allowed_path_prefixes = (str(resolved_conda_prefix), "/usr/lib", "/System/")
    allowed_literal_prefixes = ("@rpath", "@loader_path", "@executable_path")

    stdout = _run_cmd(f"otool -L {exe_path}", raise_on_error=False)
    # The first line names the binary itself; remaining lines list dependencies.
    for line in stdout.splitlines()[1:]:
        stripped = line.strip()
        if not stripped:
            continue
        dep_path = stripped.split(" (", 1)[0].strip()

        if dep_path.startswith(allowed_literal_prefixes):
            continue
        if any(dep_path.startswith(prefix) for prefix in allowed_path_prefixes):
            continue

        raise RuntimeError(
            f"ROMS binary {exe_path} links against {dep_path!r}, which is "
            "outside the active conda environment, /usr/lib, and /System. "
            "This indicates mixed-toolchain linkage."
        )


def verify_roms_linkage(exe_path: Path) -> None:
    """Verify that the compiled ROMS binary is not linked against a mixed set of
    toolchains, and (on Linux, where declared) embeds a RUNPATH so it does not
    depend on `LD_LIBRARY_PATH` at runtime.

    Parameters
    ----------
    exe_path : Path
        The path to the compiled `roms` binary.

    Raises
    ------
    RuntimeError
        If the linkage check fails. See `_verify_linkage_linux` and
        `_verify_linkage_darwin` for platform-specific details.
    """
    if sys.platform == "linux":
        _verify_linkage_linux(exe_path)
    elif sys.platform == "darwin":
        _verify_linkage_darwin(exe_path)
