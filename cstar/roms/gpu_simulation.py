"""A ROMS simulation that runs on gpu-ucla-roms, the CUDA-Fortran port of ucla-roms.

The GPU model is built from its own source (MARBL included, as a git submodule)
by `make gpu`, takes no cppdefs or compile-time code, reads the ucla-roms 0.11.0
namelist vocabulary, reads whole input files and writes whole NetCDF-4 outputs.
It runs on the `gpu` toolchain profile of the system (see
`cstar.system.manager.CStarSystemManager.environment_for`).

Two of the executable's inputs are fixed at build time: the reference density
`rho0` (the model stops at start-up when the namelist differs) and the storage
precision of the passive tracers. :class:`GpuBuildKey` holds the build's inputs
beyond its source, so a build is reused only for a namelist it suits.

See https://github.com/CWorthy-ocean/gpu-ucla-roms (README: "Build" and "Run"),
pinned at ``v0.1.2-beta``.
"""

from __future__ import annotations

import dataclasses
import json
import math
import os
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import yaml

from cstar.applications.ucla_roms.adapter import (
    AddtlCodeAdapter,
    BoundaryForcingAdapter,
    CarbonateSensitivityAdapter,
    CdrForcingAdapter,
    ForcingCorrectionAdapter,
    GridAdapter,
    InitialConditionAdapter,
    NestingInfoAdapter,
    RiverForcingAdapter,
    SurfaceForcingAdapter,
    TidalForcingAdapter,
)
from cstar.applications.ucla_roms_gpu.models import TOOLCHAIN, UclaRomsGpuBlueprint
from cstar.base.exceptions import CstarError
from cstar.base.utils import _get_sha256_hash, _run_cmd
from cstar.entrypoint.utils import ARG_CLOBBER
from cstar.execution.file_system import is_remote_resource
from cstar.io.source_data import SourceData
from cstar.orchestration.serialization import load_blueprint
from cstar.roms.build_verification import (
    _get_dependency_root,
    assert_single_toolchain_stack,
    rpath_link_flags,
    verify_roms_linkage,
)
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.file_system import BUILD_RECORD_NAME
from cstar.roms.gpu_external_codebase import ROMSGpuExternalCodeBase
from cstar.roms.gpu_layout import (
    GPU_WRITER_RATE_GBPS,
    GPU_WRITER_TARGET_SECONDS,
    GpuRankLayout,
    history_record_gb,
    suggested_writers_per_node,
    writers_per_file,
)
from cstar.roms.namelist import gpu_namelist_schema_for_ref
from cstar.roms.precheck import check_gpu_features
from cstar.roms.simulation import IOLayout, ROMSSimulation
from cstar.system.manager import get_sysmgr

if TYPE_CHECKING:
    from datetime import datetime

    from cstar.base.additional_code import AdditionalCode
    from cstar.roms.input_dataset import (
        ROMSBoundaryForcing,
        ROMSCarbonateSensitivity,
        ROMSCdrForcing,
        ROMSForcingCorrections,
        ROMSInitialConditions,
        ROMSModelGrid,
        ROMSNestingInfo,
        ROMSRiverForcing,
        ROMSSurfaceForcing,
        ROMSTidalForcing,
    )
    from cstar.roms.namelist import RomsNamelistBase
    from cstar.system.environment import CStarEnvironment

GPU_EXECUTABLE_NAME: Final = "roms_gpu_cuda"
"""The executable `make gpu` writes (the Makefile's `gpu` target, gpu-ucla-roms
v0.1.2-beta)."""

GPU_BUILD_DIR_NAME: Final = "build_gpu"
"""The directory, in the repository's clone, that `make gpu` writes the executable
to (`GPUDIR` in the Makefile, gpu-ucla-roms v0.1.2-beta)."""

GPU_WRITERS_ENV_VAR: Final = "ROMS_GPU_NWRITERS"
"""The environment variable that gives the model the writer ranks on each node: the
last that many ranks of every node write output (`src/shared/comm.F90`,
gpu-ucla-roms v0.1.2-beta)."""

GPU_BUILD_RECORD_KEY: Final = "gpu_build"
"""The entry of the build record that holds the `GpuBuildKey` the executable was
built for."""


def fortran_double_literal(value: float) -> str:
    """Write a number as a Fortran double-precision literal.

    The GPU model takes its reference density from a preprocessor macro,
    ``rho0 = real(ROMS_RHO0, rp)``, and stops at start-up when the namelist's
    `rho0` differs from it by more than 1e-6 (`src/host/case_io.F90`,
    gpu-ucla-roms v0.1.2-beta). A plain ``1025.3`` is a default-real (single
    precision) literal, about 1025.30005, so it must be written ``1025.3d0``.

    Parameters
    ----------
    value : float
        The number to write.

    Returns
    -------
    str
        The shortest literal that reads back as `value` in double precision, e.g.
        ``1025.3d0``, ``1000.0d0`` or ``1d+20``.

    Raises
    ------
    ValueError
        If `value` is not finite.
    """
    if not math.isfinite(value):
        raise ValueError(f"cannot write {value!r} as a Fortran literal")
    text = repr(float(value))
    if "e" in text:
        return text.replace("e", "d")
    return f"{text}d0"


@dataclass(frozen=True)
class GpuBuildKey:
    """What a compiled GPU executable depends on, beyond its source.

    A build is reused only when its key equals the key of the simulation that
    would use it.
    """

    compiler: str
    """The Fortran compiler, e.g. ``nvfortran``."""
    gpu_arch: str
    """The GPU architecture the code is generated for, e.g. ``cc80`` (A100)."""
    rho0: float
    """The reference density the model is compiled with; the namelist must agree."""
    passive_fp64: bool
    """Whether the MARBL and passive tracers are stored in double precision. Set
    when the run has MARBL tracers: the validated configuration for MARBL, whose
    results change with single precision. Without MARBL the passive tracers are
    stored as float32."""

    def fflags(self) -> str:
        """Compose the compile flags of `FFLAGS_GPU`.

        The flags of the README's build recipe. The Makefile itself appends
        `FFLAGS_MPI` and `NETCDF_FFLAGS`.

        Returns
        -------
        str
        """
        flags = ["-O2", "-g", "-cuda", f"-gpu={self.gpu_arch}", "-DUSE_CUDA"]
        if self.passive_fp64:
            flags.append("-DPASSIVE_FP64")
        flags += [
            f"-DROMS_RHO0={fortran_double_literal(self.rho0)}",
            "-Mpreprocess",
            "-Mfree",
            "-Mnofma",
            "-Isrc/shared",
        ]
        return " ".join(flags)


def _library_dirs(link_flags: str) -> list[str]:
    """List the directories that `-L` flags name, in order.

    Parameters
    ----------
    link_flags : str
        A linker flag string, e.g. a toolchain's extra link flags.

    Returns
    -------
    list[str]
    """
    tokens = shlex.split(link_flags)
    dirs: list[str] = []
    for index, token in enumerate(tokens):
        if token == "-L" and index + 1 < len(tokens):
            dirs.append(tokens[index + 1])
        elif token.startswith("-L") and len(token) > len("-L"):
            dirs.append(token[len("-L") :])
    return dirs


def gpu_make_command(
    key: GpuBuildKey,
    mpi_home: Path,
    netcdff_home: Path,
    netcdf_home: Path,
    link_flags: str,
) -> str:
    """Compose the ``make gpu`` command that builds the GPU executable.

    The general form of the README's build recipe, with the MPI and NetCDF
    installations as parameters. The libraries are named explicitly, with an
    rpath for each library directory, so the executable runs without
    `LD_LIBRARY_PATH` set.

    The NetCDF flags come from the installations, not from ``nf-config``; the
    include flag is passed as `NETCDF_FFLAGS` and the libraries in `LIBS`, which
    override the Makefile's own ``nf-config`` defaults:
    Cray's NetCDF modules ship an ``nf-config`` that prints no flags (their
    ``ftn`` wrapper adds them), and a conda environment early on ``PATH`` can
    supply an ``nf-config`` built for another compiler.

    Parameters
    ----------
    key : GpuBuildKey
        The compiler, GPU architecture and build constants.
    mpi_home : Path
        The MPI installation: its `include` and `lib` directories are used.
    netcdff_home : Path
        The NetCDF-Fortran installation: `netcdf.mod` in its `include` directory,
        `libnetcdff` in its `lib` directory.
    netcdf_home : Path
        The NetCDF-C installation, whose `lib` directory holds `libnetcdf`; the
        same as `netcdff_home` where both are installed together.
    link_flags : str
        Extra link flags for the GPU build, after the MPI libraries (e.g. Cray
        MPICH's GPU transport layer), with their variables already expanded.

    Returns
    -------
    str
        The shell command; run it in the root of the gpu-ucla-roms clone.
    """
    netcdf_libs = list(
        dict.fromkeys([str(netcdff_home / "lib"), str(netcdf_home / "lib")])
    )
    mpi_lib = mpi_home / "lib"
    rpath_dirs = dict.fromkeys([*netcdf_libs, str(mpi_lib), *_library_dirs(link_flags)])
    libs = " ".join(
        part
        for part in (
            *(f"-L{d}" for d in netcdf_libs),
            "-lnetcdff",
            "-lnetcdf",
            f"-L{mpi_lib}",
            "-lmpifort",
            "-lmpi",
            link_flags.strip(),
            *(f"-Wl,-rpath,{d}" for d in rpath_dirs),
        )
        if part
    )
    variables = {
        "FC_GPU": key.compiler,
        "FFLAGS_GPU": key.fflags(),
        "FFLAGS_MPI": f"-DUSE_MPI -I{mpi_home / 'include'}",
        "NETCDF_FFLAGS": f"-I{netcdff_home / 'include'}",
        "LIBS": libs,
    }
    return "make gpu " + " ".join(
        f"{name}={shlex.quote(value)}" for name, value in variables.items()
    )


def _expand_link_flags(link_flags: str) -> str:
    """Expand the environment variables a toolchain's link flags name.

    Parameters
    ----------
    link_flags : str
        The link flags of the system's GPU toolchain, e.g.
        ``${PE_MPICH_GTL_DIR_nvidia80} ${PE_MPICH_GTL_LIBS_nvidia80}``.

    Returns
    -------
    str
        The flags with every variable replaced by its value.

    Raises
    ------
    CstarError
        If a variable is not set, which means the GPU modules that set it are not
        loaded.
    """
    expanded = os.path.expandvars(link_flags)
    if "$" in expanded:
        unset = [token for token in expanded.split() if "$" in token]
        raise CstarError(
            f"The GPU link flags {link_flags!r} name variables that are not set: "
            f"{', '.join(unset)}. The GPU toolchain's modules set them; check that "
            "they are loaded (the system's .gpu.lmod file) and that "
            "the system's GPU toolchain in C-Star names the right variables."
        )
    return expanded


class ROMSGpuSimulation(ROMSSimulation):
    """A `ROMSSimulation` that builds and runs gpu-ucla-roms.

    The GPU model differs from ucla-roms where this class overrides the base:
    it is built from its own source (`ROMSGpuExternalCodeBase`, MARBL included)
    with no compile-time code, cppdefs, MARBL or ParallelIO codebases; it reads
    whole input files and writes whole outputs (a joined `io_layout`); and it
    is launched with one MPI rank per tile plus the writer ranks of
    `rank_layout`. Staging, namelist composition, pre-run and post-run are the
    base class's.

    Every method that touches the system, `setup`, `attach`, `build` and
    `_launch_command`, first switches the process to the system's `gpu`
    toolchain profile, and fails at once on a system that has none.

    Attributes
    ----------
    rank_layout : GpuRankLayout
        The compute and writer ranks of the job, from `discretization` and
        the constructor's `nodes` and `writers_per_node`.

    See Also
    --------
    ROMSSimulation : The base class, with the parameters this one shares.
    """

    def __init__(
        self,
        name: str,
        directory: str | Path,
        discretization: ROMSDiscretization,
        runtime_code: AdditionalCode,
        codebase: ROMSGpuExternalCodeBase | None = None,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        valid_start_date: str | datetime | None = None,
        valid_end_date: str | datetime | None = None,
        model_grid: ROMSModelGrid | None = None,
        initial_conditions: ROMSInitialConditions | None = None,
        tidal_forcing: ROMSTidalForcing | None = None,
        river_forcing: ROMSRiverForcing | None = None,
        cdr_forcing: ROMSCdrForcing | None = None,
        nesting_info: ROMSNestingInfo | None = None,
        boundary_forcing: list[ROMSBoundaryForcing] | None = None,
        surface_forcing: list[ROMSSurfaceForcing] | None = None,
        forcing_corrections: list[ROMSForcingCorrections] | None = None,
        carbonate_sensitivity: list[ROMSCarbonateSensitivity] | None = None,
        namelist_overrides: dict[str, dict[str, Any]] | None = None,
        nodes: int = 1,
        writers_per_node: int = 1,
    ):
        """Initializes a `ROMSGpuSimulation` instance.

        The parameters are those of `ROMSSimulation`, without the ones that
        have no meaning for the GPU model (`compile_time_code`, `marbl_codebase`,
        `use_pio`, `pio_codebase`, `auto_tiling`), and with the job's layout.

        Parameters
        ----------
        name : str
            The name of the simulation.
        directory : str or Path
            Path to the directory where simulation files and outputs will be stored.
        discretization : ROMSDiscretization
            The processor grid: `n_procs_x` x `n_procs_y` tiles, one GPU each.
        runtime_code : AdditionalCode
            The namelist and the MARBL settings the model reads at run time.
        codebase : ROMSGpuExternalCodeBase, optional
            The gpu-ucla-roms repository to build. If not provided, the default
            codebase is used.
        nodes : int, optional
            The nodes the job spans; the tiles are spread evenly over them.
        writers_per_node : int, optional
            The writer ranks on each node (see `GpuRankLayout`).

        Raises
        ------
        ValueError
            If `discretization` does not set `n_procs_x` and `n_procs_y`, or the
            tiles and nodes do not make a valid layout (see `GpuRankLayout`).

        See Also
        --------
        ROMSSimulation.__init__ : The remaining parameters.
        """
        super().__init__(
            name=name,
            directory=directory,
            discretization=discretization,
            runtime_code=runtime_code,
            compile_time_code=None,
            codebase=codebase,
            start_date=start_date,
            end_date=end_date,
            valid_start_date=valid_start_date,
            valid_end_date=valid_end_date,
            marbl_codebase=None,
            model_grid=model_grid,
            initial_conditions=initial_conditions,
            tidal_forcing=tidal_forcing,
            river_forcing=river_forcing,
            cdr_forcing=cdr_forcing,
            nesting_info=nesting_info,
            boundary_forcing=boundary_forcing,
            surface_forcing=surface_forcing,
            forcing_corrections=forcing_corrections,
            carbonate_sensitivity=carbonate_sensitivity,
            use_pio=False,
            pio_codebase=None,
            auto_tiling=False,
            namelist_overrides=namelist_overrides,
        )

        n_procs_x, n_procs_y = discretization.n_procs_x, discretization.n_procs_y
        if n_procs_x is None or n_procs_y is None:
            raise ValueError(
                "The GPU model runs one tile per GPU, so discretization.n_procs_x "
                "and n_procs_y must both be set (tiling.n_procs_x and n_procs_y "
                "in a blueprint)."
            )
        self.rank_layout = GpuRankLayout(
            tiles=n_procs_x * n_procs_y,
            nodes=nodes,
            writers_per_node=writers_per_node,
        )

        # the key of the build this process made, for the build record
        self._build_key: GpuBuildKey | None = None

    @classmethod
    def _check_compile_time_code(cls, compile_time_code: AdditionalCode | None) -> None:
        """Refuse compile-time code: the GPU model is built from its own source.

        Parameters
        ----------
        compile_time_code : AdditionalCode or None
            The compile-time code the simulation was constructed with.

        Raises
        ------
        ValueError
            If `compile_time_code` is given.
        """
        if compile_time_code is not None:
            raise ValueError(
                "gpu-ucla-roms builds from its own source and takes no compile-time "
                "code (cppdefs.opt or source files); remove the compile-time code."
            )

    @property
    def io_layout(self) -> IOLayout:
        """The GPU model reads whole input files and writes joined NetCDF-4
        outputs straight into `output`.

        Returns
        -------
        IOLayout
            Always `IOLayout.JOINED`.
        """
        return IOLayout.JOINED

    @property
    def default_codebase(self) -> ROMSGpuExternalCodeBase:
        """Returns the default gpu-ucla-roms external codebase.

        Returns
        -------
        ROMSGpuExternalCodeBase
            A default instance of the GPU ROMS external codebase.
        """
        return ROMSGpuExternalCodeBase()

    def _select_namelist_schema(self) -> type[RomsNamelistBase]:
        """Select the namelist schema for this simulation's gpu-ucla-roms pin.

        Every gpu-ucla-roms release reads the ucla-roms 0.11.0 vocabulary (see
        `gpu_namelist_schema_for_ref`).
        """
        codebase_copy = self.codebase.working_copy
        return gpu_namelist_schema_for_ref(
            self.codebase.source.checkout_target,
            repo_path=codebase_copy.path if codebase_copy is not None else None,
        )

    def _gpu_environment(self) -> CStarEnvironment:
        """Switch the process to the system's GPU toolchain profile.

        Loads the profile's modules and environment file, so it is the first thing
        `setup` and `attach` do: a system with no GPU toolchain fails before
        anything is cloned or downloaded. Later calls change nothing.

        Returns
        -------
        CStarEnvironment
            The GPU environment: its MPI launch prefix, compiler and variables.

        Raises
        ------
        CstarError
            If the system registers no GPU toolchain.
        """
        return get_sysmgr().environment_for(TOOLCHAIN)

    def setup(self, clobber: bool = False) -> None:
        """Prepare this simulation locally, in the system's GPU toolchain.

        Switches to the GPU toolchain profile first, so a system without one
        fails before anything is staged; then stages as `ROMSSimulation.setup`.

        Parameters
        ----------
        clobber : bool
            Empty the working directory first, so a prior attempt's files are not
            reused.

        Raises
        ------
        CstarError
            If the system has no GPU toolchain.
        """
        self._gpu_environment()
        super().setup(clobber=clobber)

    def attach(self) -> None:
        """Adopt a previously staged and built working directory, in the system's
        GPU toolchain.

        Switches to the GPU toolchain profile first, then adopts as
        `ROMSSimulation.attach`, which also checks the executable against this
        simulation's build key (see `_validate_build_flags`).

        Raises
        ------
        CstarError
            If the system has no GPU toolchain.
        CstarExpectationFailed
            Listing every previously staged component that is missing or invalid.
        """
        self._gpu_environment()
        super().attach()

    @property
    def _build_dir(self) -> Path | None:
        """The directory `make gpu` writes the executable to.

        Returns
        -------
        Path or None
            `build_gpu` in the gpu-ucla-roms clone, or `None` when the clone is
            not staged.
        """
        codebase_copy = self.codebase.working_copy
        if codebase_copy is None:
            return None
        return codebase_copy.path / GPU_BUILD_DIR_NAME

    @property
    def _executable_name(self) -> str:
        """The file name of the GPU executable in `_build_dir`."""
        return GPU_EXECUTABLE_NAME

    def _composed_namelist(self) -> RomsNamelistBase:
        """Read the runtime namelist and layer `namelist_overrides` onto it.

        What the model would read, but for the settings C-Star derives from the
        staged inputs: enough to answer for the build's constants and features.

        Raises
        ------
        ValueError
            If the namelist is not staged, or it or the overrides fail
            validation.
        """
        nml, schema, chosen_by = self._read_raw_namelist()
        return self._layer_namelist_overrides(nml, schema, chosen_by)

    def _check_runtime_settings(self, nml: RomsNamelistBase) -> None:
        """Check the composed namelist against what gpu-ucla-roms needs.

        Rule E, the GPU model's start-up feature guard, then the base class's
        rules. Of those, rule D (file names too long to read) mirrors the model;
        rule A (every output stream's rollover divides the restart period) the
        GPU model does not check, but it names and splits files as ucla-roms
        does, so a rollover that does not divide the restart period leaves a
        partial file at every restart that resume and `continue-from` start
        from; and rule C applies to ucla-roms 0.13 namelists only, never to the
        0.11.0 vocabulary the GPU reads. With no cppdefs, rule A checks only the
        streams no ucla-roms cppdef gates.

        When `rank_layout` has fewer writer ranks than
        `suggested_writers_per_node` gives, says so: a warning when one history
        record would take this run's writers longer than the rule's target to
        write, otherwise for information only.

        Parameters
        ----------
        nml : RomsNamelistBase
            The composed namelist.

        Raises
        ------
        NamelistConsistencyError
            If the namelist switches on a feature the GPU model does not have,
            has an output stream whose rollover does not divide the restart
            period, or names a file too long for the model to read.
        """
        check_gpu_features(nml)
        super()._check_runtime_settings(nml)

        layout = self.rank_layout
        suggested = suggested_writers_per_node(nml, layout.nodes)
        if layout.writers_per_node >= suggested:
            return
        # the rule also doubles the writers when history and restart are written
        # at once; that only matters when the record itself is slow to write,
        # by the writers of one file's group
        record_gb = history_record_gb(nml)
        seconds = record_gb / (GPU_WRITER_RATE_GBPS * writers_per_file(layout.writers))
        message = (
            f"tiling.writers_per_node is {layout.writers_per_node}; gpu-ucla-roms' "
            f"tools/gpu_layout.py suggests {suggested} per node on {layout.nodes} "
            f"node(s) for this namelist's history output (~{record_gb:.2f} GB per "
            f"record, ~{seconds:.0f} s to write with this run's writers)"
        )
        if seconds > GPU_WRITER_TARGET_SECONDS:
            self.log.warning(
                f"{message}; writing a record may stall the GPUs. Raise "
                "tiling.writers_per_node."
            )
        else:
            self.log.info(message)

    def _cdr_lite_enabled(self) -> bool:
        """Whether the run has CDR_LITE tracers.

        On the GPU, CDR_LITE is a run-time switch, not a build feature: it is on
        when the composed namelist asks for any OAE or DOR tracer.

        Returns
        -------
        bool
            `True` when `nt_cdr_oae + nt_cdr_dor` is above zero.
        """
        param_settings = self._composed_namelist().param_settings
        n_cdr = getattr(param_settings, "nt_cdr_oae", 0) + getattr(
            param_settings, "nt_cdr_dor", 0
        )
        return n_cdr > 0

    def _gpu_build_key(self) -> GpuBuildKey:
        """Describe the build this simulation needs, beyond the source.

        Returns
        -------
        GpuBuildKey
            The system's GPU compiler and architecture, with the `rho0` and the
            tracer storage precision the composed namelist asks for.

        Raises
        ------
        CstarError
            If the system's GPU toolchain names no GPU architecture.
        ValueError
            If the runtime namelist is not staged or fails validation.
        """
        toolchain = get_sysmgr().toolchain(TOOLCHAIN)
        if not toolchain.gpu_arch:
            raise CstarError(
                f"The {TOOLCHAIN} toolchain of {get_sysmgr().name} names no GPU "
                "architecture (gpu_arch), which the GPU build needs for -gpu=<arch>."
            )
        nml = self._composed_namelist()
        return GpuBuildKey(
            compiler=toolchain.compiler,
            gpu_arch=toolchain.gpu_arch,
            rho0=float(nml.rho0_settings.rho0),
            passive_fp64=nml.param_settings.nt_bgc > 0,
        )

    def _recorded_build_key(self, build_dir: Path) -> dict[str, Any] | None:
        """Read the `GpuBuildKey` the build record beside the executable holds.

        Parameters
        ----------
        build_dir : Path
            The directory holding the executable and its build record.

        Returns
        -------
        dict[str, Any] or None
            The recorded key as `dataclasses.asdict` writes it, or `None` when
            there is no readable record or it holds no key (a build by a
            C-Star without the GPU model).
        """
        try:
            record = json.loads((build_dir / BUILD_RECORD_NAME).read_text())
        except (OSError, ValueError):
            return None
        key = record.get(GPU_BUILD_RECORD_KEY) if isinstance(record, dict) else None
        return key if isinstance(key, dict) else None

    def _mpi_home(self) -> Path:
        """Find the MPI installation of the active (GPU) toolchain.

        Returns
        -------
        Path
            The value of `MPIHOME`, from the profile's environment file or the
            process environment.

        Raises
        ------
        CstarError
            If `MPIHOME` is set in neither.
        """
        mpi_home = _get_dependency_root("MPIHOME")
        if not mpi_home:
            raise CstarError(
                "MPIHOME is not set: the GPU toolchain profile of "
                f"{get_sysmgr().name} (its .gpu.env file) must set MPIHOME to "
                "the MPI installation, whose include and lib directories the GPU "
                "build uses."
            )
        return Path(mpi_home)

    def _netcdf_homes(self) -> tuple[Path, Path]:
        """Find the NetCDF installations of the active (GPU) toolchain.

        `NETCDFFHOME` names the NetCDF-Fortran installation where it is apart
        from NetCDF-C; otherwise both are under `NETCDFHOME`. Build verification
        reads the two variables the same way.

        Returns
        -------
        tuple[Path, Path]
            The NetCDF-Fortran installation and the NetCDF-C installation.

        Raises
        ------
        CstarError
            If neither `NETCDFFHOME` nor `NETCDFHOME` is set.
        """
        netcdf_home = _get_dependency_root("NETCDFHOME")
        netcdff_home = _get_dependency_root("NETCDFFHOME") or netcdf_home
        if not netcdff_home:
            raise CstarError(
                "NETCDFHOME is not set: the GPU toolchain profile of "
                f"{get_sysmgr().name} (its .gpu.env file) must set NETCDFHOME to "
                "the NetCDF installation (and NETCDFFHOME to NetCDF-Fortran's, if "
                "it is installed apart), whose include and lib directories the GPU "
                "build uses."
            )
        return Path(netcdff_home), Path(netcdf_home or netcdff_home)

    def build(self, rebuild: bool = False) -> None:
        """Compile the GPU executable from the gpu-ucla-roms source.

        Runs ``make gpu`` in the clone, with the system's GPU compiler,
        architecture and link flags and the `rho0` and tracer precision this
        simulation's namelist needs (see `GpuBuildKey`). An executable already
        built, unchanged and for the same key is reused unless `rebuild` is set.
        The build is verified for a single toolchain stack and consistent
        linkage, and a record of the namelist schema and the build key is written
        beside the executable for `attach`.

        Parameters
        ----------
        rebuild : bool, optional
            If True, forces recompilation even if the executable is up to date.

        Raises
        ------
        ValueError
            If the codebase is not staged, or the runtime namelist is not staged.
        CstarError
            If the GPU toolchain does not name a GPU architecture, `MPIHOME` or
            `NETCDFHOME` is not set, or a variable of the toolchain's link flags
            is not set.
        RuntimeError
            If an error occurs during the compilation process.

        See Also
        --------
        setup : Stages the codebase and the namelist before compilation.
        """
        codebase_copy = self.codebase.working_copy
        if codebase_copy is None:
            raise ValueError(
                "Unable to compile ROMSGpuSimulation: "
                + "\nROMSGpuSimulation.codebase is not staged locally."
                + "\n Call ROMSGpuSimulation.setup() and try again"
            )

        self._gpu_environment()
        key = self._gpu_build_key()

        build_dir = codebase_copy.path / GPU_BUILD_DIR_NAME
        exe_path = build_dir / self._executable_name
        if (
            (exe_path.exists())
            and (self._exe_hash is not None)
            and (_get_sha256_hash(exe_path) == self._exe_hash)
            and (self._recorded_build_key(build_dir) == dataclasses.asdict(key))
            and not rebuild
        ):
            self.log.info(
                f"The GPU model has already been built at {exe_path} for this "
                "namelist, and the source code appears not to have changed. "
                "If you would like to recompile, call "
                "ROMSGpuSimulation.build(rebuild = True)"
            )
            return

        # an executable this build replaces takes its record with it, so a
        # record never outlives the build it describes
        self._namelist_schema = None
        self._build_key = None
        try:
            (build_dir / BUILD_RECORD_NAME).unlink(missing_ok=True)
        except OSError as e:
            self.log.warning(f"Could not remove the previous GPU build record: {e}")

        assert_single_toolchain_stack(None)
        mpi_home = self._mpi_home()
        netcdff_home, netcdf_home = self._netcdf_homes()
        command = gpu_make_command(
            key,
            mpi_home=mpi_home,
            netcdff_home=netcdff_home,
            netcdf_home=netcdf_home,
            link_flags=" ".join(
                flags
                for flags in (
                    _expand_link_flags(get_sysmgr().toolchain(TOOLCHAIN).link_flags),
                    rpath_link_flags(),
                )
                if flags
            ),
        )
        _run_cmd(
            command,
            cwd=codebase_copy.path,
            msg_pre="Compiling gpu-ucla-roms...",
            msg_post=f"gpu-ucla-roms compiled at {build_dir}",
            msg_err="Error when compiling gpu-ucla-roms.",
            raise_on_error=True,
        )

        verify_roms_linkage(exe_path)

        self.exe_path = exe_path
        self._exe_hash = _get_sha256_hash(exe_path)
        self._namelist_schema = self._select_namelist_schema()
        self._build_key = key
        self._write_build_record(build_dir, self._namelist_schema)

    def _build_record(self, schema: type[RomsNamelistBase]) -> dict[str, Any]:
        """Describe the build for the record written beside the executable.

        Adds the `GpuBuildKey` the executable was compiled for to the base
        record, for `_validate_build_flags`.

        Parameters
        ----------
        schema : type[RomsNamelistBase]
            The namelist schema selected for the build.

        Returns
        -------
        dict[str, Any]
            The JSON-serializable record.
        """
        assert self._build_key is not None, "the build record is written by build()"
        return {
            **super()._build_record(schema),
            GPU_BUILD_RECORD_KEY: dataclasses.asdict(self._build_key),
        }

    def _validate_build_flags(self, build_dir: Path) -> None:
        """Ensure the executable was built for this simulation's build key.

        Called by `attach` before it adopts a previously compiled executable.
        Every build C-Star makes records its key, so an executable without one
        was built by hand or left by a failed rebuild (`build` removes the
        record before it compiles); it is refused, since the model checks only
        `rho0` at start-up and would run with the wrong tracer precision.

        Parameters
        ----------
        build_dir : Path
            The directory holding the executable and its build record.

        Raises
        ------
        ValueError
            If there is no recorded key, or it differs from the one this
            simulation needs; the message names what differs.
        """
        recorded = self._recorded_build_key(build_dir)
        if recorded is None:
            raise ValueError(
                f"it has no GPU build record ({build_dir / BUILD_RECORD_NAME}), so "
                "C-Star cannot tell which rho0 and tracer precision it was built "
                f"for. Re-run the step with {ARG_CLOBBER} to rebuild it"
            )

        needed = dataclasses.asdict(self._gpu_build_key())
        differences = [
            f"{name} was {recorded.get(name)!r} and is now {value!r}"
            for name, value in needed.items()
            if recorded.get(name) != value
        ]
        if differences:
            raise ValueError(
                "it was built for different settings than this simulation needs ("
                + "; ".join(differences)
                + f"). Re-run the step with {ARG_CLOBBER} to rebuild it"
            )

    def _launch_command(self, namelist_name: str) -> str:
        """Build the shell command that launches the GPU model.

        One MPI rank per tile plus the writers, in the GPU toolchain's MPI
        launcher; the job lays its ranks out uniformly over the nodes, and the
        model makes the last `writers_per_node` ranks of every node writers.
        `MPICH_GPU_SUPPORT_ENABLED` comes from the GPU profile's environment
        file, not from here.

        Parameters
        ----------
        namelist_name : str
            The name of the generated namelist, in the run directory the
            command runs from.

        Returns
        -------
        str
            The launch command.
        """
        return " ".join(
            [
                "env",
                f"{GPU_WRITERS_ENV_VAR}={self.rank_layout.writers_per_node}",
                self._gpu_environment().mpi_exec_prefix,
                "-n",
                f"{self.rank_layout.tasks}",
                f"./{self._executable_name}",
                "run",
                namelist_name,
            ],
        )

    @classmethod
    def from_dict(
        cls,
        simulation_dict: dict,
        directory: str | Path,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
    ) -> ROMSGpuSimulation:
        """Not supported: build a GPU simulation with `from_blueprint`.

        Raises
        ------
        NotImplementedError
            Always.
        """
        raise NotImplementedError(
            "A ROMSGpuSimulation cannot be built from a dictionary; "
            "build it with ROMSGpuSimulation.from_blueprint."
        )

    @classmethod
    def from_blueprint(cls, blueprint: str) -> ROMSGpuSimulation:
        """Create a `ROMSGpuSimulation` instance from a `ucla_roms_gpu` YAML blueprint.

        Nothing is fetched: the codebase and the input datasets are staged by
        `setup`.

        Parameters
        ----------
        blueprint : str
            The path or URL to the YAML blueprint file. Relative `location`
            values in a blueprint file resolve against the directory of that
            file.

        Returns
        -------
        ROMSGpuSimulation
            An initialized `ROMSGpuSimulation` instance based on the blueprint data.

        Raises
        ------
        ValueError
            If the blueprint file is not in YAML format or if required fields are missing.

        See Also
        --------
        ROMSSimulation.from_blueprint : The same for a ucla-roms blueprint.
        """
        if is_remote_resource(str(blueprint)):
            # a downloaded blueprint has no directory for relative locations to
            # be relative to, so they are left as authored
            source = SourceData(location=blueprint)
            data = source.retriever.read()
            bp = UclaRomsGpuBlueprint.model_validate(
                yaml.safe_load(data.decode("utf-8"))
            )
        else:
            bp = load_blueprint(blueprint, UclaRomsGpuBlueprint)

        return cls(
            name=bp.name,
            directory=bp.effective_working_dir,
            discretization=ROMSDiscretization(
                n_procs_x=bp.tiling.n_procs_x, n_procs_y=bp.tiling.n_procs_y
            ),
            runtime_code=AddtlCodeAdapter(bp, "run_time").adapt(),
            codebase=ROMSGpuExternalCodeBase(
                source_repo=str(bp.code.roms.location),
                checkout_target=bp.code.roms.checkout_target,
            ),
            start_date=bp.runtime_params.start_date,
            end_date=bp.runtime_params.end_date,
            valid_start_date=bp.valid_start_date,
            valid_end_date=bp.valid_end_date,
            namelist_overrides=bp.namelist_overrides,
            model_grid=GridAdapter(bp).adapt(),
            initial_conditions=InitialConditionAdapter(bp).adapt(),
            tidal_forcing=TidalForcingAdapter(bp).adapt(),
            river_forcing=RiverForcingAdapter(bp).adapt(),
            forcing_corrections=ForcingCorrectionAdapter(bp).adapt(),
            carbonate_sensitivity=CarbonateSensitivityAdapter(bp).adapt(),
            boundary_forcing=BoundaryForcingAdapter(bp).adapt(),
            surface_forcing=SurfaceForcingAdapter(bp).adapt(),
            cdr_forcing=CdrForcingAdapter(bp).adapt(),
            nesting_info=NestingInfoAdapter(bp).adapt(),
            nodes=bp.tiling.nodes,
            writers_per_node=bp.tiling.writers_per_node,
        )
