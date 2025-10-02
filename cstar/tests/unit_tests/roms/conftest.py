from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock

import numpy as np
import pytest

from cstar.base import AdditionalCode
from cstar.base.datasource import DataSource
from cstar.roms import ROMSDiscretization, ROMSExternalCodeBase, ROMSSimulation
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSInputDataset,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)
from cstar.roms.runtime_settings import ROMSRuntimeSettings
from cstar.tests.unit_tests.fake_abc_subclasses import (
    FakeROMSInputDataset,
)


@pytest.fixture
def fake_romsexternalcodebase() -> Generator[ROMSExternalCodeBase, None, None]:
    """Provides a `ROMSExternalCodeBase` instance with fake values for testing."""
    with mock.patch(
        "cstar.base.external_codebase._get_hash_from_checkout_target",
        return_value="test123",
    ):
        yield ROMSExternalCodeBase(
            source_repo="https://github.com/roms/repo.git",
            checkout_target="roms_branch",
        )


################################################################################
# ROMSRuntimeSettings
################################################################################
@pytest.fixture
def fake_romsruntimesettings() -> ROMSRuntimeSettings:
    """Fixture providing a `ROMSRuntimeSettings` instance for testing.

    The example instance corresponds to the file `fixtures/example_runtime_settings.in`
    in order to test the `ROMSRuntimeSettings.to_file` and `from_file` methods.

    Paths do not correspond to real files.

    Returns
    -------
    ROMSRuntimeSettings
       The example ROMSRuntimeSettings instance
    """
    return ROMSRuntimeSettings(
        title="Example runtime settings",
        time_stepping={"ntimes": 360, "dt": 60, "ndtfast": 60, "ninfo": 1},
        bottom_drag={
            "rdrg": 0.0e-4,
            "rdrg2": 1e-3,
            "zob": 1e-2,
            "cdb_min": 1e-4,
            "cdb_max": 1e-2,
        },
        initial={"nrrec": 1, "ininame": Path("input_datasets/roms_ini.nc")},
        forcing={
            "filenames": [
                Path("input_datasets/roms_frc.nc"),
                Path("input_datasets/roms_frc_bgc.nc"),
                Path("input_datasets/roms_bry.nc"),
                Path("input_datasets/roms_bry_bgc.nc"),
            ]
        },
        output_root_name="ROMS_test",
        s_coord={"theta_s": 5.0, "theta_b": 2.0, "tcline": 300.0},
        rho0=1000.0,
        lin_rho_eos={"Tcoef": 0.2, "T0": 1.0, "Scoef": 0.822, "S0": 1.0},
        marbl_biogeochemistry={
            "marbl_namelist_fname": Path("marbl_in"),
            "marbl_tracer_list_fname": Path("marbl_tracer_list_fname"),
            "marbl_diag_list_fname": Path("marbl_diagnostic_output_list"),
        },
        lateral_visc=0.0,
        gamma2=1.0,
        tracer_diff2=[
            0.0,
        ]
        * 38,
        vertical_mixing={"Akv_bak": 0, "Akt_bak": np.zeros(37)},
        my_bak_mixing={"Akq_bak": 1.0e-5, "q2nu2": 0.0, "q2nu4": 0.0},
        sss_correction=7.777,
        sst_correction=10.0,
        ubind=0.1,
        v_sponge=0.0,
        grid=Path("input_datasets/roms_grd.nc"),
        climatology=Path("climfile2.nc"),
    )


################################################################################
# Runtime and compile-time code
################################################################################
@pytest.fixture
def fake_roms_runtime_code(tmp_path) -> AdditionalCode:
    """Provides an example of ROMSSimulation.runtime_code with fake values for testing"""
    directory = tmp_path
    rc = AdditionalCode(
        location=directory.parent,
        subdir="subdir/",
        checkout_target="main",
        files=[
            "file1",
            "file2.in",
            "marbl_in",
            "marbl_tracer_output_list",
            "marbl_diagnostic_output_list",
        ],
    )
    return rc


@pytest.fixture
def fake_roms_compile_time_code(tmp_path) -> AdditionalCode:
    """Provides an example of ROMSSimulation.compile_time_code with fake values for testing"""
    directory = tmp_path
    cc = AdditionalCode(
        location=directory.parent,
        subdir="subdir/",
        checkout_target="main",
        files=["file1.h", "file2.opt"],
    )
    return cc


################################################################################
# ROMSInputDataset
################################################################################
@pytest.fixture
def fake_romsinputdataset_netcdf_local() -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local NetCDF source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'path'
    - DataSource.source_type: Property mocked as 'netcdf'
    - DataSource.basename: Property mocked as 'local_file.nc'

    Yields:
    -------
        FakeROMSInputDataset: A mock dataset pointing to a local NetCDF file.
    """
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
    ):
        mock_location_type.return_value = "path"
        mock_source_type.return_value = "netcdf"

        dataset = FakeROMSInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def fake_romsinputdataset_yaml_local() -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local YAML source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'path'
    - DataSource.source_type: Property mocked as 'yaml'
    - DataSource.basename: Property mocked as 'local_file.yaml'

    Yields:
    -------
        FakeROMSInputDataset: A mock dataset pointing to a local YAML file.
    """
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        mock_location_type.return_value = "path"
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "local_file.yaml"

        dataset = FakeROMSInputDataset(
            location="some/local/source/path/local_file.yaml",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def fake_romsinputdataset_yaml_remote() -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a remote YAML source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'url'
    - DataSource.source_type: Property mocked as 'yaml'
    - DataSource.basename: Property mocked as 'remote_file.yaml'

    Yields:
    -------
        FakeROMSInputDataset: A mock dataset pointing to a local YAML file.
    """
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        mock_location_type.return_value = "url"
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "remote_file.yaml"

        dataset = FakeROMSInputDataset(
            location="https://dodgyfakeyamlfiles.ru/all/remote_file.yaml",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def fake_model_grid() -> ROMSModelGrid:
    """Provides a ROMSModelGrid instance with fake attrs for testing"""
    return ROMSModelGrid(location="http://my.files/grid.nc", file_hash="123")


@pytest.fixture
def fake_initial_conditions() -> ROMSInitialConditions:
    """Provides a ROMSInitialConditions instance with fake attrs for testing"""
    return ROMSInitialConditions(location="http://my.files/initial.nc", file_hash="234")


@pytest.fixture
def fake_tidal_forcing() -> ROMSTidalForcing:
    """Provides a ROMSTidalForcing instance with fake attrs for testing"""
    return ROMSTidalForcing(location="http://my.files/tidal.nc", file_hash="345")


@pytest.fixture
def fake_river_forcing() -> ROMSRiverForcing:
    """Provides a ROMSRiverForcing instance with fake attrs for testing"""
    return ROMSRiverForcing(location="http://my.files/river.nc", file_hash="543")


@pytest.fixture
def fake_boundary_forcing() -> ROMSBoundaryForcing:
    """Provides a ROMSBoundaryForcing instance with fake attrs for testing"""
    return ROMSBoundaryForcing(location="http://my.files/boundary.nc", file_hash="456")


@pytest.fixture
def fake_surface_forcing() -> ROMSSurfaceForcing:
    """Provides a ROMSSurfaceForcing instance with fake attrs for testing"""
    return ROMSSurfaceForcing(location="http://my.files/surface.nc", file_hash="567")


@pytest.fixture
def fake_forcing_corrections() -> ROMSForcingCorrections:
    """Provides a ROMSForcingCorrections  instance with fake attrs for testing"""
    return ROMSForcingCorrections(
        location="http://my.files/sw_corr.nc", file_hash="890"
    )


################################################################################
# ROMSSimulation
################################################################################


@pytest.fixture
def fake_romssimulation(
    fake_marblexternalcodebase,
    fake_romsexternalcodebase,
    fake_roms_runtime_code,
    fake_roms_compile_time_code,
    fake_model_grid,
    fake_initial_conditions,
    fake_tidal_forcing,
    fake_river_forcing,
    fake_boundary_forcing,
    fake_surface_forcing,
    fake_forcing_corrections,
    tmp_path,
) -> ROMSSimulation:
    """Fixture providing a `ROMSSimulation` instance for testing.

    This fixture initializes a `ROMSSimulation` with a comprehensive configuration,
    including discretization settings, mock external ROMS and MARBL codebases.
    runtime and compile-time code, and multiple input datasets (grid, initial
    conditions, tidal forcing, boundary forcing, and surface forcing). The
    temporary directory (`tmp_path`) is used as the working directory.

    Yields
    ------
    tuple[ROMSSimulation, Path]
        A tuple containing:
        - `ROMSSimulation` instance with fully configured attributes.
        - The temporary directory where the simulation is stored.
    """
    directory = tmp_path
    sim = ROMSSimulation(
        name="ROMSTest",
        directory=directory,
        discretization=ROMSDiscretization(time_step=60, n_procs_x=2, n_procs_y=3),
        codebase=fake_romsexternalcodebase,
        runtime_code=fake_roms_runtime_code,
        compile_time_code=fake_roms_compile_time_code,
        start_date="2025-01-01",
        end_date="2025-12-31",
        valid_start_date="2024-01-01",
        valid_end_date="2026-01-01",
        marbl_codebase=fake_marblexternalcodebase,
        model_grid=fake_model_grid,
        initial_conditions=fake_initial_conditions,
        tidal_forcing=fake_tidal_forcing,
        river_forcing=fake_river_forcing,
        boundary_forcing=[
            fake_boundary_forcing,
        ],
        surface_forcing=[
            fake_surface_forcing,
        ],
        forcing_corrections=[
            fake_forcing_corrections,
        ],
    )

    return sim


@pytest.fixture
def fake_romssimulation_dict(fake_romssimulation) -> dict[str, Any]:
    """Fixture returning the dictionary associated with the `fake_romssimulation` fixture.

    Used for independently testing to/from_dict methods.
    """
    sim = fake_romssimulation
    return_dict = {
        "name": sim.name,
        "valid_start_date": sim.valid_start_date,
        "valid_end_date": sim.valid_end_date,
        "codebase": {
            "source_repo": sim.codebase.source_repo,
            "checkout_target": sim.codebase.checkout_target,
        },
        "discretization": {
            "time_step": sim.discretization.time_step,
            "n_procs_x": sim.discretization.n_procs_x,
            "n_procs_y": sim.discretization.n_procs_y,
        },
        "runtime_code": {
            "location": sim.runtime_code.source.location,
            "subdir": sim.runtime_code.subdir,
            "checkout_target": sim.runtime_code.checkout_target,
            "files": sim.runtime_code.files,
        },
        "compile_time_code": {
            "location": sim.compile_time_code.source.location,
            "subdir": sim.compile_time_code.subdir,
            "checkout_target": sim.compile_time_code.checkout_target,
            "files": sim.compile_time_code.files,
        },
        "marbl_codebase": {
            "source_repo": sim.marbl_codebase.source_repo,
            "checkout_target": sim.marbl_codebase.checkout_target,
        },
        "model_grid": {
            "location": sim.model_grid.source.location,
            "file_hash": sim.model_grid.source.file_hash,
        },
        "initial_conditions": {
            "location": sim.initial_conditions.source.location,
            "file_hash": sim.initial_conditions.source.file_hash,
        },
        "tidal_forcing": {
            "location": sim.tidal_forcing.source.location,
            "file_hash": sim.tidal_forcing.source.file_hash,
        },
        "river_forcing": {
            "location": sim.river_forcing.source.location,
            "file_hash": sim.river_forcing.source.file_hash,
        },
        "surface_forcing": [
            {
                "location": sim.surface_forcing[0].source.location,
                "file_hash": sim.surface_forcing[0].source.file_hash,
            }
        ],
        "boundary_forcing": [
            {
                "location": sim.boundary_forcing[0].source.location,
                "file_hash": sim.boundary_forcing[0].source.file_hash,
            },
        ],
        "forcing_corrections": [
            {
                "location": sim.forcing_corrections[0].source.location,
                "file_hash": sim.forcing_corrections[0].source.file_hash,
            }
        ],
    }
    return return_dict


@pytest.fixture
def fake_romssimulation_dict_no_forcing_lists(
    fake_romssimulation_dict,
) -> dict[str, Any]:
    """As fake_romssimulation_dict, but without list values for certain forcing types."""
    sim_dict = fake_romssimulation_dict
    for k in ["surface_forcing", "boundary_forcing", "forcing_corrections"]:
        sim_dict[k] = sim_dict[k][0]
    return sim_dict
