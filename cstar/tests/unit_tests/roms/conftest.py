from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest import mock

import numpy as np
import pytest

from cstar.base import AdditionalCode
from cstar.io.constants import SourceClassification
from cstar.io.source_data import SourceData
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
def romsexternalcodebase(
    mocksourcedata_remote_repo,
) -> Generator[ROMSExternalCodeBase, None, None]:
    """Fixture providing a `ROMSExternalCodeBase` instance for testing.

    Patches `SourceData` calls to avoid network and filesystem interaction.
    """
    location = "https://github.com/roms/repo.git"
    identifier = "roms_branch"
    source_data = mocksourcedata_remote_repo(location=location, identifier=identifier)
    patch_source_data = mock.patch(
        "cstar.base.external_codebase.SourceData", return_value=source_data
    )
    with patch_source_data:
        yield ROMSExternalCodeBase(source_repo=location, checkout_target=identifier)


@pytest.fixture
def romsexternalcodebase_staged(
    romsexternalcodebase,
    stagedrepository,
    roms_path,
) -> Generator[ROMSExternalCodeBase, None, None]:
    """Fixture providing a staged `ROMSExternalCodeBase` instance for testing.

    Sets `working_copy` to a mock StagedRepository instance.
    """
    recb = romsexternalcodebase
    staged_data = stagedrepository(
        path=roms_path, source=recb.source, changed_from_source=False
    )
    recb._working_copy = staged_data
    yield recb


################################################################################
# ROMSRuntimeSettings
################################################################################
@pytest.fixture
def romsruntimesettings() -> ROMSRuntimeSettings:
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
def roms_runtime_code(additionalcode_local) -> AdditionalCode:
    """Provides an example of ROMSSimulation.runtime_code with fake values for testing"""
    rc = additionalcode_local(
        location="/some/local/dir",
        subdir="subdir/",
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
def roms_compile_time_code(additionalcode_local) -> AdditionalCode:
    """Provides an example of ROMSSimulation.compile_time_code with fake values for testing"""
    cc = additionalcode_local(
        location="/some/local/dir",
        subdir="subdir/",
        files=["file1.h", "file2.opt"],
    )
    return cc


################################################################################
# ROMSInputDataset
################################################################################
@pytest.fixture
def romsinputdataset_local_netcdf(
    mocksourcedata_local_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local NetCDF source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a local NetCDF file.
    """
    fake_location = "some/local/source/path/local_file.nc"
    source_data = mocksourcedata_local_file(location=fake_location)
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_remote_netcdf(
    mocksourcedata_remote_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local NetCDF source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a local NetCDF file.
    """
    fake_location = "http://example.com/local_file.nc"
    source_data = mocksourcedata_remote_file(location=fake_location)
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_remote_partitioned_source(
    mocksourcedata_remote_file,
    mock_sourcedatacollection,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a remote, partitioned NetCDF source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to remote, partitioned NetCDF data.
    """
    fake_location = "http://example.com//local_file_00.nc"
    fake_np_xi = 5
    fake_np_eta = 2

    nparts = fake_np_xi * fake_np_eta
    source_data = mocksourcedata_remote_file(
        location=fake_location, identifier="unusedhash"
    )
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    parted_locations = [
        fake_location.replace("00", str(i).zfill(2)) for i in range(nparts)
    ]
    unused_identifiers = [f"unusedhash{i}" for i in range(nparts)]
    source_data_collection = mock_sourcedatacollection(
        locations=parted_locations,
        identifiers=unused_identifiers,
        classification=SourceClassification.REMOTE_BINARY_FILE,
    )
    patch_source_data_collection = mock.patch(
        "cstar.roms.input_dataset.SourceDataCollection.from_locations",
        return_value=source_data_collection,
    )

    with patch_source_data, patch_source_data_collection:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            source_np_xi=5,
            source_np_eta=2,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_local_yaml(
    mocksourcedata_local_text_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a local YAML source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a local YAML file.
    """
    fake_location = "some/local/source/path/local_file.yaml"
    source_data = mocksourcedata_local_text_file(location=fake_location)
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def romsinputdataset_remote_yaml(
    mocksourcedata_remote_text_file,
) -> Generator[ROMSInputDataset, None, None]:
    """Fixture to provide a ROMSInputDataset with a remote YAML source.

    Yields:
    -------
        FakeROMSInputDataset: A dataset instance pointing to a remote YAML file.
    """
    fake_location = "https://dodgyfakeyamlfiles.ru/all/remote_file.yaml"
    source_data = mocksourcedata_remote_text_file(
        location=fake_location,
    )
    patch_source_data = mock.patch(
        "cstar.roms.input_dataset.SourceData", return_value=source_data
    )
    with patch_source_data:
        dataset = FakeROMSInputDataset(
            location=fake_location,
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def roms_model_grid(
    mocksourcedata_remote_file,
) -> Callable[[str, str, SourceData], ROMSModelGrid]:
    """Provides a ROMSModelGrid instance with fake attrs for testing."""
    default_location = "http://my.files/grid.nc"
    default_hash = "123"
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSModelGrid(
                location=location,
                file_hash=file_hash,
            )

    return _create


@pytest.fixture
def roms_initial_conditions(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSInitialConditions
]:
    """Provides a ROMSInitialConditions instance with fake attrs for testing."""
    default_location = "http://my.files/initial.nc"
    default_hash = "234"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSInitialConditions(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
            )

    return _create


@pytest.fixture
def roms_tidal_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSTidalForcing
]:
    """Provides a ROMSTidalForcing instance with fake attrs for testing."""
    default_location = "http://my.files/tidal.nc"
    default_hash = "345"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSTidalForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_river_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSRiverForcing
]:
    """Provides a ROMSRiverForcing instance with fake attrs for testing"""
    default_location = "http://my.files/river.nc"
    default_hash = "543"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSRiverForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_boundary_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSBoundaryForcing
]:
    """Provides a ROMSBoundaryForcing instance with fake attrs for testing"""
    default_location = "http://my.files/boundary.nc"
    default_hash = "456"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSBoundaryForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_surface_forcing(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSSurfaceForcing
]:
    """Provides a ROMSSurfaceForcing instance with fake attrs for testing."""
    default_location = "http://my.files/surface.nc"
    default_hash = "567"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSSurfaceForcing(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


@pytest.fixture
def roms_forcing_corrections(
    mocksourcedata_remote_file,
) -> Callable[
    [str, str, SourceData, datetime | None, datetime | None], ROMSForcingCorrections
]:
    """Provides a ROMSForcingCorrections instance with fake attrs for testing"""
    default_location = "http://my.files/sw_corr.nc"
    default_hash = "890"
    default_start_date = None
    default_end_date = None
    default_sourcedata = mocksourcedata_remote_file(
        location=default_location,
        identifier=default_hash,
    )

    def _create(
        location=default_location,
        file_hash=default_hash,
        sourcedata=default_sourcedata,
        start_date=default_start_date,
        end_date=default_end_date,
    ):
        patch_source_data = mock.patch(
            "cstar.roms.input_dataset.SourceData", return_value=sourcedata
        )
        with patch_source_data:
            return ROMSForcingCorrections(
                location=location,
                file_hash=file_hash,
                start_date=start_date,
                end_date=end_date,
            )

    return _create


################################################################################
# ROMSSimulation
################################################################################


@pytest.fixture
def stub_romssimulation(
    marblexternalcodebase,
    romsexternalcodebase,
    roms_runtime_code,
    roms_compile_time_code,
    roms_model_grid,
    roms_initial_conditions,
    roms_tidal_forcing,
    roms_river_forcing,
    roms_boundary_forcing,
    roms_surface_forcing,
    roms_forcing_corrections,
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
        codebase=romsexternalcodebase,
        runtime_code=roms_runtime_code,
        compile_time_code=roms_compile_time_code,
        start_date="2025-01-01",
        end_date="2025-12-31",
        valid_start_date="2024-01-01",
        valid_end_date="2026-01-01",
        marbl_codebase=marblexternalcodebase,
        model_grid=roms_model_grid(),
        initial_conditions=roms_initial_conditions(),
        tidal_forcing=roms_tidal_forcing(),
        river_forcing=roms_river_forcing(),
        boundary_forcing=[
            roms_boundary_forcing(),
        ],
        surface_forcing=[
            roms_surface_forcing(),
        ],
        forcing_corrections=[
            roms_forcing_corrections(),
        ],
    )

    return sim


@pytest.fixture
def stub_romssimulation_dict(stub_romssimulation) -> dict[str, Any]:
    """Fixture returning the dictionary associated with the `stub_romssimulation` fixture.

    Used for independently testing to/from_dict methods.
    """
    sim = stub_romssimulation
    return_dict = {
        "name": sim.name,
        "valid_start_date": sim.valid_start_date,
        "valid_end_date": sim.valid_end_date,
        "codebase": {
            "source_repo": sim.codebase.source.location,
            "checkout_target": sim.codebase.source.checkout_target,
        },
        "discretization": {
            "time_step": sim.discretization.time_step,
            "n_procs_x": sim.discretization.n_procs_x,
            "n_procs_y": sim.discretization.n_procs_y,
        },
        "runtime_code": sim.runtime_code._constructor_args,
        "compile_time_code": sim.compile_time_code._constructor_args,
        "marbl_codebase": {
            "source_repo": sim.marbl_codebase.source.location,
            "checkout_target": sim.marbl_codebase.source.checkout_target,
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
        "boundary_forcing": [
            {
                "location": sim.boundary_forcing[0].source.location,
                "file_hash": sim.boundary_forcing[0].source.file_hash,
            },
        ],
        "surface_forcing": [
            {
                "location": sim.surface_forcing[0].source.location,
                "file_hash": sim.surface_forcing[0].source.file_hash,
            }
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
def stub_romssimulation_dict_no_forcing_lists(
    stub_romssimulation_dict,
) -> dict[str, Any]:
    """As stub_romssimulation_dict, but without list values for certain forcing types."""
    sim_dict = stub_romssimulation_dict
    for k in ["surface_forcing", "boundary_forcing", "forcing_corrections"]:
        sim_dict[k] = sim_dict[k][0]
    return sim_dict


@pytest.fixture
def patch_romssimulation_init_sourcedata(
    stub_romssimulation,
    mocksourcedata_remote_repo,
    mocksourcedata_remote_file,
    mock_sourcedatacollection,
) -> Callable[[], AbstractContextManager[None]]:
    """Fixture returning a contextmanager patching all ROMSSimulation.__init__ SourceData calls.

    Used in tests that create a new ROMSSimulation instance.
    """
    sim = stub_romssimulation

    # External codebase SourceData mocks
    mock_externalcodebase_sourcedata = mocksourcedata_remote_repo(
        location=sim.codebase.source.location,
        identifier=sim.codebase.source.identifier,
    )
    mock_marbl_externalcodebase_sourcedata = mocksourcedata_remote_repo(
        location=sim.marbl_codebase.source.location,
        identifier=sim.marbl_codebase.source.identifier,
    )

    # ROMS input dataset SourceData mocks
    mock_model_grid_sourcedata = mocksourcedata_remote_file(
        location=sim.model_grid.source.location,
        identifier=sim.model_grid.source.identifier,
    )
    mock_initial_conditions_sourcedata = mocksourcedata_remote_file(
        location=sim.initial_conditions.source.location,
        identifier=sim.initial_conditions.source.identifier,
    )
    mock_tidal_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.tidal_forcing.source.location,
        identifier=sim.tidal_forcing.source.identifier,
    )
    mock_river_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.river_forcing.source.location,
        identifier=sim.river_forcing.source.identifier,
    )
    mock_boundary_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.boundary_forcing[0].source.location,
        identifier=sim.boundary_forcing[0].source.identifier,
    )
    mock_surface_forcing_sourcedata = mocksourcedata_remote_file(
        location=sim.surface_forcing[0].source.location,
        identifier=sim.surface_forcing[0].source.identifier,
    )
    mock_forcing_corrections_sourcedata = mocksourcedata_remote_file(
        location=sim.forcing_corrections[0].source.location,
        identifier=sim.forcing_corrections[0].source.identifier,
    )

    # AdditionalCode SourceData mocks
    mock_runtime_code_sourcedata = mock_sourcedatacollection(
        locations=sim.runtime_code.source.locations,
        identifiers=["fake_hash" for i in sim.runtime_code.source],
        classification=SourceClassification.LOCAL_TEXT_FILE,
    )
    mock_runtime_code_classify_side_effect = [
        SourceClassification.LOCAL_DIRECTORY,
    ]

    mock_compile_time_code_sourcedata = mock_sourcedatacollection(
        locations=sim.compile_time_code.source.locations,
        identifiers=["fake_hash" for i in sim.compile_time_code.source],
        classification=SourceClassification.LOCAL_TEXT_FILE,
    )
    mock_compile_time_code_classify_side_effect = [
        SourceClassification.LOCAL_DIRECTORY,
    ]

    @contextmanager
    def _context():
        with (
            mock.patch(
                "cstar.base.external_codebase.SourceData",
                side_effect=[
                    mock_externalcodebase_sourcedata,
                    mock_marbl_externalcodebase_sourcedata,
                ],
            ),
            mock.patch(
                "cstar.roms.simulation.ROMSExternalCodeBase.is_configured",
                new_callable=mock.PropertyMock,
                return_value=False,
            ),
            mock.patch(
                "cstar.roms.input_dataset.SourceData",
                side_effect=[
                    mock_model_grid_sourcedata,
                    mock_initial_conditions_sourcedata,
                    mock_tidal_forcing_sourcedata,
                    mock_river_forcing_sourcedata,
                    mock_boundary_forcing_sourcedata,
                    mock_surface_forcing_sourcedata,
                    mock_forcing_corrections_sourcedata,
                ],
            ),
            mock.patch(
                "cstar.base.additional_code.SourceDataCollection.from_locations",
                side_effect=[
                    mock_runtime_code_sourcedata,
                    mock_compile_time_code_sourcedata,
                ],
            ),
            mock.patch(
                "cstar.io.source_data._SourceInspector.classify",
                side_effect=[
                    *mock_runtime_code_classify_side_effect,
                    *mock_compile_time_code_classify_side_effect,
                ],
            ),
        ):
            yield

    return _context
