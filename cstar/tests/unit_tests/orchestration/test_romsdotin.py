import textwrap
import uuid
from pathlib import Path

import pytest

from cstar.orchestration.roms_dot_in import load_raw_runtime_settings


@pytest.fixture
def roms_dot_in_sample(tmp_path: Path) -> Path:
    content = textwrap.dedent("""\
        title:
                gom

        time_stepping: NTIMES   dt[sec]  NDTFAST  NINFO
                        3360      900       60      2

        S-coord: THETA_S,   THETA_B,    TCLINE (m)
                5.0d0      2.0d0      600.d0

        grid:  filename
            ./input_files/partitioned_files/grid_12km.nc

        forcing: filename
            ./input_files/partitioned_files/surf_frc_phys_200001.nc
            ./input_files/partitioned_files/surf_frc_phys_200002.nc
            ./input_files/partitioned_files/surf_frc_bgc_clim.nc
            ./input_files/partitioned_files/bnd_frc_phys_200001.nc
            ./input_files/partitioned_files/bnd_frc_phys_200002.nc
            ./input_files/partitioned_files/bnd_frc_bgc_clim.nc
            ./input_files/partitioned_files/tides_Jan1_2000.nc
            ./input_files/partitioned_files/river_force.nc

        initial: NRREC  filename
                2
                ./input_files/partitioned_files/init_condis_bgc.nc

        MARBL_biogeochemistry: namelist  tracer_output_list   diagnostic_output_list
            marbl_in
            marbl_tracer_output_list
            marbl_diagnostic_output_list

        output_root_name:
                atlas_base

        rho0:
            1027.4d0

        lateral_visc:   VISC2,
                        0.

        vertical_mixing:  Akv  Akt1  Akt2
                        1.0E-4  0.0   0.0 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0.

        tracer_diff2: TNU2(1:NT)           [m^2/sec for all]
                    0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0.

        bottom_drag:     RDRG [m/s],  RDRG2,  Zob [m],  Cdb_min, Cdb_max
                        0.0E-04      0.d0     2.0E-2     1.E-4    1.E-2

        gamma2:
                        1.d0

        v_sponge:         V_SPONGE [m^2/sec]
                        500.

        ubind:  binding velocity [m/s]
                    0.2

        SSS_correction:   dSSSdt [cm/day]
                        7.777
        """)
    target = tmp_path / "test.in"
    target.write_text(content)
    return target


def test_load_raw_settings_basic(tmp_path: Path) -> None:
    """Verify the key / header / value format is parsed with a simple example."""
    unique_name = str(uuid.uuid4())
    mock_rdi_path = tmp_path / "chris.in"
    mock_rdi_path.write_text(f"output_root_name:\n{unique_name}\n")

    settings = load_raw_runtime_settings(mock_rdi_path)

    assert "output_root_name" in settings

    actual_header, actual_value = settings["output_root_name"]
    assert actual_header == ""
    assert actual_value == unique_name


def test_load_raw_settings(roms_dot_in_sample: Path) -> None:
    """Verify a full roms.in file can be processsed."""
    settings = load_raw_runtime_settings(roms_dot_in_sample)

    assert "output_root_name" in settings

    actual_header, actual_value = settings["output_root_name"]
    assert actual_header == ""
    assert actual_value == "atlas_base"
