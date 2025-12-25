from pathlib import Path

import pytest

from cstar.roms.runtime_settings import ROMSRuntimeSettings


@pytest.mark.parametrize(
    "output_root_name_input",
    [
        "ROMS_test",
        "another_name",
        "a_third_name",
    ],
)
def test_fixed_output_root_name(tmp_path, output_root_name_input):
    """Verify that ROMSRuntimeSettings always has a fixed output_root_name."""
    roms_in_content = f"""
title: test
time_stepping: 1 1 1 1
bottom_drag: 1 1 1
initial: 0
forcing:
output_root_name: {output_root_name_input}
"""
    roms_in_file = tmp_path / "roms.in"
    roms_in_file.write_text(roms_in_content)

    settings = ROMSRuntimeSettings.from_file(roms_in_file)

    assert str(settings.output_root_name) == "output"
