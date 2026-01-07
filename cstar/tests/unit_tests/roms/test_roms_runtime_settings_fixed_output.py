import textwrap

import pytest

from cstar.base.utils import DEFAULT_OUTPUT_ROOT_NAME
from cstar.roms.runtime_settings import ROMSRuntimeSettings


@pytest.mark.parametrize(
    "output_root_name_input",
    [
        DEFAULT_OUTPUT_ROOT_NAME,
        "another_name",
        "a_third_name",
    ],
)
def test_fixed_output_root_name(tmp_path, output_root_name_input):
    """Verify that ROMSRuntimeSettings always has a fixed output_root_name.

    This confirms that the pydantic model overrides the value input from the *.in file.
    """
    roms_in_content = textwrap.dedent(f"""\
        title: \n\ttest
            time_stepping:\n\t1 1 1 1
        bottom_drag:\n\t 1 1 1
        initial: confirm-the-labels-are-ignored\n\t 0
            forcing: confirm-whitespace-preceding-key-is-ignored\n\aaa/x.nc
        output_root_name:\n\t{output_root_name_input}
        """)
    roms_in_file = tmp_path / "roms.in"
    roms_in_file.write_text(roms_in_content)

    settings = ROMSRuntimeSettings.from_file(roms_in_file)

    assert str(settings.output_root_name) == DEFAULT_OUTPUT_ROOT_NAME
