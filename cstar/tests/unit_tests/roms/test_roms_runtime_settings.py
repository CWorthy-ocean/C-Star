import shutil
from contextlib import nullcontext
from pathlib import Path
from typing import TypeVar
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

import cstar.roms.runtime_settings as rrs
from cstar.base.utils import _replace_text_in_file
from cstar.roms import ROMSRuntimeSettings
from cstar.roms.runtime_settings import (
    Forcing,
    InitialConditions,
    ROMSRuntimeSettingsSection,
    SingleEntryROMSRuntimeSettingsSection,
)
from cstar.tests.unit_tests.fake_abc_subclasses import (
    FakeROMSRuntimeSettingsSection,
    FakeROMSRuntimeSettingsSectionEmpty,
)

T = TypeVar("T")


class TestHelperMethods:
    """Test module-level non-public methods for formatting various types for use in
    serializers.
    """

    @pytest.mark.parametrize(
        "fl,st",
        [(0, "0."), (1e-1, "0.1"), (1e-4, "1.000000E-04"), (1e5, "1.000000E+05")],
    )
    def test_format_float(self, fl, st):
        assert rrs._format_float(fl) == st

    @pytest.mark.parametrize(
        "field,result",
        [
            ("MARBLBiogeochemistry", "MARBL_biogeochemistry"),
            ("SCoord", "S-coord"),
            ("MYBakMixing", "MY_bak_mixing"),
            ("InitialConditions", "initial"),
            ("BottomDrag", "bottom_drag"),
            ("Forcing", "forcing"),
        ],
    )
    def test_get_alias(self, field, result):
        assert rrs._get_alias(field) == result


class TestROMSRuntimeSettingsSection:
    """Test class for the ROMSRuntimeSettingsSection class and its methods.

    Tests
    -----
    test_init_with_args
       Test ROMSRuntimeSettingsSection.__init__ with *args instead of **kwargs
    test_validate_from_lines_calls_from_lines
       Test ROMSRuntimeSettingsSection.validate_from_lines calls `from_lines`
       if given a list of strings
    test_validate_from_lines_falls_back_to_handler
       Test ROMSRuntimeSettingsSection.validate_from_lines falls back to
       handler if not given a list of strings
    test_validate_from_lines_uses_handler_on_exception
       Test ROMSRuntimeSettingsSection.validate_from_lines falls back to
       handler if from_lines fails
    test_default_serializer
       Test the `ROMSRuntimeSettingsSection.default_serializer` returns
       an expected string from a range of attributes
    test_from_lines_returns_none_if_no_lines
       Test the `ROMSRuntimeSettingsSection.from_lines` method returns None
       if given an empty list
    test_from_lines_on_float_section_with_D_formatting
       Test a ROMSRuntimeSettingsSection subclass with a single float entry
       is correctly returned by `from_lines` with fortran-style formatting
    test_from_lines_multiple_fields
       Test a ROMSRuntimeSettingsSection with more than one entry is correctly
       returned by `from_lines`
    test_from_lines_list_field_at_end
       Test that ROMSRuntimeSettingsSection.from_lines correctly handles lines
       where the final entry is a list (using all remaining values to populate it)
    test_from_lines_list_field_only
       Test that ROMSRuntimeSettingsSection.from_lines correctly returns
       a valid instance when the only entry is a list
    test_from_lines_multiline_flag
       Test that ROMSRuntimeSettingsSection.from_lines correctly handles the case
       where entries are on different lines.
    """

    def test_init_with_args(self) -> None:
        """Test ROMSRuntimeSettingsSection.__init__ with *args instead of **kwargs."""

        class TestSection(ROMSRuntimeSettingsSection):
            val1: float
            val2: str

        section = TestSection(5.0, "hello")
        assert section.val1 == 5
        assert section.val2 == "hello"

    def test_format_list_of_floats(self):
        lf = FakeROMSRuntimeSettingsSectionEmpty()._format_and_join_values(
            [1e-3, 0.0, 2]
        )
        assert lf == "1.000000E-03 0. 2"

    def test_format_path(self):
        p = FakeROMSRuntimeSettingsSectionEmpty()._format_and_join_values(
            Path("mypath.nc")
        )
        assert p == "mypath.nc"

    def test_format_list_of_paths(self):
        lp = FakeROMSRuntimeSettingsSectionEmpty()._format_and_join_values(
            [Path("path1.nc"), Path("path2.nc")]
        )
        assert lp == "path1.nc path2.nc"

    @pytest.mark.parametrize("o,st", [(5, "5"), ("helloworld", "helloworld")])
    def test_format_other(self, o, st):
        assert FakeROMSRuntimeSettingsSectionEmpty()._format_and_join_values(o) == st

    def test_format_list_of_other(self):
        o = FakeROMSRuntimeSettingsSectionEmpty()._format_and_join_values([5, "foo"])
        assert o == "5 foo"

    def test_validate_from_lines_calls_from_lines(self):
        """Test ROMSRuntimeSettingsSection.validate_from_lines calls `from_lines` if
        given a list of strings.
        """
        mock_handler = MagicMock()
        test_lines = ["1", "2.0"]

        with patch.object(
            FakeROMSRuntimeSettingsSection, "from_lines", return_value="parsed"
        ) as mock_from_lines:
            result = FakeROMSRuntimeSettingsSection.validate_from_lines(
                test_lines, mock_handler
            )

        mock_from_lines.assert_called_once_with(test_lines)
        mock_handler.assert_not_called()
        assert result == "parsed"

    def test_validate_from_lines_falls_back_to_handler(self):
        """Test ROMSRuntimeSettingsSection.validate_from_lines falls back to handler if
        not given a list of strings.
        """
        mock_handler = MagicMock(return_value="fallback")
        test_data = {"a": 1, "b": 2.0}

        with patch.object(
            FakeROMSRuntimeSettingsSection, "from_lines"
        ) as mock_from_lines:
            result = FakeROMSRuntimeSettingsSection.validate_from_lines(
                test_data, mock_handler
            )

        mock_from_lines.assert_not_called()
        mock_handler.assert_called_once_with(test_data)
        assert result == "fallback"

    def test_validate_from_lines_raises_on_exception(self):
        """Test ROMSRuntimeSettingsSection.validate_from_lines falls back to handler if
        from_lines fails.
        """
        mock_handler = MagicMock(return_value="handled")
        test_lines = ["bad", "data"]

        with pytest.raises(ValueError):
            FakeROMSRuntimeSettingsSection.validate_from_lines(test_lines, mock_handler)

    def test_default_serializer(self):
        """Test the `ROMSRuntimeSettingsSection.default_serializer` returns an expected
        string from a range of attributes.
        """
        obj = FakeROMSRuntimeSettingsSection(
            floats=[0.0, 1e-5, 123.4],
            paths=[Path("a.nc"), Path("b.nc")],
            others=["x", 42],
            floatval=1e5,
            pathval=Path("grid.nc"),
            otherval="done",
        )

        expected = "fake_roms_runtime_settings_section: floats paths others floatval pathval otherval\n    0. 1.000000E-05 123.4    a.nc b.nc    x 42    1.000000E+05    grid.nc    done\n\n"

        assert obj.model_dump() == expected

    def test_key_order_property(self):
        obj = FakeROMSRuntimeSettingsSection(
            floats=[0.0, 1e-5, 123.4],
            paths=[Path("a.nc"), Path("b.nc")],
            others=["x", 42],
            floatval=1e5,
            pathval=Path("grid.nc"),
            otherval="done",
        )
        assert obj.key_order == [
            "floats",
            "paths",
            "others",
            "floatval",
            "pathval",
            "otherval",
        ]

    def test_from_lines_raises_if_no_lines(self) -> None:
        """Test the `ROMSRuntimeSettingsSection.from_lines` method returns None if given
        an empty list.
        """
        with pytest.raises(ValueError):
            FakeROMSRuntimeSettingsSection.from_lines([])

    def test_from_lines_on_float_section_with_D_formatting(self) -> None:
        """Test a ROMSRuntimeSettingsSection subclass with a single float entry is
        correctly returned by `from_lines` with fortran-style formatting.
        """

        class FloatSection(ROMSRuntimeSettingsSection):
            val: float

        section = FloatSection.from_lines(["5.0D0"])
        assert section.val == 5.0

    def test_from_lines_multiple_fields(self) -> None:
        """Test a ROMSRuntimeSettingsSection with more than one entry is correctly
        returned by `from_lines`
        """

        class MixedSection(ROMSRuntimeSettingsSection):
            count: int
            name: str

        section = MixedSection.from_lines(["7 example_name"])
        assert section.count == 7
        assert section.name == "example_name"

    def test_from_lines_list_field_at_end(self) -> None:
        """Test that ROMSRuntimeSettingsSection.from_lines correctly handles lines where
        the final entry is a list (using all remaining values to populate it)
        """

        class ListAtEnd(ROMSRuntimeSettingsSection):
            prefix: str
            items: list[int]

        section = ListAtEnd.from_lines(["prefix 1 2 3"])
        assert section.prefix == "prefix"
        assert section.items == [1, 2, 3]

    def test_from_lines_list_field_only(self) -> None:
        """Test that ROMSRuntimeSettingsSection.from_lines correctly returns a valid
        instance when the only entry is a list.
        """

        class OnlyList(ROMSRuntimeSettingsSection):
            entries: list[str]

        section = OnlyList.from_lines(["a b c"])
        assert section.entries == ["a", "b", "c"]

    def test_from_lines_multiline_flag(self) -> None:
        """Test that ROMSRuntimeSettingsSection.from_lines correctly handles the case
        where entries are on different lines.
        """

        class MultiLinePaths(ROMSRuntimeSettingsSection):
            multi_line = True
            paths: list[Path]

        section = MultiLinePaths.from_lines(["a.nc", "b.nc", "c.nc"])
        assert section.paths == [Path("a.nc"), Path("b.nc"), Path("c.nc")]

    def test_from_lines_on_initial_conditions_with_nrrec_0(self) -> None:
        """Test the bespoke InitialConditions.from_lines() method handles the situation
        where nrrec is 0 and ininame is empty.
        """
        lines = [
            "0",
        ]
        ic = InitialConditions.from_lines(lines)

        assert ic.nrrec == 0
        assert ic.ininame is None
        assert ic.model_dump() == "initial: nrrec ininame\n    0\n\n"

    def test_from_lines_on_forcing_with_filenames_empty(self) -> None:
        """Test the bespoke Forcing.from_lines() method handles the situation where
        filenames is empty.
        """
        lines: list[str] = []
        fr = Forcing.from_lines(lines)
        assert fr.filenames is None
        assert fr.model_dump() == "forcing: filenames\n    \n\n"


class TestSingleEntryROMSRuntimeSettingsSection:
    """Test class for the SingleEntryROMSRuntimeSettingsSection class and its methods.

    Tests
    -----
    test_init_subclass_sets_attrs
       Tests that SingleEntryROMSRuntimeSettingsSection.__init_subclass__ correctly
       sets the section_name and key_order attributes
    test_single_entry_validator_returns_cls_when_type_matches
       Tests that the `single_entry_validator` method passes a correctly typed value to cls()
       without requiring a dict or kwargs for initialization
    test_single_entry_validator_calls_handler_when_type_does_not_match
       Tests that `single_entry_validator` falls back to the handler if the value supplied to
       __init__ is not of the expected type
    test_str_and_repr_return_value
       Tests that the `str` and `repr` functions for SingleEntryROMSRuntimeSettingsSection
       simply return a string of the single entry's value
    test_single_entry_section_raises_if_multiple_fields
       Tests that attempting to initialize a SingleEntryROMSRuntimeSettingsSection
       with multiple entries raises a TypeError
    """

    class MockSingleEntrySection(SingleEntryROMSRuntimeSettingsSection):
        value: float

    def test_single_entry_validator_returns_cls_when_type_matches(self) -> None:
        """Tests that the `single_entry_validator` method passes a correctly typed value
        to cls() without requiring a dict or kwargs for initialization.
        """
        result = self.MockSingleEntrySection(3.14)

        assert isinstance(
            result, TestSingleEntryROMSRuntimeSettingsSection.MockSingleEntrySection
        )
        assert result.value == 3.14

    def test_single_entry_validator_calls_handler_when_type_does_not_match(
        self,
    ) -> None:
        """Tests that `single_entry_validator` falls back to the handler if the value
        supplied to __init__ is not of the expected type.
        """
        handler = MagicMock(return_value="fallback")
        result = self.MockSingleEntrySection.single_entry_validator(
            "not a float", handler
        )

        handler.assert_called_once_with("not a float")
        assert result == "fallback"

    @pytest.mark.parametrize(
        "obj,annotation,context",
        [
            ("abc", str, nullcontext()),
            (123, str, pytest.raises(ValidationError)),
            ("abc", list, pytest.raises(ValidationError)),
            (["abc"], list, nullcontext()),
            (["abc"], list[str], nullcontext()),
            (["abc"], list[int], pytest.raises(ValidationError)),
            ({"a", "b"}, set[str], nullcontext()),
            ({"a": 1, "b": 2}, dict[str, int], nullcontext()),  # type: ignore
            ({"a": 1, "b": 2}, dict[str, float], nullcontext()),  # type: ignore
            ({"a": 1, "b": 2}, dict[str, str], pytest.raises(ValidationError)),  # type: ignore
            (Path.cwd(), Path, nullcontext()),
        ],
    )
    def test_strict_validation_for_single_entries(
        self, obj, annotation, context
    ) -> None:
        class MyTestClass(SingleEntryROMSRuntimeSettingsSection):
            value: annotation

        with context:
            assert MyTestClass(obj).value == obj

    def test_str_and_repr_return_value(self) -> None:
        """Tests that the `str` and `repr` functions for
        SingleEntryROMSRuntimeSettingsSection simply return a string of the single
        entry's value.
        """
        section = self.MockSingleEntrySection(1.0)
        assert str(section) == "1.0"
        assert repr(section) == "1.0"

    def test_single_entry_section_raises_if_multiple_fields(self) -> None:
        """Tests that attempting to initialize a SingleEntryROMSRuntimeSettingsSection
        with multiple entries raises a TypeError.
        """
        with pytest.raises(TypeError):

            class InvalidSection(SingleEntryROMSRuntimeSettingsSection):
                a: int
                b: float

            InvalidSection(a=1, b=2)


class TestROMSRuntimeSettings:
    """Test class for the ROMSRuntimeSettings and its methods.

    Tests
    -----
    test_load_raw_sections_parses_multiple_sections
       Tests that the load_raw_sections method correctly parses a multi-section
       file to a multi-key dictionary
    test_load_raw_sections_skips_blank_and_comment_lines
       Tests that the load_raw_sections method skips lines beginning with "!" or
       with no value
    test_load_raw_sections_raises_on_missing_file
       Tests that the load_raw_sections method raises a FileNotFoundError if
       the supplied file does not exist
    test_to_file
       Tests against a reference file the writing of fake_romsruntimesettings
    test_to_file_skips_missing_section
       Tests to_file does not attempt to write a section that is None
       in the ROMSRuntimeSettings instance
    test_from_file
       Tests the reading of a reference file against fake_romsruntimesettings
    test_from_file_with_missing_optional_sections
       Tests that `from_file` sets as None any ROMSRuntimeSettings attributes
       corresponding to missing optional sections
    test_from_file_raises_if_missing_section
       Tests that `from_file` raises a ValueError if any missing non-optional
       sections
    test_file_roundtrip
       Tests that the fake_romsruntimesettings instance written to_file is
       functionally identical with the one subsequently read back with from_file
    """

    def test_load_raw_sections_parses_multiple_sections(self, tmp_path: Path) -> None:
        file = tmp_path / "test.in"
        file.write_text(
            """
            first line is garbage
            title: title
                This is a test run

            time_stepping: ntimes dt ndtfast ninfo
                360 60 60 1

            ! this is a comment
            bottom_drag: rdrg rdrg2 zob
                0.0 1.0E-3 1.0E-2
            """
        )

        result = ROMSRuntimeSettings._load_raw_sections(file)

        assert result == {
            "title": ["This is a test run"],
            "time_stepping": ["360 60 60 1"],
            "bottom_drag": ["0.0 1.0E-3 1.0E-2"],
        }

    def test_load_raw_sections_skips_blank_and_comment_lines(
        self, tmp_path: Path
    ) -> None:
        file = tmp_path / "test.in"
        file.write_text(
            """
            title: title
                This is a test

            ! ignored comment
                ! another one
        """
        )

        result = ROMSRuntimeSettings._load_raw_sections(file)
        assert result == {"title": ["This is a test"]}

    def test_load_raw_sections_raises_on_missing_file(self) -> None:
        with pytest.raises(FileNotFoundError, match="does not exist"):
            ROMSRuntimeSettings._load_raw_sections(Path("not_a_file.in"))

    def test_to_file(
        self, fake_romsruntimesettings: ROMSRuntimeSettings, tmp_path: Path
    ) -> None:
        """Test the ROMSRuntimeSettings.to_file method.

        This test writes the example ROMSRuntimeSettings instance
        defined by the fake_romsruntimesettings fixture to a temporary
        file and compares each non-commented line in the example `.in`
        file `fixtures/fake_romsruntimesettings.in` with those in the
        temporary file.

        Mocks and Fixtures
        ------------------
        fake_romsruntimesettings: ROMSRuntimeSettings
           Fixture returning an example ROMSRuntimeSettings instance
        tmp_path: Path
           Fixture creating and returning a temporary pathlib.Path

        Asserts
        -------
        - The lines in the written file match those in the reference file
        """
        fake_romsruntimesettings.to_file(
            Path(__file__).parent / "fixtures/fake_romsruntimesettings.in"
        )
        fake_romsruntimesettings.to_file(tmp_path / "test.in")

        with (
            open(tmp_path / "test.in") as out_f,
            open(
                Path(__file__).parent / "fixtures/fake_romsruntimesettings.in"
            ) as ref_f,
        ):
            ref = [
                line for line in ref_f.readlines() if not line.strip().startswith("!")
            ]
            out = out_f.readlines()
            assert ref == out, f"Expected \n{ref}\n,got\n{out}"

    def test_to_file_skips_missing_section(self, fake_romsruntimesettings, tmp_path):
        fake_romsruntimesettings.climatology = None

        fake_romsruntimesettings.to_file(tmp_path / "tmp.in")
        with open(tmp_path / "tmp.in") as f:
            lns = f.readlines()
        assert all(["climatology" not in s for s in lns])

    def test_from_file(self, fake_romsruntimesettings: ROMSRuntimeSettings) -> None:
        """Test the ROMSRuntimeSettings.from_file method.

        This test compares the ROMSRuntimeSettings instance created from
        the reference file `fixtures/fake_romsruntimesettings.in` with the
        example instance returned by the `fake_romsruntimesettings` fixture.

        Mocks and Fixtures
        ------------------
        fake_romsruntimesettings: ROMSRuntimeSettings
           Fixture returning an example ROMSRuntimeSettings instance

        Asserts
        -------
        - Compares each attribute of the reference and tested ROMSRuntimeSettings
          instances and checks for equality.
        """
        tested_settings = ROMSRuntimeSettings.from_file(
            Path(__file__).parent / "fixtures/fake_romsruntimesettings.in"
        )
        expected_settings = fake_romsruntimesettings

        assert tested_settings.title == expected_settings.title
        assert tested_settings.time_stepping == expected_settings.time_stepping
        assert tested_settings.bottom_drag == expected_settings.bottom_drag
        assert tested_settings.initial == expected_settings.initial
        assert tested_settings.forcing == expected_settings.forcing
        assert tested_settings.output_root_name == expected_settings.output_root_name
        assert tested_settings.s_coord == expected_settings.s_coord
        assert tested_settings.rho0 == expected_settings.rho0
        assert tested_settings.lin_rho_eos == expected_settings.lin_rho_eos
        assert (
            tested_settings.marbl_biogeochemistry
            == expected_settings.marbl_biogeochemistry
        )
        assert tested_settings.lateral_visc == expected_settings.lateral_visc
        assert tested_settings.gamma2 == expected_settings.gamma2
        assert tested_settings.my_bak_mixing == expected_settings.my_bak_mixing
        assert tested_settings.sss_correction == expected_settings.sss_correction
        assert tested_settings.sst_correction == expected_settings.sst_correction
        assert tested_settings.ubind == expected_settings.ubind
        assert tested_settings.v_sponge == expected_settings.v_sponge
        assert tested_settings.grid == expected_settings.grid
        assert tested_settings.climatology == expected_settings.climatology
        assert tested_settings.tracer_diff2 == expected_settings.tracer_diff2
        assert tested_settings.vertical_mixing == expected_settings.vertical_mixing

    def test_from_file_with_missing_optional_sections(self, tmp_path: Path) -> None:
        """Confirms that ROMSRuntimeSettings.from_file sets the attributes corresponding
        to settings that are not present in the file to None.

        This test copies the reference file in `fixtures/fake_romsruntimesettings.in`
        to a temporary path and modifies it to remove the value of the `climatology`
        entry, then confirms that `ROMSRuntimeSettings.from_file(tmp_file).climatology
        is None

        Mocks and Fixtures
        ------------------
        tmp_path: Path
           Fixture creating and returning a temporary pathlib.Path

        Asserts
        -------
        - The ROMSRuntimeSettings instance returned by `from_file` with the modified
          file has `None` for its `climatology` attribute
        """
        modified_file = tmp_path / "modified_example_settings.in"
        shutil.copy2(
            Path(__file__).parent / "fixtures/fake_romsruntimesettings.in",
            modified_file,
        )
        _replace_text_in_file(modified_file, "climfile2.nc", "")
        tested_settings = ROMSRuntimeSettings.from_file(modified_file)
        assert tested_settings.climatology is None

    def test_from_file_raises_if_missing_section(self, tmp_path: Path) -> None:
        modified_file = tmp_path / "modified_example_settings.in"
        shutil.copy2(
            Path(__file__).parent / "fixtures/fake_romsruntimesettings.in",
            modified_file,
        )
        _replace_text_in_file(modified_file, "title: title", "")
        _replace_text_in_file(modified_file, "Example runtime settings", "")
        with pytest.raises(ValueError, match="Required field missing from file."):
            ROMSRuntimeSettings.from_file(modified_file)

    def test_file_roundtrip(
        self, fake_romsruntimesettings: ROMSRuntimeSettings, tmp_path: Path
    ) -> None:
        """Tests that the `to_file`/`from_file` roundtrip results in a functionally
        indentical ROMSRuntimeSettings instance.

        Mocks and Fixtures
        ------------------
        - fake_romsruntimesettings: ROMSRuntimeSettings
           A fixture returning an example ROMSRuntimeSettings instance
        - tmp_path: Path
           Fixture creating and returning a temporary pathlib.Path

        Asserts
        -------
        - Each attribute in the instance returned by `from_file` is equal
          to those in the instance passed to `to_file`
        """
        expected_settings = fake_romsruntimesettings
        expected_settings.to_file(tmp_path / "test.in")

        tested_settings = ROMSRuntimeSettings.from_file(tmp_path / "test.in")

        assert tested_settings.title == expected_settings.title
        assert tested_settings.time_stepping == expected_settings.time_stepping
        assert tested_settings.bottom_drag == expected_settings.bottom_drag
        assert tested_settings.initial == expected_settings.initial
        assert tested_settings.forcing == expected_settings.forcing
        assert tested_settings.output_root_name == expected_settings.output_root_name
        assert tested_settings.s_coord == expected_settings.s_coord
        assert tested_settings.rho0 == expected_settings.rho0
        assert tested_settings.lin_rho_eos == expected_settings.lin_rho_eos
        assert (
            tested_settings.marbl_biogeochemistry
            == expected_settings.marbl_biogeochemistry
        )
        assert tested_settings.lateral_visc == expected_settings.lateral_visc
        assert tested_settings.gamma2 == expected_settings.gamma2
        assert tested_settings.my_bak_mixing == expected_settings.my_bak_mixing
        assert tested_settings.sss_correction == expected_settings.sss_correction
        assert tested_settings.sst_correction == expected_settings.sst_correction
        assert tested_settings.ubind == expected_settings.ubind
        assert tested_settings.v_sponge == expected_settings.v_sponge
        assert tested_settings.grid == expected_settings.grid
        assert tested_settings.climatology == expected_settings.climatology
        assert tested_settings.tracer_diff2 == expected_settings.tracer_diff2
        assert tested_settings.vertical_mixing == expected_settings.vertical_mixing


class TestStrAndRepr:
    """Test that the __str__ and __repr__ functions of the ROMSRuntimeSettings class
    behave as expected.
    """

    def test_str(self, fake_romsruntimesettings: ROMSRuntimeSettings) -> None:
        """Test that the __str__ function of ROMSRuntimeSettings matches an expected
        string for the example instance.

        Mocks and Fixtures
        ------------------
        fake_romsruntimesettings: ROMSRuntimeSettings
           A fixture returning an example ROMSRuntimeSettings instance

        Asserts
        -------
        - str(fake_romsruntimesettings) matches an expected reference string
        """
        expected_str = """ROMSRuntimeSettings
-------------------
Title (`ROMSRuntimeSettings.title`): Example runtime settings
Output filename prefix (`ROMSRuntimeSettings.output_root_name`): ROMS_test
Time stepping (`ROMSRuntimeSettings.time_stepping`):
- Number of steps (`ntimes`) = 360,
- Time step (`dt`, sec) = 60,
- Mode-splitting ratio (`ndtfast`) = 60,
- Runtime diagnostic frequency (`ninfo`, steps) = 1
Bottom drag (`ROMSRuntimeSettings.bottom_drag`):
- Linear bottom drag coefficient (`rdrg`, m/s) = 0.0,
- Quadratic bottom drag coefficient (`rdrg2`, nondim) = 0.001
- Bottom roughness height (`zob`,m) = 0.01
Grid file (`ROMSRuntimeSettings.grid`): input_datasets/roms_grd.nc
Initial conditions file (`ROMSRuntimeSettings.initial`): input_datasets/roms_ini.nc
Forcing file(s): [PosixPath('input_datasets/roms_frc.nc'),
          PosixPath('input_datasets/roms_frc_bgc.nc'),
          PosixPath('input_datasets/roms_bry.nc'),
          PosixPath('input_datasets/roms_bry_bgc.nc')]
S-coordinate parameters (`ROMSRuntimeSettings.s_coord`):
- Surface stretching parameter (`theta_s`) = 5.0,
- Bottom stretching parameter (`theta_b`) = 2.0,
- Critical depth (`hc` or `tcline`, m) = 300.0
Boussinesq reference density (`rho0`, kg/m3) = 1000.0
Linear equation of state parameters (`ROMSRuntimeSettings.lin_rho_eos`):
- Thermal expansion coefficient, ⍺ (`Tcoef`, kg/m3/K) = 0.2,
- Reference temperature (`T0`, °C) = 1.0,
- Haline contraction coefficient, β (`Scoef`, kg/m3/PSU) = 0.822,
- Reference salinity (`S0`, psu) = 1.0
MARBL input (`ROMSRuntimeSettings.marbl_biogeochemistry`):
- MARBL runtime settings file: marbl_in,
- MARBL output tracer list: marbl_tracer_list_fname,
- MARBL output diagnostics list: marbl_diagnostic_output_list
Horizontal Laplacian kinematic viscosity (`ROMSRuntimeSettings.lateral_visc`, m2/s) = 0.0
Boundary slipperiness parameter (`ROMSRuntimeSettings.gamma2`, free-slip=+1, no-slip=-1) = 1.0
Horizontal Laplacian mixing coefficients for tracers (`ROMSRuntimeSettings.tracer_diff2`, m2/s) = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
Vertical mixing parameters (`ROMSRuntimeSettings.vertical_mixing`):
- Background vertical viscosity (`Akv_bak`, m2/s) = 0.0,
- Background vertical mixing for tracers (`Akt_bak`, m2/s) = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
Mellor-Yamada Level 2.5 turbulent closure parameters (`ROMSRuntimeSettings.my_bak_mixing`):
- Background vertical TKE mixing [`Akq_bak`, m2/s] = 1e-05,
- Horizontal Laplacian TKE mixing [`q2nu2`, m2/s] = 0.0,
- Horizontal biharmonic TKE mixing [`q2nu4`, m4/s] = 0.0,
SSS correction (`ROMSRuntimeSettings.sss_correction`): 7.777
SST correction (`ROMSRuntimeSettings.sst_correction`): 10.0
Open boundary binding velocity (`ROMSRuntimeSettings.ubind`, m/s) = 0.1
Maximum sponge layer viscosity (`ROMSRuntimeSettings.v_sponge`, m2/s) = 0.0
Climatology data files (`ROMSRuntimeSettings.climatology`): climfile2.nc"""

        assert str(fake_romsruntimesettings) == expected_str, (
            f"expected \n{expected_str}\n, got\n{fake_romsruntimesettings!s}"
        )

    def test_repr(self, fake_romsruntimesettings: ROMSRuntimeSettings) -> None:
        """Test that the __repr__ function of ROMSRuntimeSettings matches an expected
        string for the example instance.

        Mocks and Fixtures
        ------------------
        fake_romsruntimesettings: ROMSRuntimeSettings
           A fixture returning an example ROMSRuntimeSettings instance

        Asserts
        -------
        - repr(fake_romsruntimesettings) matches an expected reference string
        """
        expected_repr = """ROMSRuntimeSettings(title='Example runtime settings', time_stepping={'ntimes': 360, 'dt': 60, 'ndtfast': 60, 'ninfo': 1}, bottom_drag={'rdrg': 0.0, 'rdrg2': 0.001, 'zob': 0.01}, initial={'nrrec': 1, 'ininame': PosixPath('input_datasets/roms_ini.nc')}, forcing=["('filenames', [PosixPath('input_datasets/roms_frc.nc'), PosixPath('input_datasets/roms_frc_bgc.nc'), PosixPath('input_datasets/roms_bry.nc'), PosixPath('input_datasets/roms_bry_bgc.nc')])"], output_root_name='ROMS_test', grid='input_datasets/roms_grd.nc', climatology='climfile2.nc', s_coord={'theta_s': 5.0, 'theta_b': 2.0, 'tcline': 300.0}, rho0=1000.0, lin_rho_eos={'Tcoef': 0.2, 'T0': 1.0, 'Scoef': 0.822, 'S0': 1.0}, marbl_biogeochemistry={'marbl_namelist_fname': PosixPath('marbl_in'), 'marbl_tracer_list_fname': PosixPath('marbl_tracer_list_fname'), 'marbl_diag_list_fname': PosixPath('marbl_diagnostic_output_list')}, lateral_visc=0.0, gamma2=1.0, tracer_diff2=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], vertical_mixing={'Akv_bak': 0.0, 'Akt_bak': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}, my_bak_mixing={'Akq_bak': 1e-05, 'q2nu2': 0.0, 'q2nu4': 0.0}, sss_correction=7.777, sst_correction=10.0, ubind=0.1, v_sponge=0.0)"""

        assert expected_repr == repr(fake_romsruntimesettings), (
            f"expected \n{expected_repr}\n, got\n{fake_romsruntimesettings!r}"
        )
