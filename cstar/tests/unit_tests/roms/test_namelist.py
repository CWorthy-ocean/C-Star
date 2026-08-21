"""Tests for the versioned ROMS namelist schemas in `cstar.roms.namelist`."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from cstar.roms.namelist import (
    RomsNamelist,
    RomsNamelistBase,
    RomsNamelistV0_5_0,
    namelist_schema_for_ref,
)

OLD_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist.nml"
NEW_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_5_0.nml"


@pytest.mark.parametrize(
    "checkout_target",
    ["0.4.9", "v0.4.9", "0.0.1"],
)
def test_namelist_schema_for_ref_pre_0_5_0(checkout_target):
    """Refs below the 0.5.0 breaking release select the unversioned `RomsNamelist`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelist


@pytest.mark.parametrize(
    "checkout_target",
    ["0.5.0", "v0.5.0", "0.7.3", "12.0.0"],
)
def test_namelist_schema_for_ref_post_0_5_0(checkout_target):
    """Refs at or above 0.5.0 select `RomsNamelistV0_5_0`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_5_0


@pytest.mark.parametrize(
    "checkout_target",
    [
        "main",
        "roms_branch",
        "abc123def456",
        "a" * 40,
        "1.2",
        "v1.2.3.4",
        None,
    ],
)
def test_namelist_schema_for_ref_fallback_warns(checkout_target):
    """Non-release-tag refs fall back to the latest schema and warn about it."""
    with pytest.warns(UserWarning, match="use at your own risk"):
        schema = namelist_schema_for_ref(checkout_target)
    assert schema is RomsNamelistV0_5_0


def test_roms_namelist_round_trip():
    """`RomsNamelist.read` parses the pre-0.5.0 fixture and keeps `nrpf_rst`."""
    nml = RomsNamelist.read(OLD_NAMELIST)
    d = nml.to_f90nml_dict()
    assert "nrpf_rst" in d["basic_output_settings"]


def test_roms_namelist_v0_5_0_round_trip():
    """`RomsNamelistV0_5_0.read` parses the 0.5.0 fixture with the renamed keys."""
    nml = RomsNamelistV0_5_0.read(NEW_NAMELIST)
    d = nml.to_f90nml_dict()
    assert "output_period_particles" in d["particles_settings"]
    assert "nrpf_particles" in d["particles_settings"]
    assert "nrpf_rst" not in d["basic_output_settings"]


def test_roms_namelist_v0_5_0_rejects_old_fixture():
    """`RomsNamelistV0_5_0` is strict: the pre-0.5.0 fixture has now-extra/missing keys."""
    with pytest.raises(ValidationError):
        RomsNamelistV0_5_0.read(OLD_NAMELIST)


def test_roms_namelist_rejects_new_fixture():
    """`RomsNamelist` is strict: the 0.5.0 fixture has now-extra/missing keys."""
    with pytest.raises(ValidationError):
        RomsNamelist.read(NEW_NAMELIST)


def test_roms_namelist_base_rejects_direct_use():
    """`RomsNamelistBase` refuses validation against itself.

    Its version-varying groups are typed as the loose common models, so using
    it directly would silently accept a namelist no ucla-roms version reads.
    """
    with pytest.raises(TypeError, match="not a usable schema"):
        RomsNamelistBase.read(OLD_NAMELIST)
