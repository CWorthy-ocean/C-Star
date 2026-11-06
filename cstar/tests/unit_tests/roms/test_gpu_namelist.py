"""Tests for the gpu-ucla-roms namelist tier selection in `cstar.roms.namelist`.

Every gpu-ucla-roms release up to v0.1.2-beta reads the ucla-roms 0.11.0 namelist
vocabulary (``NAMELIST_COMPAT``), so C-Star writes `RomsNamelistV0_11_0` for it
and never the 0.12/0.13 tiers, whose extra keys the GPU model rejects.
"""

import warnings
from unittest import mock

import pytest

from cstar.roms.namelist import (
    GPU_NAMELIST_SCHEMA_REGISTRY,
    NEWEST_KNOWN_UCLA_ROMS_GPU,
    RomsNamelistV0_11_0,
    RomsNamelistV0_12_0,
    _parse_gpu_release,
    gpu_namelist_schema_for_ref,
    namelist_schema_named,
)

NOT_A_RELEASE = "is not a release tag"
NEWER_THAN_KNOWN = "newer than the newest gpu-ucla-roms release this C-Star knows"
HASH = "a" * 40


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        ("v0.1.2-beta", (0, 1, 2)),
        ("v0.1.0-beta", (0, 1, 0)),
        ("0.1.1-beta", (0, 1, 1)),
        ("0.1.2", (0, 1, 2)),
        ("v0.1.2", (0, 1, 2)),
        ("v0.2.0-rc.1", (0, 2, 0)),
        ("v1.10.3-alpha", (1, 10, 3)),
    ],
)
def test_parse_gpu_release_accepts_v_prefix_and_pre_release_suffix(ref, expected):
    assert _parse_gpu_release(ref) == expected


@pytest.mark.parametrize(
    "ref",
    [
        "main",
        HASH,
        "abc123def456",
        "1.2",
        "0.1.2.3",
        "v0.1.2-",
        "vv0.1.2",
        "0.1.2_beta",
        "",
    ],
)
def test_parse_gpu_release_rejects_non_release_refs(ref):
    assert _parse_gpu_release(ref) is None


def test_registry_is_one_open_entry_mapping_to_the_0_11_0_tier():
    assert GPU_NAMELIST_SCHEMA_REGISTRY == ((None, None, RomsNamelistV0_11_0),)
    assert NEWEST_KNOWN_UCLA_ROMS_GPU == (0, 1, 2)


def test_the_gpu_tier_is_findable_by_name_for_a_build_record():
    assert namelist_schema_named("RomsNamelistV0_11_0") is RomsNamelistV0_11_0


@pytest.mark.parametrize(
    "ref",
    [
        "v0.1.0-beta",
        "v0.1.1-beta",
        "v0.1.2-beta",
        "0.1.2-beta",
        "0.1.2",
        "v0.1.2",
        "0.1.0",
    ],
)
def test_release_tags_select_the_0_11_0_tier_without_warning(ref, recwarn):
    assert gpu_namelist_schema_for_ref(ref) is RomsNamelistV0_11_0
    assert len(recwarn) == 0


@pytest.mark.parametrize("ref", [None, "main", "gpu-branch", "abc123def456", HASH])
def test_a_ref_that_is_not_a_release_warns_and_uses_the_last_entry(ref):
    with pytest.warns(UserWarning, match=NOT_A_RELEASE) as record:
        schema = gpu_namelist_schema_for_ref(ref)
    assert schema is RomsNamelistV0_11_0
    message = str(record[0].message)
    assert "gpu-ucla-roms" in message
    assert "RomsNamelistV0_11_0" in message
    assert "v0.1.2-beta" in message  # what to pin instead
    assert record[0].filename == __file__


def test_a_hash_without_a_clone_warns_and_never_asks_git():
    with (
        mock.patch("cstar.roms.namelist._describe_nearest_tag") as describe,
        pytest.warns(UserWarning, match=NOT_A_RELEASE),
    ):
        assert gpu_namelist_schema_for_ref(HASH) is RomsNamelistV0_11_0
    describe.assert_not_called()


@pytest.mark.parametrize("ref", ["v0.1.3-beta", "0.2.0", "v1.0.0", "v0.13.0", "0.12.0"])
def test_a_release_newer_than_known_warns_and_uses_the_last_entry(ref):
    """A ucla-roms-looking tag (``v0.13.0``) is just a newer gpu-ucla-roms one."""
    with pytest.warns(UserWarning, match=NEWER_THAN_KNOWN) as record:
        schema = gpu_namelist_schema_for_ref(ref)
    assert schema is RomsNamelistV0_11_0
    message = str(record[0].message)
    assert "RomsNamelistV0_11_0" in message
    assert "0.1.2" in message  # what to pin at or below
    assert record[0].filename == __file__


def test_a_commit_hash_is_resolved_to_its_nearest_release_tag(recwarn):
    with mock.patch(
        "cstar.roms.namelist._describe_nearest_tag", return_value=("v0.1.1-beta", 0)
    ) as describe:
        schema = gpu_namelist_schema_for_ref(HASH, repo_path="/some/repo")
    assert schema is RomsNamelistV0_11_0
    describe.assert_called_once_with("/some/repo", HASH)
    assert len(recwarn) == 0


def test_a_commit_ahead_of_its_release_warns():
    with (
        mock.patch(
            "cstar.roms.namelist._describe_nearest_tag",
            return_value=("v0.1.2-beta", 3),
        ),
        pytest.warns(
            UserWarning,
            match=r"gpu-ucla-roms commit .* is 3 commit\(s\) after release v0.1.2-beta",
        ) as record,
    ):
        schema = gpu_namelist_schema_for_ref(HASH, repo_path="/some/repo")
    assert schema is RomsNamelistV0_11_0
    assert record[0].filename == __file__


def test_a_commit_on_a_newer_release_warns_about_it():
    with (
        mock.patch(
            "cstar.roms.namelist._describe_nearest_tag", return_value=("v0.2.0-beta", 0)
        ),
        pytest.warns(UserWarning, match=NEWER_THAN_KNOWN) as record,
    ):
        schema = gpu_namelist_schema_for_ref(HASH, repo_path="/some/repo")
    assert schema is RomsNamelistV0_11_0
    assert "release v0.2.0-beta" in str(record[0].message)


@pytest.mark.parametrize("described", [None, ("not-semver", 0)])
def test_an_unresolvable_hash_warns_and_uses_the_last_entry(described):
    with (
        mock.patch("cstar.roms.namelist._describe_nearest_tag", return_value=described),
        pytest.warns(UserWarning, match=NOT_A_RELEASE),
    ):
        schema = gpu_namelist_schema_for_ref(HASH, repo_path="/some/repo")
    assert schema is RomsNamelistV0_11_0


def test_a_branch_is_never_resolved_through_the_clone():
    with (
        mock.patch("cstar.roms.namelist._describe_nearest_tag") as describe,
        pytest.warns(UserWarning, match=NOT_A_RELEASE),
    ):
        schema = gpu_namelist_schema_for_ref("main", repo_path="/some/repo")
    assert schema is RomsNamelistV0_11_0
    describe.assert_not_called()


@pytest.mark.parametrize(
    "ref",
    [
        None,
        "main",
        HASH,
        "v0.1.0-beta",
        "v0.1.2-beta",
        "0.12.0",
        "v0.13.0",
        "v0.14.0",
        "v9.9.9",
    ],
)
def test_the_gpu_never_gets_a_newer_namelist_tier(ref):
    """0.12.0 and 0.13.0 add keys the GPU model stops on (``NAMELIST_COMPAT``)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        schema = gpu_namelist_schema_for_ref(ref)
    assert schema is RomsNamelistV0_11_0
    assert not issubclass(schema, RomsNamelistV0_12_0)
