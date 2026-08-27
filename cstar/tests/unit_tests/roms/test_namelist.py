"""Tests for the versioned ROMS namelist schemas in `cstar.roms.namelist`."""

import subprocess
from pathlib import Path
from unittest import mock

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


@pytest.fixture
def tagged_ucla_roms_clone(tmp_path: Path) -> dict[str, str | Path]:
    """A tiny local git repo mimicking ucla-roms release-tag history.

    History: commit tagged ``0.4.2`` -> commit tagged only ``exp-marker`` (a
    non-release tag, which release resolution must ignore) -> commit tagged
    ``0.5.0``. Returns the repo path and the three commit hashes.
    """
    repo = tmp_path / "ucla-roms"
    repo.mkdir()

    def _git(*args: str) -> str:
        result = subprocess.run(
            ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    _git("init", "-q")
    (repo / "f").write_text("1")
    _git("add", "f")
    _git("commit", "-q", "-m", "release 0.4.2")
    hash_0_4_2 = _git("rev-parse", "HEAD")
    _git("tag", "0.4.2")

    (repo / "f").write_text("2")
    _git("commit", "-aq", "-m", "post-0.4.2 work")
    hash_after_0_4_2 = _git("rev-parse", "HEAD")
    _git("tag", "exp-marker")

    (repo / "f").write_text("3")
    _git("commit", "-aq", "-m", "release 0.5.0")
    hash_0_5_0 = _git("rev-parse", "HEAD")
    _git("tag", "0.5.0")

    return {
        "repo": repo,
        "hash_0_4_2": hash_0_4_2,
        "hash_after_0_4_2": hash_after_0_4_2,
        "hash_0_5_0": hash_0_5_0,
    }


def test_namelist_schema_for_ref_resolves_hash_at_release_tag(
    tagged_ucla_roms_clone,
    recwarn,
):
    """A commit hash sitting exactly on a release tag selects that release's
    schema, without any warning.
    """
    info = tagged_ucla_roms_clone
    assert (
        namelist_schema_for_ref(info["hash_0_4_2"], repo_path=info["repo"])
        is RomsNamelist
    )
    assert (
        namelist_schema_for_ref(info["hash_0_5_0"], repo_path=info["repo"])
        is RomsNamelistV0_5_0
    )
    assert len(recwarn) == 0


def test_namelist_schema_for_ref_resolves_hash_ahead_of_release_tag(
    tagged_ucla_roms_clone,
):
    """A commit hash between releases selects the nearest ancestor release's
    schema and warns that the commit may contain unreleased changes.

    The commit carries a non-release tag (``exp-marker``) that resolution
    must skip over in favor of the release tag ``0.4.2``.
    """
    info = tagged_ucla_roms_clone
    with pytest.warns(UserWarning, match="commit\\(s\\) after release 0.4.2"):
        schema = namelist_schema_for_ref(
            info["hash_after_0_4_2"], repo_path=info["repo"]
        )
    assert schema is RomsNamelist


def test_namelist_schema_for_ref_branch_ignores_repo_path(tagged_ucla_roms_clone):
    """A branch name is never resolved via the local clone: it falls back to
    the latest schema with the use-at-your-own-risk warning even when a repo
    path is available.
    """
    info = tagged_ucla_roms_clone
    with pytest.warns(UserWarning, match="use at your own risk"):
        schema = namelist_schema_for_ref("main", repo_path=info["repo"])
    assert schema is RomsNamelistV0_5_0


def test_namelist_schema_for_ref_unresolvable_hash_falls_back():
    """A hash that resolves to a non-release tag (or fails to resolve) falls
    back to the latest schema with the use-at-your-own-risk warning.
    """
    with mock.patch(
        "cstar.roms.namelist._describe_nearest_tag", return_value=("not-semver", 0)
    ):
        with pytest.warns(UserWarning, match="use at your own risk"):
            schema = namelist_schema_for_ref("a" * 40, repo_path="/some/repo")
    assert schema is RomsNamelistV0_5_0

    with mock.patch("cstar.roms.namelist._describe_nearest_tag", return_value=None):
        with pytest.warns(UserWarning, match="use at your own risk"):
            schema = namelist_schema_for_ref("a" * 40, repo_path="/some/repo")
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


class TestUnknownOverrideKeys:
    """Tests for `RomsNamelistBase.unknown_override_keys`."""

    def test_valid_names_return_no_violations(self):
        """Known group and key names produce no violations."""
        overrides = {
            "time_stepping": {"dt": 60.0, "ntimes": 10},
            "param_settings": {"np_xi": 2},
        }
        assert RomsNamelistV0_5_0.unknown_override_keys(overrides) == []

    def test_unknown_group_and_key_all_reported(self):
        """Unknown groups and unknown keys are each reported, in one pass."""
        overrides = {
            "not_a_group": {"dt": 1},
            "time_stepping": {"not_a_key": 1, "dt": 60.0},
        }
        violations = RomsNamelistV0_5_0.unknown_override_keys(overrides)
        assert len(violations) == 2
        assert any("not_a_group" in v for v in violations)
        assert any("not_a_key" in v for v in violations)

    def test_version_specific_key(self):
        """`nrpf_rst` exists pre-0.5.0 and was removed in 0.5.0."""
        overrides = {"basic_output_settings": {"nrpf_rst": 1}}
        assert RomsNamelist.unknown_override_keys(overrides) == []
        violations = RomsNamelistV0_5_0.unknown_override_keys(overrides)
        assert len(violations) == 1
        assert "nrpf_rst" in violations[0]

    def test_base_class_rejected(self):
        """The base class is not a usable schema for key checks."""
        with pytest.raises(TypeError, match="not a usable schema"):
            RomsNamelistBase.unknown_override_keys({"time_stepping": {"dt": 1}})
