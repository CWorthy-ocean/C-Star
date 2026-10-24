"""Tests for the versioned ROMS namelist schemas in `cstar.roms.namelist`."""

import logging
from pathlib import Path
from unittest import mock

import pytest
import yaml
from pydantic import ValidationError

from cstar.roms.namelist import (
    NAMELIST_SCHEMA_REGISTRY,
    NEWEST_KNOWN_UCLA_ROMS,
    CdrLiteOutputSettings,
    CdrLiteSettings,
    CdrLiteSettingsV0_12_0,
    ExtractDataSettingsV0_12_0,
    MarblBiogeochemistrySettingsV0_11_0,
    ParamSettings,
    ParamSettingsV0_4_0,
    PioSettings,
    RomsNamelist,
    RomsNamelistBase,
    RomsNamelistV0_4_0,
    RomsNamelistV0_5_0,
    RomsNamelistV0_6_0,
    RomsNamelistV0_7_0,
    RomsNamelistV0_9_0,
    RomsNamelistV0_11_0,
    RomsNamelistV0_12_0,
    _parse_semver,
    format_roms_version,
    is_commit_hash,
    namelist_schema_for_ref,
    release_ref_for_remote,
)

OLD_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist.nml"
NEW_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_5_0.nml"
V0_6_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_6_0.nml"
V0_7_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_7_0.nml"
V0_9_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_9_0.nml"
V0_11_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_11_0.nml"
V0_12_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_12_0.nml"

PIO_SETTINGS_BLOCK = "&pio_settings\n    pio_stride = 1\n/\n\n"


@pytest.mark.parametrize(
    "checkout_target",
    ["0.0.1", "0.3.0", "v0.3.9"],
)
def test_namelist_schema_for_ref_pre_0_4_0(checkout_target):
    """Refs below the 0.4.0 breaking release select the unversioned `RomsNamelist`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelist


@pytest.mark.parametrize(
    "checkout_target",
    ["0.4.0", "v0.4.0", "0.4.9", "v0.4.9"],
)
def test_namelist_schema_for_ref_0_4_0_range(checkout_target):
    """Refs in `[0.4.0, 0.5.0)` select `RomsNamelistV0_4_0`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_4_0


@pytest.mark.parametrize(
    "checkout_target",
    ["0.5.0", "v0.5.0", "0.5.9"],
)
def test_namelist_schema_for_ref_0_5_0_range(checkout_target):
    """Refs in `[0.5.0, 0.6.0)` select `RomsNamelistV0_5_0`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_5_0


@pytest.mark.parametrize(
    "checkout_target",
    ["0.6.0", "v0.6.0", "0.6.1", "0.6.9"],
)
def test_namelist_schema_for_ref_post_0_6_0(checkout_target):
    """Refs in `[0.6.0, 0.7.0)` select `RomsNamelistV0_6_0`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_6_0


@pytest.mark.parametrize(
    "checkout_target",
    ["0.7.0", "v0.7.0", "0.7.3", "0.8.0", "0.8.5"],
)
def test_namelist_schema_for_ref_post_0_7_0(checkout_target):
    """Refs in `[0.7.0, 0.9.0)` select `RomsNamelistV0_7_0`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_7_0


@pytest.mark.parametrize(
    "checkout_target",
    ["0.9.0", "v0.9.0", "0.9.3", "0.10.0", "0.10.1"],
)
def test_namelist_schema_for_ref_post_0_9_0(checkout_target):
    """Refs in `[0.9.0, 0.11.0)` select `RomsNamelistV0_9_0`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_9_0


@pytest.mark.parametrize(
    "checkout_target",
    ["0.11.0", "v0.11.0", "0.11.4"],
)
def test_namelist_schema_for_ref_post_0_11_0(checkout_target):
    """Refs in `[0.11.0, 0.12.0)` select `RomsNamelistV0_11_0`."""
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_11_0


@pytest.mark.parametrize(
    "checkout_target",
    ["0.12.0", "v0.12.0"],
)
def test_namelist_schema_for_ref_post_0_12_0(checkout_target):
    """Refs at or above 0.12.0 select `RomsNamelistV0_12_0` (newer releases
    too, with a warning: see the newer-than-known tests below).
    """
    assert namelist_schema_for_ref(checkout_target) is RomsNamelistV0_12_0


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
    assert schema is RomsNamelistV0_12_0


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
        is RomsNamelistV0_4_0
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
    assert schema is RomsNamelistV0_4_0


def test_namelist_schema_for_ref_branch_ignores_repo_path(tagged_ucla_roms_clone):
    """A branch name is never resolved via the local clone: it falls back to
    the latest schema with the use-at-your-own-risk warning even when a repo
    path is available.
    """
    info = tagged_ucla_roms_clone
    with pytest.warns(UserWarning, match="use at your own risk"):
        schema = namelist_schema_for_ref("main", repo_path=info["repo"])
    assert schema is RomsNamelistV0_12_0


def test_namelist_schema_for_ref_unresolvable_hash_falls_back():
    """A hash that resolves to a non-release tag (or fails to resolve) falls
    back to the latest schema with the use-at-your-own-risk warning.
    """
    with mock.patch(
        "cstar.roms.namelist._describe_nearest_tag", return_value=("not-semver", 0)
    ):
        with pytest.warns(UserWarning, match="use at your own risk"):
            schema = namelist_schema_for_ref("a" * 40, repo_path="/some/repo")
    assert schema is RomsNamelistV0_12_0

    with mock.patch("cstar.roms.namelist._describe_nearest_tag", return_value=None):
        with pytest.warns(UserWarning, match="use at your own risk"):
            schema = namelist_schema_for_ref("a" * 40, repo_path="/some/repo")
    assert schema is RomsNamelistV0_12_0


NEWER_THAN_KNOWN = "newer than the newest release this C-Star knows"
BUNDLED_MODEL_SPECS = (
    Path(__file__).parents[3] / "catalog" / "bundled" / "ModelSpec"
).glob("*/model.yaml")


def _newer_than_known_records(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "cstar.roms.namelist" and NEWER_THAN_KNOWN in r.getMessage()
    ]


def test_newest_known_ucla_roms_not_behind_registry():
    """Adding a namelist tier bumps `NEWEST_KNOWN_UCLA_ROMS`: a release the
    registry has a tier for must never warn that C-Star does not know it.
    """
    last_lower = NAMELIST_SCHEMA_REGISTRY[-1][0]
    assert last_lower is not None
    assert last_lower <= NEWEST_KNOWN_UCLA_ROMS


@pytest.mark.parametrize(
    "spec", sorted(BUNDLED_MODEL_SPECS), ids=lambda p: p.parent.name
)
def test_bundled_model_specs_pin_known_releases(spec: Path):
    """Adopting a ucla-roms release as a bundled ModelSpec bumps
    `NEWEST_KNOWN_UCLA_ROMS`, so the shipped models never warn.
    """
    roms = yaml.safe_load(spec.read_text())["code"]["roms"]
    version = _parse_semver(str(roms.get("commit") or roms.get("branch")))
    if version is not None:
        assert version <= NEWEST_KNOWN_UCLA_ROMS


def test_format_roms_version():
    assert format_roms_version((0, 12, 0)) == "0.12.0"


@pytest.mark.parametrize(
    "version",
    [
        (*NEWEST_KNOWN_UCLA_ROMS[:2], NEWEST_KNOWN_UCLA_ROMS[2] + 1),
        (NEWEST_KNOWN_UCLA_ROMS[0], NEWEST_KNOWN_UCLA_ROMS[1] + 1, 0),
        (NEWEST_KNOWN_UCLA_ROMS[0] + 1, 0, 0),
    ],
    ids=["patch", "minor", "major"],
)
def test_namelist_schema_for_ref_newer_than_known_release_warns(
    version: tuple[int, int, int], caplog: pytest.LogCaptureFixture
):
    """A release tag above `NEWEST_KNOWN_UCLA_ROMS` selects the latest schema
    with one warning and one log line naming the tag, the schema and the newest
    known release.
    """
    tag = format_roms_version(version)
    newest = format_roms_version(NEWEST_KNOWN_UCLA_ROMS)
    last_cls = NAMELIST_SCHEMA_REGISTRY[-1][2]
    with (
        caplog.at_level(logging.WARNING, logger="cstar.roms.namelist"),
        pytest.warns(UserWarning) as record,
    ):
        schema = namelist_schema_for_ref(f"v{tag}")

    assert schema is last_cls
    assert len(record) == 1
    message = str(record[0].message)
    assert f"ucla-roms release v{tag} is {NEWER_THAN_KNOWN} ({newest})" in message
    assert last_cls.__name__ in message
    assert record[0].filename == __file__
    assert _newer_than_known_records(caplog) == [message]


def test_namelist_schema_for_ref_newer_than_known_logs_once_per_ref(
    caplog: pytest.LogCaptureFixture,
):
    """Logging ignores warning filters and the wizard reselects the schema on
    every rebuild, so each too-new ref is logged once; the warning repeats.
    """
    major = NEWEST_KNOWN_UCLA_ROMS[0] + 1
    with (
        caplog.at_level(logging.WARNING, logger="cstar.roms.namelist"),
        pytest.warns(UserWarning, match=NEWER_THAN_KNOWN) as record,
    ):
        namelist_schema_for_ref(f"{major}.0.0")
        namelist_schema_for_ref(f"{major}.0.0")
        namelist_schema_for_ref(f"{major}.0.1")
    assert len(record) == 3
    assert len(_newer_than_known_records(caplog)) == 2


@pytest.mark.parametrize(
    "checkout_target",
    [
        format_roms_version(NEWEST_KNOWN_UCLA_ROMS),
        f"v{format_roms_version(NEWEST_KNOWN_UCLA_ROMS)}",
        format_roms_version(NAMELIST_SCHEMA_REGISTRY[-1][0]),  # type: ignore[arg-type]
    ],
)
def test_namelist_schema_for_ref_known_release_in_last_range_is_silent(
    checkout_target: str, recwarn: pytest.WarningsRecorder, caplog
):
    """A release inside the last range, up to the newest known one, selects
    the latest schema without any warning or log line.
    """
    with caplog.at_level(logging.WARNING, logger="cstar.roms.namelist"):
        schema = namelist_schema_for_ref(checkout_target)
    assert schema is NAMELIST_SCHEMA_REGISTRY[-1][2]
    assert len(recwarn) == 0
    assert not _newer_than_known_records(caplog)


def test_namelist_schema_for_ref_hash_at_newer_than_known_release_warns(
    caplog: pytest.LogCaptureFixture,
):
    """A commit hash whose nearest release tag is newer than the newest known
    release gets the same warning, naming both the hash and the tag.
    """
    tag = format_roms_version((NEWEST_KNOWN_UCLA_ROMS[0] + 1, 0, 0))
    with (
        mock.patch("cstar.roms.namelist._describe_nearest_tag", return_value=(tag, 0)),
        caplog.at_level(logging.WARNING, logger="cstar.roms.namelist"),
        pytest.warns(UserWarning) as record,
    ):
        schema = namelist_schema_for_ref("a" * 40, repo_path="/some/repo")

    assert schema is NAMELIST_SCHEMA_REGISTRY[-1][2]
    assert len(record) == 1
    message = str(record[0].message)
    assert f"commit {'a' * 40!r} (release {tag}) is {NEWER_THAN_KNOWN}" in message
    assert _newer_than_known_records(caplog) == [message]


def test_namelist_schema_for_ref_hash_ahead_of_newer_than_known_release_warns_twice():
    """A commit after a too-new release warns about both: the commits since
    the tag, and the tag being newer than C-Star knows.
    """
    tag = format_roms_version((NEWEST_KNOWN_UCLA_ROMS[0] + 1, 0, 0))
    with (
        mock.patch("cstar.roms.namelist._describe_nearest_tag", return_value=(tag, 3)),
        pytest.warns(UserWarning) as record,
    ):
        namelist_schema_for_ref("a" * 40, repo_path="/some/repo")

    messages = [str(w.message) for w in record]
    assert len(messages) == 2
    assert f"3 commit(s) after release {tag}" in messages[0]
    assert NEWER_THAN_KNOWN in messages[1]


def test_namelist_schema_for_ref_hash_at_known_release_is_silent(recwarn):
    """A commit hash on the newest known release does not warn."""
    with mock.patch(
        "cstar.roms.namelist._describe_nearest_tag",
        return_value=(format_roms_version(NEWEST_KNOWN_UCLA_ROMS), 0),
    ):
        namelist_schema_for_ref("a" * 40, repo_path="/some/repo")
    assert len(recwarn) == 0


def test_release_ref_for_remote_newer_than_known_tag_warns_on_selection():
    """Without a clone, the hash resolves to its tag silently; selecting the
    schema for that tag then warns that the release is newer than known.
    """
    tag = format_roms_version((NEWEST_KNOWN_UCLA_ROMS[0] + 1, 0, 0))
    with mock.patch(
        "cstar.roms.namelist._describe_remote_nearest_tag", return_value=(tag, 0)
    ):
        ref = release_ref_for_remote("/some/repo", "a" * 40)
    assert ref == tag
    with pytest.warns(UserWarning, match=f"release {tag} is {NEWER_THAN_KNOWN}"):
        namelist_schema_for_ref(ref)


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        ("65fdf96f", True),
        ("7b5a046c81b783979c6d9963c3800476b00f3a6f", True),
        ("0.10.1", False),
        ("v0.10.1", False),
        ("main", False),
        ("abc123", False),  # shorter than an abbreviated hash
        ("", False),
        (None, False),
    ],
)
def test_is_commit_hash(ref, expected):
    assert is_commit_hash(ref) is expected


def test_release_ref_for_remote_resolves_hash_at_release_tag(
    tagged_ucla_roms_clone, recwarn
):
    """A commit hash sitting exactly on a release tag resolves to that tag
    without a warning.
    """
    info = tagged_ucla_roms_clone
    assert release_ref_for_remote(str(info["repo"]), info["hash_0_4_2"]) == "0.4.2"
    assert release_ref_for_remote(str(info["repo"]), info["hash_0_5_0"]) == "0.5.0"
    assert len(recwarn) == 0


def test_release_ref_for_remote_warns_when_hash_is_ahead_of_release(
    tagged_ucla_roms_clone,
):
    """A commit hash between releases resolves to the nearest ancestor release
    tag, skipping the non-release ``exp-marker`` tag it carries, and warns.
    """
    info = tagged_ucla_roms_clone
    with pytest.warns(UserWarning, match="commit\\(s\\) after release 0.4.2"):
        ref = release_ref_for_remote(str(info["repo"]), info["hash_after_0_4_2"])
    assert ref == "0.4.2"


@pytest.mark.parametrize("checkout_target", ["main", "0.5.0", "v0.5.0", ""])
def test_release_ref_for_remote_leaves_non_hash_refs_alone(checkout_target, recwarn):
    """A tag, a branch or an empty ref comes back unchanged with no clone."""
    with mock.patch("cstar.roms.namelist._describe_remote_nearest_tag") as describe:
        assert (
            release_ref_for_remote("https://example.com/ucla-roms", checkout_target)
            == checkout_target
        )
    describe.assert_not_called()
    assert len(recwarn) == 0


def test_release_ref_for_remote_unreachable_location_warns_and_keeps_hash():
    """A hash that cannot be resolved (the unit-test network guard refuses an
    https remote) is returned as given, with the latest-schema fallback warning.
    """
    with pytest.warns(
        UserWarning,
        match=rf"Could not resolve .*{'a' * 40}.* latest known namelist schema "
        rf"\({RomsNamelistV0_12_0.__name__}\)",
    ):
        ref = release_ref_for_remote(
            "https://github.com/CWorthy-ocean/ucla-roms", "a" * 40
        )
    assert ref == "a" * 40


def test_release_ref_for_remote_unparseable_tag_warns_and_keeps_hash():
    """A resolution that yields a tag C-Star cannot parse falls back too."""
    with mock.patch(
        "cstar.roms.namelist._describe_remote_nearest_tag",
        return_value=("1.2.3-rc1", 0),
    ):
        with pytest.warns(UserWarning, match="Could not resolve"):
            ref = release_ref_for_remote("/some/repo", "a" * 40)
    assert ref == "a" * 40


def test_release_ref_for_remote_clones_once_per_location_and_ref():
    """Repeat calls for one ``(location, ref)`` reuse the first answer, a failed
    resolution included, and warn once; another location is resolved afresh.
    """
    with mock.patch(
        "cstar.roms.namelist._describe_remote_nearest_tag", return_value=None
    ) as describe:
        with pytest.warns(UserWarning, match="Could not resolve") as record:
            release_ref_for_remote("/some/repo", "a" * 40)
            release_ref_for_remote("/some/repo", "a" * 40)
        assert len(record) == 1
        assert describe.call_count == 1

        with pytest.warns(UserWarning, match="Could not resolve"):
            release_ref_for_remote("/other/repo", "a" * 40)
        assert describe.call_count == 2


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


def test_roms_namelist_v0_6_0_round_trip():
    """`RomsNamelistV0_6_0.read` parses the 0.6.0 fixture, keeping the 0.5.0
    renamed keys and adding `pio_settings`.
    """
    nml = RomsNamelistV0_6_0.read(V0_6_0_NAMELIST)
    d = nml.to_f90nml_dict()
    assert "output_period_particles" in d["particles_settings"]
    assert "nrpf_particles" in d["particles_settings"]
    assert "nrpf_rst" not in d["basic_output_settings"]
    assert d["pio_settings"] == {"pio_stride": 1}


def test_roms_namelist_v0_5_0_rejects_old_fixture():
    """`RomsNamelistV0_5_0` is strict: the pre-0.5.0 fixture has now-extra/missing keys."""
    with pytest.raises(ValidationError):
        RomsNamelistV0_5_0.read(OLD_NAMELIST)


def test_roms_namelist_rejects_new_fixture():
    """`RomsNamelist` is strict: the 0.5.0 fixture has now-extra/missing keys."""
    with pytest.raises(ValidationError):
        RomsNamelist.read(NEW_NAMELIST)


def test_roms_namelist_v0_5_0_rejects_pio_settings_fixture():
    """`RomsNamelistV0_5_0` is strict: `&pio_settings` was added in 0.6.0, so a
    file carrying it (otherwise identical to the 0.5.0 fixture) is rejected as
    an unknown group.
    """
    with pytest.raises(ValidationError):
        RomsNamelistV0_5_0.read(V0_6_0_NAMELIST)


def test_roms_namelist_base_rejects_direct_use():
    """`RomsNamelistBase` refuses validation against itself.

    Its version-varying groups are typed as the loose common models, so using
    it directly would silently accept a namelist no ucla-roms version reads.
    """
    with pytest.raises(TypeError, match="not a usable schema"):
        RomsNamelistBase.read(OLD_NAMELIST)


def test_pio_settings_defaults_when_group_missing(tmp_path):
    """A 0.6.0-schema namelist without `&pio_settings` still validates,
    defaulting `pio_stride` to 1.
    """
    text = V0_6_0_NAMELIST.read_text()
    assert PIO_SETTINGS_BLOCK in text
    trimmed = tmp_path / "no_pio_settings.nml"
    trimmed.write_text(text.replace(PIO_SETTINGS_BLOCK, ""))

    nml = RomsNamelistV0_6_0.read(trimmed)
    assert nml.pio_settings.pio_stride == 1


def test_pio_settings_reads_and_round_trips_through_write(tmp_path):
    """`pio_settings.pio_stride` reads from the 0.6.0 fixture and a modified
    value round-trips through `write`.
    """
    nml = RomsNamelistV0_6_0.read(V0_6_0_NAMELIST)
    assert nml.pio_settings.pio_stride == 1

    nml.pio_settings.pio_stride = 4
    out = tmp_path / "namelist.nml"
    nml.write(out)

    reread = RomsNamelistV0_6_0.read(out)
    assert reread.pio_settings.pio_stride == 4


def test_pio_settings_always_written_even_when_constructed_without_it(tmp_path):
    """`pio_settings` is present in written output even when the model was
    constructed without an explicit `&pio_settings` group (using the default).
    """
    text = V0_6_0_NAMELIST.read_text().replace(PIO_SETTINGS_BLOCK, "")
    trimmed = tmp_path / "no_pio_settings.nml"
    trimmed.write_text(text)

    nml = RomsNamelistV0_6_0.read(trimmed)
    d = nml.to_f90nml_dict()
    assert d["pio_settings"] == {"pio_stride": 1}


def test_pio_settings_rejects_non_positive_stride():
    """`pio_stride` must be >= 1."""
    with pytest.raises(ValidationError):
        PioSettings(pio_stride=0)


def test_roms_namelist_v0_7_0_round_trip(tmp_path):
    """`RomsNamelistV0_7_0.read` parses the 0.7.0 fixture, and a read -> write
    -> read round trip reproduces the same model.
    """
    nml = RomsNamelistV0_7_0.read(V0_7_0_NAMELIST)

    out = tmp_path / "namelist.nml"
    nml.write(out)
    reread = RomsNamelistV0_7_0.read(out)

    assert reread == nml


def test_roms_namelist_v0_6_0_rejects_v0_7_0_fixture():
    """`RomsNamelistV0_6_0` is strict: the 0.7.0 fixture carries the two new
    CDR output groups, so it is rejected as unknown extra groups.
    """
    with pytest.raises(ValidationError):
        RomsNamelistV0_6_0.read(V0_7_0_NAMELIST)


def test_cdr_output_settings_default_when_groups_missing():
    """A 0.6.0-schema namelist without `&cdr_tracer_output_settings` or
    `&cdr_gas_exch_output_settings` still validates under `RomsNamelistV0_7_0`,
    defaulting both groups to their ucla-roms reference values.
    """
    nml = RomsNamelistV0_7_0.read(V0_6_0_NAMELIST)

    assert nml.cdr_tracer_output_settings.do_cdr_tracer_output is False
    assert nml.cdr_tracer_output_settings.wrt_cdr_trc_avg is True
    assert nml.cdr_tracer_output_settings.nrpf_cdr_trc == 4
    assert nml.cdr_gas_exch_output_settings.do_cdr_gas_exch_output is False
    assert nml.cdr_gas_exch_output_settings.wrt_cdr_gas_avg is True
    assert nml.cdr_gas_exch_output_settings.nrpf_cdr_gas == 4


def test_cdr_output_settings_always_written_even_when_constructed_without_them():
    """Both new CDR output groups are present in written output even when the
    model was constructed without explicit groups (using the defaults).
    """
    nml = RomsNamelistV0_7_0.read(V0_6_0_NAMELIST)
    d = nml.to_f90nml_dict()

    assert d["cdr_tracer_output_settings"]["do_cdr_tracer_output"] is False
    assert d["cdr_tracer_output_settings"]["nrpf_cdr_trc"] == 4
    assert d["cdr_gas_exch_output_settings"]["do_cdr_gas_exch_output"] is False
    assert d["cdr_gas_exch_output_settings"]["nrpf_cdr_gas"] == 4


def test_roms_namelist_v0_9_0_round_trip(tmp_path):
    """`RomsNamelistV0_9_0.read` parses the 0.9.0 fixture, and a read -> write
    -> read round trip reproduces the same model.
    """
    nml = RomsNamelistV0_9_0.read(V0_9_0_NAMELIST)
    assert nml.cdr_lite_output_settings.do_cdr_lite_output is False
    assert nml.cdr_lite_output_settings.wrt_gas_exchange is False
    assert nml.cdr_lite_settings.cdr_online_carbonate_sensitivity is False

    out = tmp_path / "namelist.nml"
    nml.write(out)
    reread = RomsNamelistV0_9_0.read(out)

    assert reread == nml


def test_roms_namelist_v0_9_0_rejects_v0_7_0_fixture():
    """`RomsNamelistV0_9_0` is strict: ucla-roms 0.9.0 renamed
    `&cdr_tracer_output_settings` to `&cdr_lite_output_settings`, so the 0.7.0
    fixture is rejected for the old group.
    """
    with pytest.raises(ValidationError, match="cdr_tracer_output_settings"):
        RomsNamelistV0_9_0.read(V0_7_0_NAMELIST)


def test_roms_namelist_v0_7_0_rejects_v0_9_0_fixture():
    """`RomsNamelistV0_7_0` is strict: the 0.9.0 fixture carries the renamed and
    new CDR_LITE groups, which it rejects as unknown.
    """
    with pytest.raises(ValidationError, match="cdr_lite_output_settings"):
        RomsNamelistV0_7_0.read(V0_9_0_NAMELIST)


def test_roms_namelist_v0_9_0_does_not_write_cdr_tracer_group():
    """`RomsNamelistV0_9_0` never carries the pre-0.9.0 tracer output group, so
    it cannot be written into a 0.9.0 namelist.
    """
    assert "cdr_tracer_output_settings" not in RomsNamelistV0_9_0.model_fields
    nml = RomsNamelistV0_9_0.read(V0_9_0_NAMELIST)
    assert "cdr_tracer_output_settings" not in nml.to_f90nml_dict()


def test_roms_namelist_v0_11_0_round_trip(tmp_path):
    """`RomsNamelistV0_11_0.read` parses the 0.11.0 fixture (keys spelled
    `use_constant_Fe`/`constant_Fe_value`, as ROMS does), and a read -> write ->
    read round trip reproduces the same model with the keys lowercased.
    """
    nml = RomsNamelistV0_11_0.read(V0_11_0_NAMELIST)
    assert nml.marbl_biogeochemistry_settings.use_constant_fe is True
    assert nml.marbl_biogeochemistry_settings.constant_fe_value == 5e-4

    out = tmp_path / "namelist.nml"
    nml.write(out)
    written = out.read_text()
    assert "use_constant_fe = .true." in written
    assert "constant_fe_value" in written
    reread = RomsNamelistV0_11_0.read(out)

    assert reread == nml


def test_roms_namelist_v0_9_0_rejects_v0_11_0_fixture():
    """`RomsNamelistV0_9_0` is strict: ucla-roms releases before 0.11.0 stop on
    the constant-iron keys, so the schema for them rejects the 0.11.0 fixture.
    """
    with pytest.raises(ValidationError, match="use_constant_fe"):
        RomsNamelistV0_9_0.read(V0_11_0_NAMELIST)


def test_roms_namelist_v0_11_0_constant_fe_defaults_and_always_written():
    """A 0.9.0 namelist without the keys still validates under
    `RomsNamelistV0_11_0`, defaulting to the ucla-roms reference values, and
    both keys are written regardless.
    """
    nml = RomsNamelistV0_11_0.read(V0_9_0_NAMELIST)
    marbl = nml.marbl_biogeochemistry_settings
    assert marbl.use_constant_fe is False
    assert marbl.constant_fe_value == 3.0e-3

    written = nml.to_f90nml_dict()["marbl_biogeochemistry_settings"]
    assert written["use_constant_fe"] is False
    assert written["constant_fe_value"] == 3.0e-3


def test_roms_namelist_v0_9_0_does_not_write_constant_fe():
    """`RomsNamelistV0_9_0` carries no constant-iron keys, so it cannot write them."""
    nml = RomsNamelistV0_9_0.read(V0_9_0_NAMELIST)
    written = nml.to_f90nml_dict()["marbl_biogeochemistry_settings"]
    assert "use_constant_fe" not in written
    assert "constant_fe_value" not in written


def test_roms_namelist_v0_12_0_round_trip(tmp_path):
    """`RomsNamelistV0_12_0.read` parses the 0.12.0 fixture, whose time
    interpolation is switched off, and a read -> write -> read round trip
    reproduces the same model.
    """
    nml = RomsNamelistV0_12_0.read(V0_12_0_NAMELIST)
    assert isinstance(nml.cdr_lite_settings, CdrLiteSettingsV0_12_0)
    assert nml.cdr_lite_settings.cdr_carbonate_sensitivity_time_interpolation is False
    assert isinstance(nml.extract_data_settings, ExtractDataSettingsV0_12_0)

    out = tmp_path / "namelist.nml"
    nml.write(out)
    assert "cdr_carbonate_sensitivity_time_interpolation = .false." in out.read_text()
    assert RomsNamelistV0_12_0.read(out) == nml


def test_roms_namelist_v0_11_0_rejects_v0_12_0_fixture():
    """`RomsNamelistV0_11_0` is strict: ucla-roms before 0.12.0 stops on the
    time-interpolation key, so the 0.11.0 schema rejects it and never writes it.
    """
    with pytest.raises(
        ValidationError, match="cdr_carbonate_sensitivity_time_interpolation"
    ):
        RomsNamelistV0_11_0.read(V0_12_0_NAMELIST)
    written = RomsNamelistV0_11_0.read(V0_11_0_NAMELIST).to_f90nml_dict()
    assert (
        "cdr_carbonate_sensitivity_time_interpolation"
        not in written["cdr_lite_settings"]
    )


def test_roms_namelist_v0_12_0_time_interpolation_defaults_and_always_written():
    """A 0.11.0 namelist without the key still validates under
    `RomsNamelistV0_12_0`, defaulting to interpolation on (what earlier
    releases always did), and the written namelist carries it.
    """
    nml = RomsNamelistV0_12_0.read(V0_11_0_NAMELIST)
    assert nml.cdr_lite_settings.cdr_carbonate_sensitivity_time_interpolation is True
    written = nml.to_f90nml_dict()["cdr_lite_settings"]
    assert written["cdr_carbonate_sensitivity_time_interpolation"] is True


_TWO_CHILDREN = {
    "do_extract": True,
    "output_period_extract": 3600.0,
    "nrpf_extract": 24,
    "extract_file": ["a.nc", "b.nc"],
    "n_chd": [90, 50],
    "theta_s_chd": [5.0, 6.0],
    "theta_b_chd": [2.0, 3.0],
    "hc_chd": [250.0, 300.0],
    "extract_root_name": ["child_a", "child_b"],
}


def test_extract_v0_12_0_accepts_one_entry_per_child(tmp_path):
    """From 0.12.0 the per-child extraction keys take one entry per child (PR
    #395); the arrays survive a write -> read round trip, and a single value
    stays a single value.
    """
    nml = RomsNamelistV0_12_0.read(V0_12_0_NAMELIST)
    assert nml.extract_data_settings.extract_file == "sample_edata.nc"
    nml.extract_data_settings = ExtractDataSettingsV0_12_0(**_TWO_CHILDREN)

    out = tmp_path / "namelist.nml"
    nml.write(out)
    reread = RomsNamelistV0_12_0.read(out)
    assert reread.extract_data_settings.n_chd == [90, 50]
    assert reread.extract_data_settings.extract_root_name == ["child_a", "child_b"]
    assert reread == nml


@pytest.mark.parametrize(
    "key", ["n_chd", "theta_s_chd", "theta_b_chd", "hc_chd", "extract_root_name"]
)
def test_extract_v0_12_0_needs_every_child_entry(key):
    """ROMS stops when a child after the first lacks its own vertical-grid
    entry (only child 1 has defaults); a list `extract_root_name` must also
    cover every child.
    """
    with pytest.raises(ValidationError, match=f"{key} has 1 entries for 2 child"):
        ExtractDataSettingsV0_12_0(**{**_TWO_CHILDREN, key: _TWO_CHILDREN[key][:1]})


def test_extract_v0_12_0_rules_apply_only_when_extracting():
    """ROMS checks the per-child layout only with `do_extract` on; a single
    `extract_root_name` is fine for several children (only PARALLEL_IO builds
    need one per child, which the schema cannot see).
    """
    off = {**_TWO_CHILDREN, "do_extract": False, "n_chd": 90}
    assert ExtractDataSettingsV0_12_0(**off).n_chd == 90
    shared_root = {**_TWO_CHILDREN, "extract_root_name": "child"}
    assert ExtractDataSettingsV0_12_0(**shared_root).extract_root_name == "child"


def test_extract_v0_12_0_rejects_more_than_max_children_and_blank_files():
    many = [f"c{i}.nc" for i in range(9)]
    with pytest.raises(ValidationError, match="extract_file has 9 entries"):
        ExtractDataSettingsV0_12_0(
            **{**_TWO_CHILDREN, "do_extract": False, "extract_file": many}
        )
    with pytest.raises(ValidationError, match="blank entry"):
        ExtractDataSettingsV0_12_0(**{**_TWO_CHILDREN, "extract_file": ["a.nc", " "]})


def test_extract_before_0_12_0_rejects_several_children():
    """Earlier releases read one child; the 0.11.0 schema rejects arrays."""
    data = RomsNamelistV0_11_0.read(V0_11_0_NAMELIST).model_dump()
    data["extract_data_settings"] = _TWO_CHILDREN
    with pytest.raises(ValidationError, match="extract_file"):
        RomsNamelistV0_11_0.model_validate(data)


def test_constant_fe_value_must_not_be_negative():
    """ROMS stops on a negative `constant_Fe_value`: the schema rejects it on
    construction and on assignment, and accepts zero.
    """
    nml = RomsNamelistV0_11_0.read(V0_11_0_NAMELIST)
    marbl = nml.marbl_biogeochemistry_settings

    with pytest.raises(ValidationError, match="constant_fe_value"):
        MarblBiogeochemistrySettingsV0_11_0(
            **{**marbl.model_dump(), "constant_fe_value": -1.0}
        )
    with pytest.raises(ValidationError, match="constant_fe_value"):
        marbl.constant_fe_value = -1.0
    assert marbl.constant_fe_value == 5e-4

    marbl.constant_fe_value = 0.0
    assert marbl.constant_fe_value == 0.0


def test_cdr_lite_groups_default_when_missing():
    """A 0.6.0-schema namelist without any CDR output group still validates
    under `RomsNamelistV0_9_0`, defaulting the three CDR groups to their
    ucla-roms reference values.
    """
    nml = RomsNamelistV0_9_0.read(V0_6_0_NAMELIST)

    assert nml.cdr_lite_settings == CdrLiteSettings()
    assert nml.cdr_lite_settings.cdr_online_carbonate_sensitivity is False
    assert nml.cdr_lite_output_settings == CdrLiteOutputSettings()
    assert nml.cdr_lite_output_settings.do_cdr_lite_output is False
    assert nml.cdr_lite_output_settings.wrt_cdr_lite_avg is True
    assert nml.cdr_lite_output_settings.nrpf_cdr_lite == 4
    assert nml.cdr_lite_output_settings.output_period_cdr_lite == 3600.0
    assert nml.cdr_lite_output_settings.wrt_gas_exchange is False
    assert nml.cdr_gas_exch_output_settings.do_cdr_gas_exch_output is False
    assert nml.cdr_gas_exch_output_settings.nrpf_cdr_gas == 4


def test_cdr_lite_groups_always_written_even_when_constructed_without_them():
    """The CDR_LITE groups are present in written output even when the model was
    constructed without explicit groups (using the defaults), in the field
    order ucla-roms documents.
    """
    nml = RomsNamelistV0_9_0.read(V0_6_0_NAMELIST)
    d = nml.to_f90nml_dict()

    assert d["cdr_lite_settings"] == {"cdr_online_carbonate_sensitivity": False}
    assert d["cdr_lite_output_settings"]["do_cdr_lite_output"] is False
    assert d["cdr_lite_output_settings"]["wrt_gas_exchange"] is False
    assert d["cdr_gas_exch_output_settings"]["nrpf_cdr_gas"] == 4
    assert list(d["cdr_lite_output_settings"]) == [
        "do_cdr_lite_output",
        "wrt_cdr_lite_avg",
        "cdr_lite_monthly_averages",
        "output_period_cdr_lite",
        "nrpf_cdr_lite",
        "wrt_tracers",
        "wrt_vertical_integrals",
        "wrt_thickness_weighted",
        "wrt_sources",
        "wrt_alk",
        "wrt_dic",
        "wrt_gas_exchange",
    ]


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

    def test_pio_settings_key(self):
        """`pio_settings.pio_stride` is only a known group/key from 0.6.0 on:
        `RomsNamelistV0_6_0` accepts it, but `RomsNamelist` and
        `RomsNamelistV0_5_0` (which lack the group) report it as unknown.
        """
        overrides = {"pio_settings": {"pio_stride": 4}}
        assert RomsNamelistV0_6_0.unknown_override_keys(overrides) == []

        violations = RomsNamelist.unknown_override_keys(overrides)
        assert len(violations) == 1
        assert "pio_settings" in violations[0]

        violations = RomsNamelistV0_5_0.unknown_override_keys(overrides)
        assert len(violations) == 1
        assert "pio_settings" in violations[0]

    def test_cdr_output_settings_keys(self):
        """`cdr_tracer_output_settings`/`cdr_gas_exch_output_settings` keys are
        only known from 0.7.0 on: `RomsNamelistV0_7_0` accepts them, but
        `RomsNamelistV0_6_0` (which lacks the groups) reports them as unknown.
        (`cdr_tracer_output_settings` is renamed away again in 0.9.0.)
        """
        overrides = {
            "cdr_tracer_output_settings": {"wrt_alk": False},
            "cdr_gas_exch_output_settings": {"wrt_cdr_gas_avg": False},
        }
        assert RomsNamelistV0_7_0.unknown_override_keys(overrides) == []

        violations = RomsNamelistV0_6_0.unknown_override_keys(overrides)
        assert len(violations) == 2
        assert any("cdr_tracer_output_settings" in v for v in violations)
        assert any("cdr_gas_exch_output_settings" in v for v in violations)

    def test_cdr_lite_output_settings_keys(self):
        """`cdr_lite_output_settings`/`cdr_lite_settings` keys are known from
        0.9.0 on, where `cdr_tracer_output_settings` was renamed away:
        `RomsNamelistV0_9_0` accepts the new groups and rejects the old one,
        and `RomsNamelistV0_7_0` does the reverse.
        """
        new = {
            "cdr_lite_output_settings": {"do_cdr_lite_output": True},
            "cdr_lite_settings": {"cdr_online_carbonate_sensitivity": True},
        }
        old = {"cdr_tracer_output_settings": {"do_cdr_tracer_output": True}}
        assert RomsNamelistV0_9_0.unknown_override_keys(new) == []

        violations = RomsNamelistV0_9_0.unknown_override_keys(old)
        assert len(violations) == 1
        assert "cdr_tracer_output_settings" in violations[0]

        assert RomsNamelistV0_7_0.unknown_override_keys(old) == []
        violations = RomsNamelistV0_7_0.unknown_override_keys(new)
        assert len(violations) == 2
        assert any("cdr_lite_output_settings" in v for v in violations)
        assert any("cdr_lite_settings" in v for v in violations)

    def test_cdr_lite_output_settings_rejects_pre_rename_key(self):
        """A pre-0.9.0 key name inside the renamed group is reported as unknown."""
        overrides = {"cdr_lite_output_settings": {"do_cdr_tracer_output": True}}
        violations = RomsNamelistV0_9_0.unknown_override_keys(overrides)
        assert len(violations) == 1
        assert "do_cdr_tracer_output" in violations[0]

    def test_constant_fe_keys(self):
        """The constant-iron keys are known from 0.11.0 on: `RomsNamelistV0_11_0`
        accepts them (lowercase, as f90nml spells them) and `RomsNamelistV0_9_0`
        reports both as unknown.
        """
        overrides = {
            "marbl_biogeochemistry_settings": {
                "use_constant_fe": True,
                "constant_fe_value": 1.0e-3,
            }
        }
        assert RomsNamelistV0_11_0.unknown_override_keys(overrides) == []

        violations = RomsNamelistV0_9_0.unknown_override_keys(overrides)
        assert len(violations) == 2
        assert any("use_constant_fe" in v for v in violations)
        assert any("constant_fe_value" in v for v in violations)

    def test_carbonate_sensitivity_time_interpolation_key(self):
        """Known from 0.12.0 on; unknown to `RomsNamelistV0_11_0`."""
        key = "cdr_carbonate_sensitivity_time_interpolation"
        overrides = {"cdr_lite_settings": {key: False}}
        assert RomsNamelistV0_12_0.unknown_override_keys(overrides) == []
        violations = RomsNamelistV0_11_0.unknown_override_keys(overrides)
        assert len(violations) == 1
        assert key in violations[0]


# ---------------------------------------------------------------------------
# ParamSettings / ParamSettingsV0_4_0 -- passive CDR tracer counts
# ---------------------------------------------------------------------------
_PARAM_SETTINGS_KWARGS = {
    "np_xi": 1,
    "np_eta": 1,
    "llm": 10,
    "mmm": 10,
    "nz": 3,
    "nt_passive": 0,
    "nt_bgc": 0,
}


def test_param_settings_rejects_cdr_tracer_counts():
    """`ParamSettings` (< 0.4.0) is strict: `nt_cdr_oae`/`nt_cdr_dor` are unknown
    (`extra="forbid"`), since ucla-roms < 0.4.0 has no passive CDR tracers.
    """
    with pytest.raises(ValidationError, match="nt_cdr_oae"):
        ParamSettings(**_PARAM_SETTINGS_KWARGS, nt_cdr_oae=1)


@pytest.mark.parametrize(
    "cls",
    [
        ParamSettingsV0_4_0,
        RomsNamelistV0_5_0,
        RomsNamelistV0_7_0,
        RomsNamelistV0_9_0,
        RomsNamelistV0_11_0,
        RomsNamelistV0_12_0,
    ],
)
def test_param_settings_v0_4_0_defaults_cdr_tracer_counts_to_zero(cls):
    """`nt_cdr_oae`/`nt_cdr_dor` default to 0 (the Fortran initializer) when
    omitted, on `ParamSettingsV0_4_0` directly and on every namelist schema
    whose `param_settings` group is (or subclasses) it.
    """
    param_settings_cls = (
        cls
        if cls is ParamSettingsV0_4_0
        else cls.model_fields["param_settings"].annotation
    )
    settings = param_settings_cls(**_PARAM_SETTINGS_KWARGS)
    assert settings.nt_cdr_oae == 0
    assert settings.nt_cdr_dor == 0


def test_param_settings_v0_4_0_accepts_non_zero_cdr_tracer_counts():
    """Non-zero `nt_cdr_oae`/`nt_cdr_dor` are accepted."""
    settings = ParamSettingsV0_4_0(**_PARAM_SETTINGS_KWARGS, nt_cdr_oae=2, nt_cdr_dor=1)
    assert settings.nt_cdr_oae == 2
    assert settings.nt_cdr_dor == 1


@pytest.mark.parametrize("field", ["nt_cdr_oae", "nt_cdr_dor"])
def test_param_settings_v0_4_0_rejects_negative_cdr_tracer_counts(field):
    """`nt_cdr_oae`/`nt_cdr_dor` must be `>= 0`."""
    with pytest.raises(ValidationError):
        ParamSettingsV0_4_0(**_PARAM_SETTINGS_KWARGS, **{field: -1})
