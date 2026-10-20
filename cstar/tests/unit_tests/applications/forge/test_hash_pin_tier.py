"""A ucla-roms commit-hash pin selects the same namelist tier in Forge as in roms_marbl.

roms_marbl resolves a commit hash to its nearest release tag in its clone and
reads Forge's namelist strictly, so Forge must write the namelist of that
release's tier rather than the newest one.
"""

import warnings

import pytest
from pydantic import ValidationError
from test_forge_blueprint import _build, _FakeBuilder
from test_write_namelist import _base_settings

from cstar.applications.forge.engine import process_forge_blueprint
from cstar.applications.forge.namelist_model import (
    namelist_schema_for,
    run_time_settings_for_ref,
)
from cstar.applications.forge.settings import write_roms_namelist
from cstar.roms.namelist import (
    NAMELIST_SCHEMA_REGISTRY,
    RomsNamelistV0_4_0,
    RomsNamelistV0_5_0,
    namelist_schema_for_ref,
    release_ref_for_remote,
)

_NEWEST_SCHEMA = NAMELIST_SCHEMA_REGISTRY[-1][2]

# fixture hash -> the tier of the release it descends from
_TIER_OF_HASH = [
    ("hash_0_4_2", RomsNamelistV0_4_0),
    ("hash_after_0_4_2", RomsNamelistV0_4_0),
    ("hash_0_5_0", RomsNamelistV0_5_0),
]


@pytest.fixture
def _quiet_commit_ahead_warning():
    """The commits-ahead warning is covered by the namelist tests."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*commit.*after release")
        yield


@pytest.mark.usefixtures("_quiet_commit_ahead_warning")
@pytest.mark.parametrize(("hash_key", "expected"), _TIER_OF_HASH)
def test_forge_selects_the_tier_roms_marbl_selects(
    tagged_ucla_roms_clone, hash_key, expected
):
    """Forge (no clone: the hash is resolved through the repository's location)
    and roms_marbl (the full clone) pick the same namelist schema for a hash pin.
    """
    info = tagged_ucla_roms_clone
    pin = info[hash_key]

    forge_cls = run_time_settings_for_ref(
        release_ref_for_remote(str(info["repo"]), pin)
    )
    roms_marbl_cls = namelist_schema_for_ref(pin, repo_path=info["repo"])

    assert namelist_schema_for(forge_cls) is roms_marbl_cls
    assert roms_marbl_cls is expected


def test_unresolved_hash_selects_the_newest_tier(tagged_ucla_roms_clone):
    """The bug: Forge, handed the raw hash, guesses the newest tier while
    roms_marbl resolves the same hash to an older one.
    """
    info = tagged_ucla_roms_clone
    with pytest.warns(UserWarning, match="not a release tag"):
        forge_cls = run_time_settings_for_ref(info["hash_0_4_2"])

    assert namelist_schema_for(forge_cls) is _NEWEST_SCHEMA
    assert (
        namelist_schema_for_ref(info["hash_0_4_2"], repo_path=info["repo"])
        is not _NEWEST_SCHEMA
    )


@pytest.mark.usefixtures("_quiet_commit_ahead_warning")
@pytest.mark.parametrize(("hash_key", "expected"), _TIER_OF_HASH)
def test_forge_namelist_for_a_hash_pin_reads_in_roms_marbl(
    tagged_ucla_roms_clone, tmp_path, hash_key, expected
):
    """The namelist Forge writes for a hash pin passes roms_marbl's strict read
    of the schema it selects for that hash.
    """
    info = tagged_ucla_roms_clone
    pin = info[hash_key]

    write_roms_namelist(
        _base_settings(),
        tmp_path,
        n_tracers=34,
        roms_ref=release_ref_for_remote(str(info["repo"]), pin),
    )

    schema = namelist_schema_for_ref(pin, repo_path=info["repo"])
    assert isinstance(schema.read(tmp_path / "namelist.nml"), expected)


def test_forge_namelist_for_an_unresolved_hash_is_refused_by_roms_marbl(
    tagged_ucla_roms_clone, tmp_path
):
    """The bug end to end: the namelist Forge writes for the raw hash (newest
    tier) fails roms_marbl's strict read of the older tier it selects.
    """
    info = tagged_ucla_roms_clone
    pin = info["hash_0_4_2"]

    with pytest.warns(UserWarning, match="not a release tag"):
        write_roms_namelist(_base_settings(), tmp_path, n_tracers=34, roms_ref=pin)

    schema = namelist_schema_for_ref(pin, repo_path=info["repo"])
    with pytest.raises(ValidationError):
        schema.read(tmp_path / "namelist.nml")


def _pinned_blueprint(repo, pin):
    """A resolved blueprint pinned to the commit `pin`, whose ucla-roms is
    fetched from the local repository `repo`.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*not a release tag")
        cfg = _build(roms_ref=pin)
    cfg.code.roms.location = str(repo)
    return cfg


def test_process_forge_blueprint_validates_against_the_resolved_release(
    tagged_ucla_roms_clone, monkeypatch
):
    """`process_forge_blueprint` hands the validator the release tag a hash pin
    resolves to, not the hash.
    """
    info = tagged_ucla_roms_clone
    cfg = _pinned_blueprint(info["repo"], info["hash_0_4_2"])
    seen = {}

    def fake_validate(model_settings, *, roms_ref):
        seen["roms_ref"] = roms_ref
        return []

    monkeypatch.setattr(
        "cstar.applications.forge.engine.validate_run_time_sections", fake_validate
    )

    process_forge_blueprint(
        cfg,
        validate=True,
        ensure_data=False,
        generate=False,
        configure=False,
        executor_factory=_FakeBuilder,
    )

    assert seen["roms_ref"] == "0.4.2"
    assert cfg.code.roms.commit == info["hash_0_4_2"]
