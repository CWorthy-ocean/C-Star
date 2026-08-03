"""Tests for DomainCatalog GitHub URL handling."""

from pathlib import Path
from unittest.mock import patch

import pytest

from cstar_forge.domain_catalog import (
    DomainCatalog,
    _is_github_catalog_url,
    _parse_github_catalog_url,
)


@pytest.mark.parametrize(
    "url,expected",
    [
        (
            "https://github.com/CWorthy-ocean/cstar-forge",
            ("CWorthy-ocean", "cstar-forge", "main", Path(".")),
        ),
        (
            "https://github.com/CWorthy-ocean/cstar-forge/",
            ("CWorthy-ocean", "cstar-forge", "main", Path(".")),
        ),
        (
            "https://github.com/CWorthy-ocean/cstar-forge/tree/main/cstar_forge/catalog",
            ("CWorthy-ocean", "cstar-forge", "main", Path("cstar_forge/catalog")),
        ),
        (
            "https://github.com/CWorthy-ocean/cstar-forge/tree/develop/cstar_forge/catalog",
            ("CWorthy-ocean", "cstar-forge", "develop", Path("cstar_forge/catalog")),
        ),
        (
            "git@github.com:CWorthy-ocean/cstar-forge.git",
            ("CWorthy-ocean", "cstar-forge", "main", Path(".")),
        ),
    ],
)
def test_parse_github_catalog_url(url, expected):
    assert _parse_github_catalog_url(url) == expected


def test_is_github_catalog_url():
    assert _is_github_catalog_url("https://github.com/org/repo")
    assert _is_github_catalog_url("git@github.com:org/repo.git")
    assert not _is_github_catalog_url("/local/path/with/github/in/name")
    assert not _is_github_catalog_url("local")


def test_github_catalog_uses_org_and_repo():
    url = "https://github.com/CWorthy-ocean/cstar-forge"
    with patch("cstar_forge.domain_catalog.fsspec.filesystem") as mock_fs:
        instance = mock_fs.return_value
        instance.protocol = "github"
        instance.exists = lambda _path: False
        instance.ls = lambda _path, detail=False: []
        instance.glob = lambda _pattern: []
        catalog = DomainCatalog(
            catalog_root=url,
            suppress_validation=True,
        )
    mock_fs.assert_called_once_with(
        "github", org="CWorthy-ocean", repo="cstar-forge", sha="main"
    )
    assert catalog.catalog_root == Path(".")
    assert catalog._fs is instance


def test_parse_github_catalog_url_invalid():
    with pytest.raises(ValueError, match="Could not parse GitHub org/repo"):
        _parse_github_catalog_url("https://github.com/only-org")


# ---------------------------------------------------------------------------
# register_output / register_forcing / register_domain_from_dict /
# register_model_from_settings -- the "save modified pieces to catalog" writers
# ---------------------------------------------------------------------------
@pytest.fixture
def isolated_catalog(tmp_path):
    import shutil

    from cstar_forge.domain_catalog import default_catalog as _cat

    root = tmp_path / "catalog"
    shutil.copytree(_cat.catalog_root, root)
    return DomainCatalog(catalog_root=root)


def test_register_output_writes_and_rescans(isolated_catalog):
    from cstar_forge.domain_catalog import default_catalog as _cat

    out = _cat.output_data("standard")
    isolated_catalog.register_output("my-output", out, description="test out")
    assert "my-output" in isolated_catalog.output_names
    assert isolated_catalog.output_data("my-output") == out  # description popped


def test_register_output_refuses_collision(isolated_catalog):
    from cstar_forge.domain_catalog import default_catalog as _cat

    out = _cat.output_data("standard")
    with pytest.raises(FileExistsError):
        isolated_catalog.register_output("standard", out)


def test_register_forcing_writes_cdr_block(isolated_catalog):
    from cstar_forge.domain_catalog import default_catalog as _cat

    fdata = _cat.forcing_data("glorys-era5-unified")
    fi = {
        "initial_conditions": fdata["initial_conditions"],
        "forcing": fdata["forcing"],
    }
    isolated_catalog.register_forcing(
        "my-forcing", fi, cdr_forcing={"foo": "bar"}, description="test forcing"
    )
    assert "my-forcing" in isolated_catalog.forcing_names
    reloaded = isolated_catalog.forcing_data("my-forcing")
    assert reloaded["cdr_forcing"] == {"foo": "bar"}
    assert reloaded["initial_conditions"] == fi["initial_conditions"]


def test_register_forcing_omits_cdr_block_when_absent(isolated_catalog):
    from cstar_forge.domain_catalog import default_catalog as _cat

    fdata = _cat.forcing_data("glorys-era5-unified")
    fi = {
        "initial_conditions": fdata["initial_conditions"],
        "forcing": fdata["forcing"],
    }
    isolated_catalog.register_forcing("no-cdr-forcing", fi)
    assert "cdr_forcing" not in isolated_catalog.forcing_data("no-cdr-forcing")


def test_register_domain_from_dict_round_trips(isolated_catalog):
    from cstar_forge.domain_catalog import default_catalog as _cat

    ddata = _cat.domain_data("wio-toy")
    isolated_catalog.register_domain_from_dict("my-domain", ddata)
    assert "my-domain" in isolated_catalog.domain_names
    assert isolated_catalog.domain_data("my-domain") == ddata
    assert (isolated_catalog.domain_path("my-domain") / "Assets").is_dir()


def test_register_model_from_settings_clones_code_block(isolated_catalog):
    from cstar_forge.domain_catalog import default_catalog as _cat

    base_dir = _cat.model_dir("cson_roms-marbl_v0.1")
    isolated_catalog.register_model_from_settings(
        "my-model",
        {"param": {"nt_passive": 0}},
        base_dir,
        description="m",
    )
    assert "my-model" in isolated_catalog.model_names
    data = isolated_catalog.model_data("my-model")
    assert data["model_settings"] == {"param": {"nt_passive": 0}}
    base = _cat.model_data("cson_roms-marbl_v0.1")
    assert data["code"] == base["code"]
    assert data["bgc_mode"] == base["bgc_mode"]
    assert data["use_pio"] == base["use_pio"]


def test_register_model_from_settings_applies_live_overrides(isolated_catalog):
    from cstar_forge.domain_catalog import default_catalog as _cat

    base_dir = _cat.model_dir("cson_roms-marbl_v0.1")
    base_code = _cat.model_data("cson_roms-marbl_v0.1")["code"]
    isolated_catalog.register_model_from_settings(
        "my-model-pio",
        {"param": {"nt_passive": 0}},
        base_dir,
        description="m",
        bgc_mode="none",
        use_pio=True,
        roms_ref="main",
    )
    data = isolated_catalog.model_data("my-model-pio")
    assert data["use_pio"] is True
    assert data["bgc_mode"] == "none"
    assert data["code"]["roms"]["commit"] == "main"
    assert "branch" not in data["code"]["roms"]
    # Unused repos (pio/marbl) survive verbatim so the toggles stay usable later.
    assert data["code"]["pio"] == base_code["pio"]
    assert data["code"]["marbl"] == base_code["marbl"]
