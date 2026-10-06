"""Tests for DomainCatalog GitHub URL handling."""

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from cstar.catalog.domain_catalog import (
    _DEFAULT_CATALOG_ROOT,
    DomainCatalog,
    LayeredCatalog,
    _is_github_catalog_url,
    _parse_github_catalog_url,
    user_catalog_root,
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
    with patch("cstar.catalog.domain_catalog.fsspec.filesystem") as mock_fs:
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
# register_model_from_settings -- the "save modified specs to catalog" writers
# ---------------------------------------------------------------------------
@pytest.fixture
def isolated_catalog(tmp_path):
    import shutil

    from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT

    root = tmp_path / "catalog"
    # Copy the BUNDLED catalog (not default_catalog.catalog_root, which is now
    # the writable *user* layer -- empty/nonexistent in tests, see conftest.py).
    shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
    return DomainCatalog(catalog_root=root)


def test_register_output_writes_and_rescans(isolated_catalog):
    from cstar.catalog.domain_catalog import default_catalog as _cat

    out = _cat.output_data("standard")
    isolated_catalog.register_output("my-output", out, description="test out")
    assert "my-output" in isolated_catalog.output_names
    assert isolated_catalog.output_data("my-output") == out  # description popped


def test_register_output_refuses_collision(isolated_catalog):
    from cstar.catalog.domain_catalog import default_catalog as _cat

    out = _cat.output_data("standard")
    with pytest.raises(FileExistsError):
        isolated_catalog.register_output("standard", out)


def test_register_forcing_writes_and_rescans(isolated_catalog):
    from cstar.catalog.domain_catalog import default_catalog as _cat

    fdata = _cat.forcing_data("glorys-era5-unified")
    fi = {
        "initial_conditions": fdata["initial_conditions"],
        "forcing": fdata["forcing"],
    }
    isolated_catalog.register_forcing("my-forcing", fi, description="test forcing")
    assert "my-forcing" in isolated_catalog.forcing_names
    reloaded = isolated_catalog.forcing_data("my-forcing")
    assert reloaded["initial_conditions"] == fi["initial_conditions"]


def test_register_forcing_no_longer_accepts_cdr_forcing(isolated_catalog):
    """CDR configuration moved out of ForcingSpec into its own CdrSpec entry
    type -- register_forcing must no longer accept a cdr_forcing kwarg.
    """
    from cstar.catalog.domain_catalog import default_catalog as _cat

    fdata = _cat.forcing_data("glorys-era5-unified")
    fi = {
        "initial_conditions": fdata["initial_conditions"],
        "forcing": fdata["forcing"],
    }
    with pytest.raises(TypeError):
        isolated_catalog.register_forcing(
            "no-cdr-forcing", fi, cdr_forcing={"foo": "bar"}
        )


# ---------------------------------------------------------------------------
# register_cdr / cdr_names / cdr_data -- the new CdrSpec catalog entry type
# ---------------------------------------------------------------------------


def test_register_cdr_yaml_mode_round_trip(isolated_catalog):
    isolated_catalog.register_cdr(
        "my-cdr-yaml",
        description="test cdr",
        mode="yaml",
        cdr_forcing={"foo": "bar"},
    )
    assert "my-cdr-yaml" in isolated_catalog.cdr_names
    reloaded = isolated_catalog.cdr_data("my-cdr-yaml")
    assert reloaded["mode"] == "yaml"
    assert reloaded["cdr_forcing"] == {"foo": "bar"}
    assert reloaded["cdr_forcing_file"] is None
    assert reloaded["description"] == "test cdr"


def test_register_cdr_netcdf_mode_round_trip(isolated_catalog):
    file_ref = {"location": "/some/path/cdr.nc", "content_hash": "abc123"}
    isolated_catalog.register_cdr(
        "my-cdr-netcdf",
        mode="netcdf",
        cdr_forcing_file=file_ref,
    )
    assert "my-cdr-netcdf" in isolated_catalog.cdr_names
    reloaded = isolated_catalog.cdr_data("my-cdr-netcdf")
    assert reloaded["mode"] == "netcdf"
    assert reloaded["cdr_forcing_file"] == file_ref
    assert reloaded["cdr_forcing"] is None


def test_register_cdr_none_and_upscaled_modes(isolated_catalog):
    isolated_catalog.register_cdr("my-cdr-none", mode="none")
    isolated_catalog.register_cdr("my-cdr-upscaled", mode="upscaled")
    assert isolated_catalog.cdr_data("my-cdr-none")["cdr_forcing"] is None
    assert isolated_catalog.cdr_data("my-cdr-upscaled")["cdr_forcing_file"] is None


def test_register_cdr_invalid_mode_raises(isolated_catalog):
    with pytest.raises(ValueError, match="mode must be one of"):
        isolated_catalog.register_cdr("bad-mode", mode="bogus")


@pytest.mark.parametrize(
    "mode,kwargs",
    [
        ("none", {"cdr_forcing": {"foo": "bar"}}),
        ("none", {"cdr_forcing_file": {"location": "x", "content_hash": "y"}}),
        ("upscaled", {"cdr_forcing": {"foo": "bar"}}),
        ("simple", {}),
        ("yaml", {"cdr_forcing_file": {"location": "x", "content_hash": "y"}}),
        ("netcdf", {}),
        (
            "netcdf",
            {
                "cdr_forcing": {"foo": "bar"},
                "cdr_forcing_file": {"location": "x", "content_hash": "y"},
            },
        ),
    ],
)
def test_register_cdr_incoherent_mode_field_combo_raises(
    isolated_catalog, mode, kwargs
):
    with pytest.raises(ValueError):
        isolated_catalog.register_cdr(f"incoherent-{mode}", mode=mode, **kwargs)


def test_register_cdr_refuses_collision(isolated_catalog):
    isolated_catalog.register_cdr("dup-cdr", mode="none")
    with pytest.raises(FileExistsError):
        isolated_catalog.register_cdr("dup-cdr", mode="none")


def test_register_domain_from_dict_round_trips(isolated_catalog):
    from cstar.catalog.domain_catalog import default_catalog as _cat

    ddata = _cat.domain_data("wio-toy")
    isolated_catalog.register_domain_from_dict("my-domain", ddata)
    assert "my-domain" in isolated_catalog.domain_names
    assert isolated_catalog.domain_data("my-domain") == ddata
    assert (isolated_catalog.domain_path("my-domain") / "Assets").is_dir()


def test_register_model_from_settings_clones_code_block(isolated_catalog):
    from cstar.catalog.domain_catalog import default_catalog as _cat

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
    from cstar.catalog.domain_catalog import default_catalog as _cat

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


def test_register_model_from_settings_applies_marbl_ref(isolated_catalog):
    from cstar.catalog.domain_catalog import default_catalog as _cat

    base_dir = _cat.model_dir("cson_roms-marbl_v0.1")
    base_code = _cat.model_data("cson_roms-marbl_v0.1")["code"]
    isolated_catalog.register_model_from_settings(
        "my-model-marbl",
        {"param": {"nt_passive": 0}},
        base_dir,
        description="m",
        marbl_ref="my-marbl-tag",
    )
    data = isolated_catalog.model_data("my-model-marbl")
    assert data["code"]["marbl"]["commit"] == "my-marbl-tag"
    assert "branch" not in data["code"]["marbl"]
    assert data["code"]["marbl"]["location"] == base_code["marbl"]["location"]
    # roms untouched when only marbl_ref is overridden
    assert data["code"]["roms"] == base_code["roms"]


# ---------------------------------------------------------------------------
# LayeredCatalog (user layer over the read-only bundled layer) + user_catalog_root
# ---------------------------------------------------------------------------


def _write_domain(root: Path, name: str, **extra) -> None:
    """Write a minimal ``DomainSpec/<name>/Domain.yaml`` (+ empty Assets/) by hand,
    bypassing register_domain_from_dict so it can be written into a read-only-
    intended store before the store object exists.
    """
    d = root / "DomainSpec" / name
    (d / "Assets").mkdir(parents=True, exist_ok=True)
    data = {"grid_name": name, **extra}
    with (d / "Domain.yaml").open("w") as f:
        yaml.safe_dump(data, f)


class TestLayeredCatalog:
    """New coverage for the layered-catalog refactor (LayeredCatalog, DomainCatalog
    read_only/label, user_catalog_root, and the wizard's badge-aware dropdown
    options -- see cstar/catalog/domain_catalog.py and cstar/wizard/wizard.py).
    """

    # -- union reads, precedence, collisions ------------------------------

    def test_union_read_top_first_precedence_and_collision_logged(
        self, tmp_path, caplog
    ):
        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        bottom_root.mkdir()
        # Hand-built collision: both layers define "shared-domain", with
        # different content, so top-first precedence is actually observable.
        _write_domain(top_root, "shared-domain", description="from top")
        _write_domain(bottom_root, "shared-domain", description="from bottom")
        _write_domain(bottom_root, "bottom-only-domain")

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="top"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root,
            suppress_validation=True,
            read_only=True,
            label="bottom",
        )

        with caplog.at_level(logging.WARNING, logger="cstar.catalog.domain_catalog"):
            layered = LayeredCatalog([top, bottom])

        assert "domain:shared-domain" in caplog.text
        assert layered.domain_names == ["bottom-only-domain", "shared-domain"]
        # top-first precedence: reading the colliding name returns top's data.
        assert layered.domain_data("shared-domain")["description"] == "from top"
        assert layered.collisions() == {"domain:shared-domain": ["top", "bottom"]}

    def test_entry_source_and_unknown_key_error(self, tmp_path):
        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        bottom_root.mkdir()
        _write_domain(bottom_root, "bottom-domain")

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="top"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root,
            suppress_validation=True,
            read_only=True,
            label="bottom",
        )
        layered = LayeredCatalog([top, bottom])

        assert layered.entry_source("domain", "bottom-domain") == "bottom"
        with pytest.raises(KeyError):
            layered.entry_source("domain", "no-such-domain")

    # -- writers -----------------------------------------------------------

    def test_register_writes_into_top_store(self, tmp_path):
        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        shutil.copytree(_DEFAULT_CATALOG_ROOT, bottom_root)

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="top"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root, read_only=True, label="bundled"
        )
        layered = LayeredCatalog([top, bottom])

        layered.register_domain_from_dict("brand-new-domain", {"grid_name": "x"})
        assert "brand-new-domain" in top.domain_names
        assert "brand-new-domain" not in bottom.domain_names
        # Written into the TOP store's on-disk tree, not just its in-memory registry.
        assert (top_root / "DomainSpec" / "brand-new-domain" / "Domain.yaml").exists()
        assert not (
            bottom_root / "DomainSpec" / "brand-new-domain" / "Domain.yaml"
        ).exists()

    def test_register_collision_with_bottom_layer_raises_and_names_store(
        self, tmp_path
    ):
        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        shutil.copytree(_DEFAULT_CATALOG_ROOT, bottom_root)

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="top"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root, read_only=True, label="bundled"
        )
        layered = LayeredCatalog([top, bottom])

        existing_domain = bottom.domain_names[0]
        with pytest.raises(FileExistsError, match="bundled"):
            layered.register_domain_from_dict(existing_domain, {"grid_name": "x"})

    def test_register_cdr_writes_into_top_store(self, tmp_path):
        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        shutil.copytree(_DEFAULT_CATALOG_ROOT, bottom_root)

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="top"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root, read_only=True, label="bundled"
        )
        layered = LayeredCatalog([top, bottom])

        layered.register_cdr("brand-new-cdr", mode="none")
        assert "brand-new-cdr" in layered.cdr_names
        assert "brand-new-cdr" in top.cdr_names
        assert "brand-new-cdr" not in bottom.cdr_names
        assert (top_root / "CdrSpec" / "brand-new-cdr" / "Cdr.yaml").exists()
        assert layered.cdr_data("brand-new-cdr")["mode"] == "none"

    def test_register_cdr_collision_with_bottom_layer_raises_and_names_store(
        self, tmp_path
    ):
        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        shutil.copytree(_DEFAULT_CATALOG_ROOT, bottom_root)
        (bottom_root / "CdrSpec" / "shared-cdr").mkdir(parents=True)
        with (bottom_root / "CdrSpec" / "shared-cdr" / "Cdr.yaml").open("w") as f:
            yaml.safe_dump({"mode": "none"}, f)

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="top"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root, read_only=True, label="bundled"
        )
        layered = LayeredCatalog([top, bottom])

        with pytest.raises(FileExistsError, match="bundled"):
            layered.register_cdr("shared-cdr", mode="none")

    def test_cdr_union_read_top_first_precedence(self, tmp_path):
        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        shutil.copytree(_DEFAULT_CATALOG_ROOT, bottom_root)
        # Hand-write a bottom-layer-only CdrSpec entry (mirrors _write_domain's
        # bypass-the-writer approach for a store not yet writable).
        (bottom_root / "CdrSpec" / "from-bundled").mkdir(parents=True)
        with (bottom_root / "CdrSpec" / "from-bundled" / "Cdr.yaml").open("w") as f:
            yaml.safe_dump(
                {
                    "description": "",
                    "mode": "none",
                    "cdr_forcing": None,
                    "cdr_forcing_file": None,
                },
                f,
            )

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="top"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root, read_only=True, label="bundled"
        )
        layered = LayeredCatalog([top, bottom])
        layered.register_cdr("top-only-cdr", mode="upscaled")

        assert layered.cdr_names == ["from-bundled", "top-only-cdr"]
        # Union read resolves a bottom-layer-only entry through to its store.
        assert layered.cdr_data("from-bundled")["mode"] == "none"
        assert layered.entry_source("cdr", "from-bundled") == "bundled"
        assert layered.entry_source("cdr", "top-only-cdr") == "top"

    # -- read-only / non-local stores ---------------------------------------

    def test_read_only_store_mutators_raise_permission_error(self, tmp_path):
        root = tmp_path / "cat"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        cat = DomainCatalog(catalog_root=root, read_only=True)
        with pytest.raises(PermissionError):
            cat.register_output("my-output", {})
        with pytest.raises(PermissionError):
            cat.register_cdr("my-cdr", mode="none")

    def test_non_local_store_is_always_read_only(self):
        # Constructing a GitHub-backed store never hits the network here: fsspec's
        # filesystem() factory is mocked (mirroring test_github_catalog_uses_org_and_repo
        # above), so this only exercises the read_only-forcing logic, not fsspec/HTTP.
        url = "https://github.com/CWorthy-ocean/cstar-forge"
        with patch("cstar.catalog.domain_catalog.fsspec.filesystem") as mock_fs:
            instance = mock_fs.return_value
            instance.protocol = "github"
            instance.exists = lambda _path: False
            instance.ls = lambda _path, detail=False: []
            instance.glob = lambda _pattern: []
            cat = DomainCatalog(
                catalog_root=url, suppress_validation=True, read_only=False
            )
        assert cat.read_only is True

    def test_layered_catalog_rejects_read_only_top(self, tmp_path):
        root = tmp_path / "cat"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        read_only_top = DomainCatalog(catalog_root=root, read_only=True)
        with pytest.raises(ValueError, match="writable local catalog"):
            LayeredCatalog([read_only_top])

    # -- user_catalog_root ---------------------------------------------------

    def test_user_catalog_root_env_override_first_of_multi_entry(
        self, monkeypatch, tmp_path
    ):
        first = tmp_path / "first"
        second = tmp_path / "second"
        monkeypatch.setenv("CSTAR_CATALOG", os.pathsep.join([str(first), str(second)]))
        assert user_catalog_root() == first.expanduser().resolve()

    def test_user_catalog_root_default_is_home_anchored(self, monkeypatch, tmp_path):
        # conftest.py forces CSTAR_CATALOG globally for test isolation --
        # monkeypatch it away for this test only, never unset it globally.
        monkeypatch.delenv("CSTAR_CATALOG", raising=False)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        assert user_catalog_root() == tmp_path / "cstar" / "catalog"

    # -- laziness -------------------------------------------------------------

    def test_default_catalog_is_lazy_and_creates_nothing(self, tmp_path):
        nonexistent = tmp_path / "does-not-exist" / "catalog"
        env = dict(os.environ)
        env["CSTAR_CATALOG"] = str(nonexistent)
        code = (
            "import cstar.catalog.domain_catalog as dc\n"
            "assert dc._default_catalog is None\n"
            "import pathlib\n"
            f"assert not pathlib.Path({str(nonexistent)!r}).exists()\n"
            "print('OK')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout
        assert not nonexistent.exists()

    # -- forge_blueprint scanning on the bundled store -----------------------

    def test_forge_blueprint_names_finds_shipped_flat_blueprints(self):
        bundled = DomainCatalog(catalog_root=_DEFAULT_CATALOG_ROOT)
        expected = {
            "cson_roms-marbl_v0.1_wio-toy_10procs",
            "roms-marbl-0.3-default_wio-toy_10procs",
            "wio-toy-simple",
        }
        assert set(bundled.blueprint_names("forge")) >= expected
        assert set(bundled.forge_blueprint_names) >= expected
        path = bundled.forge_blueprint_path("wio-toy-simple")
        assert path.name == "wio-toy-simple.yaml"
        assert path.parent.name == "forge"
        assert path.exists()
        assert bundled.legacy_blueprints == []

    # -- wizard integration ---------------------------------------------------

    def test_wizard_default_blueprint_path_under_user_layer(self):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import ForgeBlueprintWizard

        wiz = ForgeBlueprintWizard()
        result = wiz._default_blueprint_path("some-name")
        expected_dir = Path(os.environ["CSTAR_CATALOG"]).expanduser().resolve()
        assert Path(result) == expected_dir / "blueprints" / "forge" / "some-name.yaml"

    def test_wizard_dd_options_mixed_badges_are_all_tuples(self, tmp_path):
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import ForgeBlueprintWizard

        top_root = tmp_path / "top"
        bottom_root = tmp_path / "bottom"
        top_root.mkdir()
        shutil.copytree(_DEFAULT_CATALOG_ROOT, bottom_root)
        _write_domain(top_root, "top-only-domain")

        top = DomainCatalog(
            catalog_root=top_root, suppress_validation=True, label="user"
        )
        bottom = DomainCatalog(
            catalog_root=bottom_root, read_only=True, label="bundled"
        )
        layered = LayeredCatalog([top, bottom])

        wiz = ForgeBlueprintWizard(catalog=layered)
        options = wiz._dd_options(layered.domain_names, "domain")
        # Some entries need a badge (bottom-sourced), so ipywidgets homogeneity
        # requires every entry -- including the un-badged top-only one -- to be
        # emitted as an explicit (label, value) tuple.
        assert all(isinstance(o, tuple) for o in options)
        label_by_value = {value: label for label, value in options}
        assert label_by_value["top-only-domain"] == "top-only-domain"  # no badge
        assert label_by_value["gulf-guinea-toy"] == "gulf-guinea-toy (bundled)"
        # dd_values recovers the bare names regardless of the tuple-badging.
        assert set(ForgeBlueprintWizard._dd_values(wiz.domain_dd)) >= {
            "top-only-domain",
            "gulf-guinea-toy",
        }

        # The real widget is built with prefix=["<custom>"] (see
        # ForgeBlueprintWizard.__init__): the mixed badge case is exactly the
        # scenario _dd_options's docstring warns about -- a str/tuple mix
        # would make ipywidgets silently store dd.value as a raw tuple -- so
        # confirm the sentinel is folded into the same homogeneous tuple list
        # and that setting dd.value to it still assigns the bare sentinel.
        assert all(isinstance(o, tuple) for o in wiz.domain_dd.options)
        assert ("<custom>", "<custom>") in wiz.domain_dd.options
        wiz.domain_dd.value = "<custom>"
        assert wiz.domain_dd.value == "<custom>"

    def test_wizard_dd_options_no_badges_are_plain_strings(self, tmp_path):
        """A single (non-layered) DomainCatalog has no ``entry_source`` -- every
        name's "badge" lookup is skipped, so ``_dd_options`` must fall back to
        plain strings (not homogeneous tuples), matching pre-layering behavior
        and staying compatible with a plain sentinel prefix.
        """
        pytest.importorskip("ipywidgets")
        from cstar.wizard.wizard import ForgeBlueprintWizard

        root = tmp_path / "cat"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        cat = DomainCatalog(catalog_root=root)
        assert not hasattr(cat, "entry_source")

        wiz = ForgeBlueprintWizard(catalog=cat)
        options = wiz._dd_options(cat.domain_names, "domain")
        assert all(isinstance(o, str) for o in options)
        assert all(isinstance(o, str) for o in wiz.domain_dd.options)


class TestReviewFixes:
    """Regression tests for the adversarial-review findings on the layered refactor."""

    def test_bundled_root_is_always_read_only(self):
        cat = DomainCatalog()  # packaged catalog, no read_only flag
        assert cat.read_only is True
        with pytest.raises(PermissionError):
            cat.register_output("review-fix-probe", {"x": 1})

    def test_user_catalog_root_ignores_empty_env_segments(self, monkeypatch, tmp_path):
        from cstar.catalog.domain_catalog import user_catalog_root

        monkeypatch.setenv("CSTAR_CATALOG", os.pathsep + str(tmp_path / "cat"))
        assert user_catalog_root() == (tmp_path / "cat").resolve()

    def test_user_catalog_root_rejects_local_top(self, monkeypatch):
        from cstar.catalog.domain_catalog import user_catalog_root

        monkeypatch.setenv("CSTAR_CATALOG", "local")
        with pytest.raises(ValueError, match="read-only"):
            user_catalog_root()

    def test_build_catalog_stack_rejects_local_top_and_appends_bundled(
        self, monkeypatch, tmp_path
    ):
        from cstar.catalog.domain_catalog import build_catalog_stack

        with pytest.raises(ValueError, match="read-only"):
            build_catalog_stack(["local"])

        stack = build_catalog_stack([str(tmp_path / "mine")])
        assert [s.label for s in stack.stores] == ["user", "bundled"]
        # Bundled entries visible through a hand-built stack, same as the env path.
        assert "wio-toy" in stack.domain_names

    def test_layered_copy_domain_into_standalone_and_uniqueness(
        self, monkeypatch, tmp_path
    ):
        from cstar.catalog.domain_catalog import build_catalog_stack

        stack = build_catalog_stack([str(tmp_path / "mine")])
        target = DomainCatalog(
            catalog_root=tmp_path / "other", suppress_validation=True
        )
        stack.copy_domain("wio-toy", target)
        assert "wio-toy" in target.domain_names
        # Copying into the stack itself under the same name would shadow the
        # bundled entry -- writers reject that stack-wide.
        with pytest.raises(FileExistsError, match="bundled"):
            stack.copy_domain("wio-toy", stack)


# ---------------------------------------------------------------------------
# blueprints/<application>/<name>.yaml scan, legacy layouts, layered reads
# ---------------------------------------------------------------------------
def _write_bp(path: Path, application: str = "roms_marbl") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"name: {path.stem}\napplication: {application}\n")
    return path


def _catalog(root: Path, **kwargs) -> DomainCatalog:
    root.mkdir(parents=True, exist_ok=True)
    return DomainCatalog(catalog_root=root, suppress_validation=True, **kwargs)


class TestBlueprintLayout:
    def test_application_directories_are_scanned(self, tmp_path, caplog):
        root = tmp_path / "cat"
        forge = _write_bp(root / "blueprints" / "forge" / "a.yaml", "forge")
        rm = _write_bp(root / "blueprints" / "roms_marbl" / "a.yaml")
        dotted = _write_bp(root / "blueprints" / "roms_marbl" / "v0.1.x.yml")

        with caplog.at_level(logging.WARNING, logger="cstar.catalog.domain_catalog"):
            catalog = _catalog(root)

        assert not caplog.records
        assert catalog.blueprint_applications == ["forge", "roms_marbl"]
        assert catalog.blueprint_names("forge") == ["a"]
        assert catalog.blueprint_names("roms_marbl") == ["a", "v0.1.x"]
        assert catalog.blueprint_names("no-such-app") == []
        assert catalog.blueprint_path("forge", "a").samefile(forge)
        assert catalog.blueprint_path("roms_marbl", "a").samefile(rm)
        assert catalog.blueprint_path("roms_marbl", "v0.1.x").samefile(dotted)
        assert catalog.legacy_blueprints == []
        assert catalog.blueprints_dir == root.resolve() / "blueprints"
        assert catalog.blueprint_dir("forge") == catalog.blueprints_dir / "forge"
        assert not (catalog.blueprints_dir / "other").exists()

    def test_wrappers_delegate(self, tmp_path):
        root = tmp_path / "cat"
        _write_bp(root / "blueprints" / "forge" / "f.yaml", "forge")
        rm = _write_bp(root / "blueprints" / "roms_marbl" / "r.yaml")
        catalog = _catalog(root)

        assert catalog.forge_blueprint_names == ["f"]
        assert catalog.roms_marbl_blueprint_names == ["r"]
        assert catalog.forge_blueprint_path("f") == catalog.blueprint_path("forge", "f")
        assert catalog.roms_marbl_blueprint_path("r").samefile(rm)
        # an int index returns the file, not a directory
        assert catalog.roms_marbl_blueprint(0).samefile(rm)
        assert catalog.roms_marbl_blueprint("r").samefile(rm)

    def test_blueprint_path_keyerror_lists_available(self, tmp_path):
        root = tmp_path / "cat"
        for n in ("a", "b", "c"):
            _write_bp(root / "blueprints" / "forge" / f"{n}.yaml", "forge")
        catalog = _catalog(root)

        with pytest.raises(KeyError) as exc:
            catalog.blueprint_path("forge", "zzz")
        msg = str(exc.value)
        assert "Blueprint 'zzz' for application 'forge' not found in catalog at" in msg
        assert "Available: a, b, c" in msg

    def test_legacy_layouts_are_readable_with_one_collapsed_warning(
        self, tmp_path, caplog
    ):
        root = tmp_path / "cat"
        flat_forge = _write_bp(
            root / "blueprints" / "old.forge_blueprint.yaml", "forge"
        )
        flat_rm = _write_bp(root / "blueprints" / "B_flat.yaml")
        nested = _write_bp(
            root / "blueprints" / "some-machine" / "nested" / "B_nested.yaml"
        )
        _write_bp(root / "blueprints" / "forge" / "new.yaml", "forge")

        with caplog.at_level(logging.WARNING, logger="cstar.catalog.domain_catalog"):
            catalog = _catalog(root)

        legacy = {(a, n): p for a, n, p in catalog.legacy_blueprints}
        assert set(legacy) == {
            ("forge", "old"),
            ("roms_marbl", "flat"),
            ("roms_marbl", "nested"),
        }
        assert legacy[("forge", "old")].samefile(flat_forge)
        assert legacy[("roms_marbl", "flat")].samefile(flat_rm)
        assert legacy[("roms_marbl", "nested")].samefile(nested)
        # the public API reads every entry, new layout and legacy alike
        assert catalog.blueprint_names("forge") == ["new", "old"]
        assert catalog.roms_marbl_blueprint_names == ["flat", "nested"]
        assert catalog.forge_blueprint_path("old").samefile(flat_forge)
        assert catalog.roms_marbl_blueprint("nested").samefile(nested)
        assert any(
            f.samefile(nested) for f in catalog._find_roms_marbl_blueprint_files()
        )
        # a returned list is a copy
        catalog.legacy_blueprints.clear()
        assert len(catalog.legacy_blueprints) == 3

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        text = warnings[0].getMessage()
        for entry in ("forge/old", "roms_marbl/flat", "roms_marbl/nested"):
            assert entry in text
        assert f"cstar admin migrate-catalog {catalog.catalog_root}" in text
        assert "blueprints/<application>/<name>.yaml" in text

    def test_current_layout_shadows_legacy_entry(self, tmp_path, caplog):
        root = tmp_path / "cat"
        new = _write_bp(root / "blueprints" / "forge" / "same.yaml", "forge")
        _write_bp(root / "blueprints" / "same.forge_blueprint.yaml", "forge")

        with caplog.at_level(logging.DEBUG, logger="cstar.catalog.domain_catalog"):
            catalog = _catalog(root)

        assert catalog.blueprint_path("forge", "same").samefile(new)
        assert catalog.legacy_blueprints == []
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert "shadowed" in caplog.text

    def test_flat_legacy_file_wins_and_loser_is_still_recorded(self, tmp_path, caplog):
        root = tmp_path / "cat"
        nested = _write_bp(root / "blueprints" / "m" / "dup" / "B_dup.yaml")
        flat = _write_bp(root / "blueprints" / "B_dup.yaml")
        other = _write_bp(root / "blueprints" / "m" / "zed" / "B_zed.yaml")

        with caplog.at_level(logging.WARNING, logger="cstar.catalog.domain_catalog"):
            catalog = _catalog(root)

        assert catalog.roms_marbl_blueprint_path("dup").samefile(flat)
        legacy = catalog.legacy_blueprints
        # winners first (flat dup, per-machine zed), then the loser
        assert [(a, n) for a, n, _ in legacy] == [
            ("roms_marbl", "dup"),
            ("roms_marbl", "zed"),
            ("roms_marbl", "dup"),
        ]
        assert legacy[0][2].samefile(flat)
        assert legacy[1][2].samefile(other)
        assert legacy[2][2].samefile(nested)
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert str(nested) in warnings[0].getMessage()
        assert "ignored: duplicate" in warnings[0].getMessage()

    def test_hidden_directories_and_files_are_skipped(self, tmp_path, caplog):
        root = tmp_path / "cat"
        _write_bp(root / "blueprints" / ".ipynb_checkpoints" / "x-checkpoint.yaml")
        _write_bp(root / "blueprints" / "forge" / ".hidden.yaml", "forge")
        _write_bp(root / "blueprints" / "forge" / "real.yaml", "forge")
        _write_bp(root / "blueprints" / ".B_hidden.yaml")
        _write_bp(root / "blueprints" / "m" / ".ipynb_checkpoints" / "B_c.yaml")
        _write_bp(root / "blueprints" / "m" / "n" / ".B_h.yaml")

        with caplog.at_level(logging.WARNING, logger="cstar.catalog.domain_catalog"):
            catalog = _catalog(root)

        assert catalog.blueprint_applications == ["forge"]
        assert catalog.blueprint_names("forge") == ["real"]
        assert catalog.legacy_blueprints == []
        assert not caplog.records

    def test_directory_classification(self, tmp_path, caplog):
        root = tmp_path / "cat"
        bp = root / "blueprints"
        (bp / "empty").mkdir(parents=True)
        # a stray sidecar beside <name>/ dirs does not make an application
        _write_bp(bp / "machine" / "_grid.yaml")
        nested = _write_bp(bp / "machine" / "n1" / "B_n1.yaml")

        with caplog.at_level(logging.DEBUG, logger="cstar.catalog.domain_catalog"):
            catalog = _catalog(root)

        assert catalog.blueprint_applications == ["roms_marbl"]
        assert catalog.blueprint_names("machine") == []
        assert catalog.roms_marbl_blueprint_path("n1").samefile(nested)
        assert "Ignoring files in legacy machine directory" in caplog.text
        assert "_grid.yaml" in caplog.text
        assert catalog.blueprint_names("empty") == []

    def test_every_file_in_a_legacy_name_directory_is_registered(self, tmp_path):
        root = tmp_path / "cat"
        one = _write_bp(root / "blueprints" / "m" / "single" / "B_whatever.yaml")
        a = _write_bp(root / "blueprints" / "m" / "multi" / "B_a.yaml")
        b = _write_bp(root / "blueprints" / "m" / "multi" / "B_b.yml")
        _write_bp(root / "blueprints" / "m" / "multi" / "settings_B_a.yaml")

        catalog = _catalog(root)

        # one file: named after its directory; several: each by its stem minus B_
        assert catalog.roms_marbl_blueprint_names == ["a", "b", "single"]
        assert catalog.roms_marbl_blueprint_path("single").samefile(one)
        assert catalog.roms_marbl_blueprint_path("a").samefile(a)
        assert catalog.roms_marbl_blueprint_path("b").samefile(b)
        assert len(catalog.legacy_blueprints) == 3

    def test_yaml_wins_over_yml_for_the_same_stem(self, tmp_path):
        root = tmp_path / "cat"
        _write_bp(root / "blueprints" / "forge" / "x.yml", "forge")
        y = _write_bp(root / "blueprints" / "forge" / "x.yaml", "forge")

        catalog = _catalog(root)

        assert catalog.blueprint_names("forge") == ["x"]
        assert catalog.blueprint_path("forge", "x").samefile(y)

    def test_one_failing_directory_does_not_abort_its_siblings(
        self, tmp_path, monkeypatch, caplog
    ):
        root = tmp_path / "cat"
        _write_bp(root / "blueprints" / "bad" / "x.yaml", "bad")
        _write_bp(root / "blueprints" / "forge" / "ok.yaml", "forge")
        _write_bp(root / "blueprints" / "m" / "n" / "B_n.yaml")
        _write_bp(root / "blueprints" / "m" / "boom" / "B_boom.yaml")
        _write_bp(root / "blueprints" / "B_flat.yaml")
        real = DomainCatalog._fs_list

        def flaky(self, path):
            if path.name in ("bad", "boom"):
                raise OSError("simulated listing failure")
            return real(self, path)

        monkeypatch.setattr(DomainCatalog, "_fs_list", flaky)
        with caplog.at_level(logging.WARNING, logger="cstar.catalog.domain_catalog"):
            catalog = _catalog(root)

        assert catalog.blueprint_names("forge") == ["ok"]
        assert catalog.blueprint_names("bad") == []
        assert catalog.roms_marbl_blueprint_names == ["flat", "n"]
        failures = [
            r.getMessage() for r in caplog.records if "Failed to scan" in r.getMessage()
        ]
        assert len(failures) == 2

    def test_each_directory_is_listed_once(self, tmp_path, monkeypatch):
        root = tmp_path / "cat"
        _write_bp(root / "blueprints" / "forge" / "a.yaml", "forge")
        _write_bp(root / "blueprints" / "m" / "n" / "B_n.yaml")
        _write_bp(root / "blueprints" / "B_flat.yaml")
        calls: list[str] = []
        real = DomainCatalog._fs_list

        def counting(self, path):
            calls.append(str(path))
            return real(self, path)

        monkeypatch.setattr(DomainCatalog, "_fs_list", counting)
        _catalog(root)

        assert len(calls) == len(set(calls))
        assert len(calls) == 4  # blueprints/, forge/, m/, m/n/


class TestLayeredBlueprints:
    def _stack(self, tmp_path) -> LayeredCatalog:
        top_root, bottom_root = tmp_path / "top", tmp_path / "bottom"
        _write_bp(top_root / "blueprints" / "forge" / "shared.yaml", "forge")
        _write_bp(top_root / "blueprints" / "forge" / "top-only.yaml", "forge")
        _write_bp(bottom_root / "blueprints" / "forge" / "shared.yaml", "forge")
        _write_bp(bottom_root / "blueprints" / "roms_marbl" / "low.yaml")
        top = _catalog(top_root, label="top")
        bottom = _catalog(bottom_root, read_only=True, label="bottom")
        return LayeredCatalog([top, bottom])

    def test_union_across_stores(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="cstar.catalog.domain_catalog"):
            layered = self._stack(tmp_path)

        assert layered.blueprint_applications == ["forge", "roms_marbl"]
        assert layered.blueprint_names("forge") == ["shared", "top-only"]
        assert layered.forge_blueprint_names == ["shared", "top-only"]
        assert layered.roms_marbl_blueprint_names == ["low"]
        assert layered.blueprint_names("nope") == []
        assert layered.blueprints_dir == layered.top.blueprints_dir
        assert layered.blueprint_dir("forge") == layered.top.blueprint_dir("forge")
        # top-first precedence on the colliding name
        assert layered.blueprint_path("forge", "shared").samefile(
            layered.top.blueprint_path("forge", "shared")
        )
        assert layered.forge_blueprint_path("top-only").samefile(
            layered.top.blueprint_path("forge", "top-only")
        )
        assert layered.roms_marbl_blueprint(0).samefile(
            layered.stores[1].blueprint_path("roms_marbl", "low")
        )
        assert "forge_blueprint:shared" in caplog.text

    def test_collisions_and_entry_source(self, tmp_path):
        layered = self._stack(tmp_path)

        assert layered.collisions() == {"forge_blueprint:shared": ["top", "bottom"]}
        assert layered.entry_source("forge_blueprint", "shared") == "top"
        assert layered.entry_source("forge_blueprint", "top-only") == "top"
        assert layered.entry_source("roms_marbl_blueprint", "low") == "bottom"
        with pytest.raises(KeyError, match=r"Blueprint \(forge\) 'nope'"):
            layered.entry_source("forge_blueprint", "nope")
        with pytest.raises(KeyError, match="not found in any catalog layer"):
            layered.blueprint_path("forge", "nope")

    def test_check_unique_is_stack_wide_for_blueprints(self, tmp_path):
        layered = self._stack(tmp_path)

        with pytest.raises(FileExistsError, match="bottom"):
            layered._check_unique("roms_marbl_blueprint", "low")
        layered._check_unique("roms_marbl_blueprint", "fresh")

    def test_legacy_blueprints_concatenate_over_stores(self, tmp_path):
        top_root, bottom_root = tmp_path / "top", tmp_path / "bottom"
        _write_bp(top_root / "blueprints" / "B_x.yaml")
        _write_bp(bottom_root / "blueprints" / "y.forge_blueprint.yaml", "forge")
        layered = LayeredCatalog(
            [_catalog(top_root, label="top"), _catalog(bottom_root, read_only=True)]
        )

        assert [(a, n) for a, n, _ in layered.legacy_blueprints] == [
            ("roms_marbl", "x"),
            ("forge", "y"),
        ]

    def test_union_names_are_sorted_like_the_other_unions(self, tmp_path):
        top_root, bottom_root = tmp_path / "top", tmp_path / "bottom"
        for n in ("m", "b"):
            _write_bp(top_root / "blueprints" / "forge" / f"{n}.yaml", "forge")
        for n in ("z", "a", "m"):
            _write_bp(bottom_root / "blueprints" / "forge" / f"{n}.yaml", "forge")
        layered = LayeredCatalog(
            [_catalog(top_root, label="top"), _catalog(bottom_root, read_only=True)]
        )

        # sorted and de-duplicated, not top-first-then-bottom
        assert layered.blueprint_names("forge") == ["a", "b", "m", "z"]
        assert layered.forge_blueprint_names == layered.blueprint_names("forge")


class TestCatalogBarBlueprintCount:
    def _bar(self):
        W = pytest.importorskip("ipywidgets")
        from cstar.wizard.ui.catalog_bar import CatalogBar

        return CatalogBar(W, on_reload=lambda _text: None)

    def test_status_counts_blueprints_across_applications(self, tmp_path):
        root = tmp_path / "cat"
        _write_bp(root / "blueprints" / "forge" / "f1.yaml", "forge")
        _write_bp(root / "blueprints" / "forge" / "f2.yaml", "forge")
        _write_bp(root / "blueprints" / "roms_marbl" / "r1.yaml")
        catalog = _catalog(root)

        bar = self._bar()
        bar.set_status_for(catalog)
        assert "3 blueprints" in bar._cat_status.value

    def test_layered_status_counts_the_union(self, tmp_path):
        top_root, bottom_root = tmp_path / "top", tmp_path / "bottom"
        _write_bp(top_root / "blueprints" / "forge" / "a.yaml", "forge")
        _write_bp(bottom_root / "blueprints" / "forge" / "a.yaml", "forge")
        _write_bp(bottom_root / "blueprints" / "roms_marbl" / "b.yaml")
        layered = LayeredCatalog(
            [_catalog(top_root, label="top"), _catalog(bottom_root, read_only=True)]
        )

        bar = self._bar()
        bar.set_status_for(layered)
        assert "2 blueprints" in bar._cat_status.value


def test_bundled_catalog_ships_the_wales_toy_roms_marbl_blueprint():
    """The bundled layer carries one ROMS-MARBL blueprint, `wales-toy`, as a file."""
    bundled = DomainCatalog(read_only=True)
    assert "wales-toy" in bundled.blueprint_names("roms_marbl")
    path = bundled.blueprint_path("roms_marbl", "wales-toy")
    assert path.name == "wales-toy.yaml" and path.is_file()
    assert "roms_marbl" in bundled.blueprint_applications


def test_wales_tutorial_blueprint_matches_the_bundled_copy():
    """docs/tutorials/wales_toy_blueprint.yaml is the bundled entry, byte for byte."""
    import cstar

    repo_root = Path(cstar.__file__).resolve().parents[1]
    tutorial = repo_root / "docs" / "tutorials" / "wales_toy_blueprint.yaml"
    bundled = DomainCatalog(read_only=True).blueprint_path("roms_marbl", "wales-toy")
    assert tutorial.read_text() == bundled.read_text()
