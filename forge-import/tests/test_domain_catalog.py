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

from cstar_forge.domain_catalog import (
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
# register_model_from_settings -- the "save modified specs to catalog" writers
# ---------------------------------------------------------------------------
@pytest.fixture
def isolated_catalog(tmp_path):
    import shutil

    from cstar_forge.domain_catalog import _DEFAULT_CATALOG_ROOT

    root = tmp_path / "catalog"
    # Copy the BUNDLED catalog (not default_catalog.catalog_root, which is now
    # the writable *user* layer -- empty/nonexistent in tests, see conftest.py).
    shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
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
    options -- see cstar_forge/domain_catalog.py and forge_blueprint_wizard.py).
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

        with caplog.at_level(logging.WARNING, logger="cstar_forge.domain_catalog"):
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

    # -- read-only / non-local stores ---------------------------------------

    def test_read_only_store_mutators_raise_permission_error(self, tmp_path):
        root = tmp_path / "cat"
        shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
        cat = DomainCatalog(catalog_root=root, read_only=True)
        with pytest.raises(PermissionError):
            cat.register_output("my-output", {})

    def test_non_local_store_is_always_read_only(self):
        # Constructing a GitHub-backed store never hits the network here: fsspec's
        # filesystem() factory is mocked (mirroring test_github_catalog_uses_org_and_repo
        # above), so this only exercises the read_only-forcing logic, not fsspec/HTTP.
        url = "https://github.com/CWorthy-ocean/cstar-forge"
        with patch("cstar_forge.domain_catalog.fsspec.filesystem") as mock_fs:
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
        monkeypatch.setenv(
            "CSTAR_FORGE_CATALOG", os.pathsep.join([str(first), str(second)])
        )
        assert user_catalog_root() == first.expanduser().resolve()

    def test_user_catalog_root_default_is_home_anchored(self, monkeypatch, tmp_path):
        # conftest.py forces CSTAR_FORGE_CATALOG globally for test isolation --
        # monkeypatch it away for this test only, never unset it globally.
        monkeypatch.delenv("CSTAR_FORGE_CATALOG", raising=False)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        assert user_catalog_root() == tmp_path / "cstar-forge-data" / "catalog"

    # -- laziness -------------------------------------------------------------

    def test_default_catalog_is_lazy_and_creates_nothing(self, tmp_path):
        nonexistent = tmp_path / "does-not-exist" / "catalog"
        env = dict(os.environ)
        env["CSTAR_FORGE_CATALOG"] = str(nonexistent)
        code = (
            "import cstar_forge.domain_catalog as dc\n"
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
        assert set(bundled.forge_blueprint_names) >= {
            "cson_roms-marbl_v0.1_wio-toy_10procs",
            "roms-marbl-0.3-default_wio-toy_10procs",
            "wio-toy-simple",
        }
        path = bundled.forge_blueprint_path("wio-toy-simple")
        assert path.name == "wio-toy-simple.forge_blueprint.yaml"
        assert path.exists()

    # -- wizard integration ---------------------------------------------------

    def test_wizard_default_blueprint_path_under_user_layer(self):
        pytest.importorskip("ipywidgets")
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

        wiz = ForgeBlueprintWizard()
        result = wiz._default_blueprint_path("some-name")
        expected_dir = Path(os.environ["CSTAR_FORGE_CATALOG"]).expanduser().resolve()
        assert (
            Path(result)
            == expected_dir / "blueprints" / "some-name.forge_blueprint.yaml"
        )

    def test_wizard_dd_options_mixed_badges_are_all_tuples(self, tmp_path):
        pytest.importorskip("ipywidgets")
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

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
        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard

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
        from cstar_forge.domain_catalog import user_catalog_root

        monkeypatch.setenv("CSTAR_FORGE_CATALOG", os.pathsep + str(tmp_path / "cat"))
        assert user_catalog_root() == (tmp_path / "cat").resolve()

    def test_user_catalog_root_rejects_local_top(self, monkeypatch):
        from cstar_forge.domain_catalog import user_catalog_root

        monkeypatch.setenv("CSTAR_FORGE_CATALOG", "local")
        with pytest.raises(ValueError, match="read-only"):
            user_catalog_root()

    def test_build_catalog_stack_rejects_local_top_and_appends_bundled(
        self, monkeypatch, tmp_path
    ):
        from cstar_forge.domain_catalog import build_catalog_stack

        with pytest.raises(ValueError, match="read-only"):
            build_catalog_stack(["local"])

        stack = build_catalog_stack([str(tmp_path / "mine")])
        assert [s.label for s in stack.stores] == ["user", "bundled"]
        # Bundled entries visible through a hand-built stack, same as the env path.
        assert "wio-toy" in stack.domain_names

    def test_blueprint_shim_works_on_layered_default(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CSTAR_FORGE_CATALOG", str(tmp_path / "user-layer"))
        import cstar_forge.catalog as catalog_shim

        bp = catalog_shim.BlueprintCatalog()
        files = bp.find_blueprint_files()
        assert isinstance(files, list)
        df = bp.blueprintDF()
        assert hasattr(df, "empty")
        assert bp._extract_model_and_grid_name(
            "cson_roms-marbl_v0.1_wio-toy_10procs"
        ) == (
            "cson_roms-marbl_v0.1",
            "wio-toy",
        )

    def test_layered_copy_domain_into_standalone_and_uniqueness(
        self, monkeypatch, tmp_path
    ):
        from cstar_forge.domain_catalog import build_catalog_stack

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

    def test_layered_path_helpers_delegate_to_top(self, tmp_path):
        from cstar_forge.domain_catalog import build_catalog_stack

        stack = build_catalog_stack([str(tmp_path / "mine")])
        d = stack.roms_marbl_blueprint_dir_for("MacOS", "bp1")
        assert str(d).startswith(str((tmp_path / "mine").resolve()))
        b = stack.build_dir_for("MacOS", "bp1")
        assert b.name == "Build"
