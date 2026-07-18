"""
Comprehensive tests for the models.py module.

Tests cover:
- OpenBoundaries (re-exported from forge.forge_blueprint)
- ModelTemplates / ModelCode (the ModelSpec's code+template-ref shape)
- ModelSpec validation (cross-ref + template-file-existence validators)
- load_models_yaml (the heavy Pydantic ModelSpec loader)
- Edge cases and error handling

ModelSpec was consolidated into a single YAML (code + flat model_settings, no more
inputs/split templates-settings/placeholder code repos) -- see
docs/developer-guide.md and cstar_forge/models.py's module docstring.
"""

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from cstar_forge.forge.forge_blueprint import CodeRepo
from cstar_forge.models import (
    ModelCode,
    ModelSpec,
    ModelTemplates,
    OpenBoundaries,
    load_models_yaml,
)


class TestOpenBoundaries:
    """Tests for OpenBoundaries class."""

    def test_openboundaries_defaults(self):
        """Test OpenBoundaries with default values."""
        boundaries = OpenBoundaries()
        assert boundaries.north is False
        assert boundaries.south is False
        assert boundaries.east is False
        assert boundaries.west is False

    def test_openboundaries_all_true(self):
        """Test OpenBoundaries with all boundaries open."""
        boundaries = OpenBoundaries(north=True, south=True, east=True, west=True)
        assert boundaries.north is True
        assert boundaries.south is True
        assert boundaries.east is True
        assert boundaries.west is True

    def test_openboundaries_partial(self):
        """Test OpenBoundaries with some boundaries open."""
        boundaries = OpenBoundaries(north=True, east=True)
        assert boundaries.north is True
        assert boundaries.south is False
        assert boundaries.east is True
        assert boundaries.west is False

    def test_openboundaries_model_dump(self):
        """Test OpenBoundaries serialization."""
        boundaries = OpenBoundaries(north=True, south=False, east=True, west=False)
        dumped = boundaries.model_dump()
        assert dumped["north"] is True
        assert dumped["south"] is False
        assert dumped["east"] is True
        assert dumped["west"] is False


def _roms_repo() -> CodeRepo:
    return CodeRepo(location="https://github.com/test/roms.git", commit="0.1.0")


def _templates(directory="templates/compile-time", files=("cppdefs.opt.j2",)):
    return ModelTemplates(directory=directory, files=list(files))


class TestModelTemplates:
    """Tests for the ModelTemplates class (directory+files, no location)."""

    def test_creation(self):
        t = ModelTemplates(directory="templates/run-time", files=["marbl_in"])
        assert t.directory == "templates/run-time"
        assert t.files == ["marbl_in"]

    def test_files_default_empty(self):
        t = ModelTemplates(directory="templates/run-time")
        assert t.files == []

    def test_missing_directory_raises(self):
        with pytest.raises(ValidationError):
            ModelTemplates(files=["marbl_in"])

    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            ModelTemplates(directory="templates/run-time", location="not allowed")


class TestModelCode:
    """Tests for the ModelCode class (roms/marbl/pio + template refs)."""

    def test_creation_full(self):
        code = ModelCode(
            roms=_roms_repo(),
            marbl=CodeRepo(location="https://github.com/test/marbl.git", commit="x"),
            pio=CodeRepo(location="https://github.com/test/pio.git", branch="main"),
            templates_commit="abc123",
            templates_compile_time=_templates(),
            templates_run_time=_templates("templates/run-time", ["marbl_in"]),
        )
        assert code.roms.location == "https://github.com/test/roms.git"
        assert code.marbl.commit == "x"
        assert code.pio.branch == "main"
        assert code.templates_commit == "abc123"

    def test_marbl_and_pio_optional(self):
        code = ModelCode(
            roms=_roms_repo(),
            templates_compile_time=_templates(),
            templates_run_time=_templates("templates/run-time", ["marbl_in"]),
        )
        assert code.marbl is None
        assert code.pio is None

    def test_missing_roms_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            ModelCode(
                templates_compile_time=_templates(),
                templates_run_time=_templates("templates/run-time", ["marbl_in"]),
            )
        assert "roms" in str(exc_info.value).lower()

    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            ModelCode(
                roms=_roms_repo(),
                templates_compile_time=_templates(),
                templates_run_time=_templates("templates/run-time", ["marbl_in"]),
                bogus="not allowed",
            )


def _model_code(**overrides):
    kw = dict(
        roms=_roms_repo(),
        templates_compile_time=_templates(),
        templates_run_time=_templates("templates/run-time", ["marbl_in"]),
    )
    kw.update(overrides)
    return ModelCode(**kw)


class TestModelSpec:
    """Tests for the ModelSpec class: name + code + flat model_settings."""

    def test_creation_minimal(self):
        spec = ModelSpec(
            name="test_model",
            code=_model_code(),
            model_settings={"cppdefs": {"marbl": True}},
        )
        assert spec.name == "test_model"
        assert spec.model_settings["cppdefs"]["marbl"] is True

    def test_model_settings_defaults_empty(self):
        spec = ModelSpec(
            name="test_model",
            code=_model_code(templates_compile_time=_templates(files=[])),
        )
        assert spec.model_settings == {}

    def test_use_pio_defaults_false(self):
        """use_pio mirrors bgc_mode: a per-run build toggle defaulting off."""
        spec = ModelSpec(
            name="test_model",
            code=_model_code(),
            model_settings={"cppdefs": {"marbl": True}},
        )
        assert spec.use_pio is False

    def test_use_pio_explicit_true(self):
        spec = ModelSpec(
            name="test_model",
            code=_model_code(),
            use_pio=True,
            model_settings={"cppdefs": {"marbl": True}},
        )
        assert spec.use_pio is True

    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            ModelSpec(
                name="test_model",
                code=_model_code(),
                model_settings={},
                inputs={"grid": {}},
            )

    def test_cross_validation_missing_settings_key_raises(self):
        """A .j2 compile-time template file needs a matching model_settings key."""
        with pytest.raises(ValidationError) as exc_info:
            ModelSpec(
                name="test_model",
                code=_model_code(
                    templates_compile_time=_templates(files=["cppdefs.opt.j2"])
                ),
                model_settings={},  # missing "cppdefs"
            )
        assert "cppdefs" in str(exc_info.value).lower()

    def test_cross_validation_passes_with_matching_key(self):
        spec = ModelSpec(
            name="test_model",
            code=_model_code(
                templates_compile_time=_templates(files=["cppdefs.opt.j2"])
            ),
            model_settings={"cppdefs": {}},
        )
        assert "cppdefs" in spec.model_settings

    def test_cross_validation_skips_non_j2_files(self):
        """Non-.j2 files (e.g. a copied-verbatim file) aren't cross-checked. Uses a
        directory that doesn't exist at the repo root so the (separate) file-
        existence validator is a no-op and this test isolates the cross-ref check.
        """
        spec = ModelSpec(
            name="test_model",
            code=_model_code(
                templates_compile_time=ModelTemplates(
                    directory="templates/does-not-exist",
                    files=["cppdefs.opt.j2", "Makefile"],
                )
            ),
            model_settings={"cppdefs": {}},
        )
        assert spec.model_settings == {"cppdefs": {}}

    def test_template_files_exist_against_real_repo(self):
        """The bundled catalog's real templates/compile-time/run-time files exist at
        the forge repo root -- this must pass with no exception.
        """
        spec = ModelSpec(
            name="cson_roms-marbl_v0.1",
            code=_model_code(),
            model_settings={"cppdefs": {}},
        )
        assert spec.code.templates_compile_time.files == ["cppdefs.opt.j2"]

    def test_template_files_exist_missing_file_raises(self):
        """A nonexistent file in a directory that *does* exist at the repo root
        (templates/compile-time is real in this checkout) must raise.
        """
        with pytest.raises(FileNotFoundError) as exc_info:
            ModelSpec(
                name="test_model",
                code=_model_code(
                    templates_compile_time=ModelTemplates(
                        directory="templates/compile-time",
                        files=["cppdefs.opt.j2", "does_not_exist.j2"],
                    )
                ),
                model_settings={"cppdefs": {}, "does_not_exist": {}},
            )
        assert "does_not_exist.j2" in str(exc_info.value)

    def test_template_files_exist_skipped_when_directory_not_found(self):
        """A stage directory that doesn't exist at the repo root is silently
        skipped (best-effort check), not treated as an error.
        """
        spec = ModelSpec(
            name="test_model",
            code=_model_code(
                templates_compile_time=ModelTemplates(
                    directory="templates/this-directory-does-not-exist",
                    files=["whatever.j2"],
                )
            ),
            model_settings={"whatever": {}},
        )
        assert spec.code.templates_compile_time.directory == (
            "templates/this-directory-does-not-exist"
        )


class TestLoadModelsYaml:
    """Tests for load_models_yaml (parses a model.yml into a ModelSpec)."""

    def _write(self, tmp_path, content, filename="model.yml"):
        path = tmp_path / filename
        path.write_text(yaml.safe_dump(content))
        return path

    def test_load_multi_model_file_minimal(self, tmp_path):
        yaml_content = {
            "test_model": {
                "code": {
                    "roms": {
                        "location": "https://github.com/test/roms.git",
                        "commit": "x",
                    },
                    "marbl": {
                        "location": "https://github.com/test/marbl.git",
                        "commit": "y",
                    },
                    "templates_compile_time": {
                        "directory": "templates/compile-time",
                        "files": ["cppdefs.opt.j2"],
                    },
                    "templates_run_time": {
                        "directory": "templates/run-time",
                        "files": ["marbl_in"],
                    },
                },
                "model_settings": {"cppdefs": {"marbl": True}},
            }
        }
        yaml_path = self._write(tmp_path, yaml_content)
        spec = load_models_yaml(yaml_path, "test_model")

        assert spec.name == "test_model"
        assert spec.code.roms.commit == "x"
        assert spec.code.marbl.commit == "y"
        assert spec.model_settings == {"cppdefs": {"marbl": True}}

    def test_load_single_model_file_format(self, tmp_path):
        """A single-model file has code/model_settings at the top level; the
        filename/directory (passed as model_name) is the logical model name.
        """
        yaml_content = {
            "code": {
                "roms": {
                    "location": "https://github.com/test/roms.git",
                    "branch": "main",
                },
                "templates_compile_time": {
                    "directory": "templates/compile-time",
                    "files": ["cppdefs.opt.j2"],
                },
                "templates_run_time": {
                    "directory": "templates/run-time",
                    "files": ["marbl_in"],
                },
            },
            "model_settings": {"cppdefs": {}},
        }
        yaml_path = self._write(tmp_path, yaml_content)
        spec = load_models_yaml(yaml_path, "my_model")

        assert spec.name == "my_model"
        assert spec.code.roms.branch == "main"

    def test_load_use_pio_round_trips(self, tmp_path):
        """A top-level use_pio key on model.yml round-trips onto ModelSpec.use_pio
        (mirrors bgc_mode), and is False by default when absent.
        """
        base_yaml = {
            "code": {
                "roms": {
                    "location": "https://github.com/test/roms.git",
                    "commit": "x",
                },
                "pio": {
                    "location": "https://github.com/NCAR/ParallelIO.git",
                    "commit": "pio2_7_0",
                },
                "templates_compile_time": {
                    "directory": "templates/compile-time",
                    "files": ["cppdefs.opt.j2"],
                },
                "templates_run_time": {
                    "directory": "templates/run-time",
                    "files": ["marbl_in"],
                },
            },
            "model_settings": {"cppdefs": {}},
        }

        no_pio_path = self._write(tmp_path, base_yaml, filename="no_pio.yml")
        assert load_models_yaml(no_pio_path, "my_model").use_pio is False

        with_pio = {**base_yaml, "use_pio": True}
        with_pio_path = self._write(tmp_path, with_pio, filename="with_pio.yml")
        assert load_models_yaml(with_pio_path, "my_model").use_pio is True

    def test_load_numeric_commit_coerced(self, tmp_path):
        """A commit value written as a bare int in YAML is still accepted (Pydantic
        coerces it to str on the CodeRepo field).
        """
        yaml_content = {
            "code": {
                "roms": {
                    "location": "https://github.com/test/roms.git",
                    "commit": 6588486,
                },
                "templates_compile_time": {
                    "directory": "templates/compile-time",
                    "files": ["cppdefs.opt.j2"],
                },
                "templates_run_time": {
                    "directory": "templates/run-time",
                    "files": ["marbl_in"],
                },
            },
            "model_settings": {"cppdefs": {}},
        }
        yaml_path = self._write(tmp_path, yaml_content)
        spec = load_models_yaml(yaml_path, "my_model")
        assert spec.code.roms.commit == "6588486"

    def test_load_missing_model_raises_keyerror(self, tmp_path):
        yaml_path = self._write(tmp_path, {"other_model": {}})
        with pytest.raises(KeyError) as exc_info:
            load_models_yaml(yaml_path, "test_model")
        assert "test_model" in str(exc_info.value)

    def test_load_missing_code_raises_valueerror(self, tmp_path):
        yaml_path = self._write(tmp_path, {"test_model": {"model_settings": {}}})
        with pytest.raises(ValueError) as exc_info:
            load_models_yaml(yaml_path, "test_model")
        assert "code" in str(exc_info.value).lower()

    def test_load_missing_roms_raises_valueerror(self, tmp_path):
        yaml_content = {
            "test_model": {
                "code": {
                    "templates_compile_time": {
                        "directory": "templates/compile-time",
                        "files": ["cppdefs.opt.j2"],
                    },
                    "templates_run_time": {
                        "directory": "templates/run-time",
                        "files": ["marbl_in"],
                    },
                },
                "model_settings": {"cppdefs": {}},
            }
        }
        yaml_path = self._write(tmp_path, yaml_content)
        with pytest.raises(ValueError) as exc_info:
            load_models_yaml(yaml_path, "test_model")
        assert "roms" in str(exc_info.value).lower()

    def test_load_missing_model_settings_key_cross_validated(self, tmp_path):
        """load_models_yaml surfaces the cross-ref validator: a .j2 compile-time
        file with no matching model_settings key raises.
        """
        yaml_content = {
            "test_model": {
                "code": {
                    "roms": {
                        "location": "https://github.com/test/roms.git",
                        "commit": "x",
                    },
                    "templates_compile_time": {
                        "directory": "templates/compile-time",
                        "files": ["cppdefs.opt.j2"],
                    },
                    "templates_run_time": {
                        "directory": "templates/run-time",
                        "files": ["marbl_in"],
                    },
                },
                "model_settings": {},  # missing "cppdefs"
            }
        }
        yaml_path = self._write(tmp_path, yaml_content)
        with pytest.raises(ValidationError):
            load_models_yaml(yaml_path, "test_model")

    def test_load_resolves_against_real_catalog(self):
        """Load the bundled cson_roms-marbl_v0.1 ModelSpec end-to-end (both the
        lightweight resolver reader and this heavy Pydantic path must agree).
        """
        from cstar_forge.domain_catalog import default_catalog

        spec = load_models_yaml(
            default_catalog.model_path("cson_roms-marbl_v0.1"), "cson_roms-marbl_v0.1"
        )
        assert spec.name == "cson_roms-marbl_v0.1"
        assert spec.code.roms.commit == "0.2.0"
        assert spec.code.templates_commit
        assert "cppdefs" in spec.model_settings
        assert "tides" in spec.model_settings
        # OutputSpec-owned sections must NOT be present on the ModelSpec
        assert "ocean_vars" not in spec.model_settings
        assert "marbl_tracers_to_write" not in spec.model_settings.get("marbl_bgc", {})

    def test_load_via_domain_catalog_load_model_spec(self):
        """domain_catalog.load_model_spec() is the production entry point into this
        heavy path (catalog-registration-time validation).
        """
        from cstar_forge.domain_catalog import default_catalog

        spec = default_catalog.load_model_spec("cson_roms-marbl_v0.1")
        assert isinstance(spec, ModelSpec)
        assert spec.name == "cson_roms-marbl_v0.1"


def test_repo_root_templates_dir_matches_models_py_assumption():
    """Sanity check for the best-effort file-existence validator: models.py assumes
    the forge repo root is one level up from cstar_forge/. Pin that assumption.
    """
    import cstar_forge.models as models_module

    repo_root = Path(models_module.__file__).resolve().parents[1]
    assert (repo_root / "templates" / "compile-time" / "cppdefs.opt.j2").exists()
    assert (repo_root / "templates" / "run-time" / "marbl_in").exists()
