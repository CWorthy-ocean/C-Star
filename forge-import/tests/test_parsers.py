from pathlib import Path
from types import SimpleNamespace

import pytest

from cson_forge.parsers import (
    AppConfig,
    DaskClusterKwargs,
    NotebookConfig,
    NotebookEntry,
    NotebookList,
    NotebookSection,
    load_app_config,
    load_roms_tools_object,
    load_yaml_params,
    normalize_file_type,
    parse_slurm_job_id,
    _parse_notebook_entries,
)


def test_load_yaml_params_single_document(tmp_path):
    yaml_path = tmp_path / "single.yml"
    yaml_path.write_text(
        "\n".join(
            [
                "ccs:",
                "  nx: 224",
                "  ny: 440",
                "  size_x: 2688",
                "  size_y: 5280",
                "  center_lon: -134.5",
                "  center_lat: 39.6",
                "  rot: 33.3",
                "  N: 100",
                "  hc: 250",
                "  theta_s: 6.0",
                "  theta_b: 6.0",
                "  verbose: true",
                "  hmin: 5.0",
            ]
        ),
        encoding="utf-8",
    )
    params = load_yaml_params(yaml_path)
    ccs = params["ccs"]

    assert ccs["nx"] == 224
    assert ccs["ny"] == 440
    assert ccs["size_x"] == 2688
    assert ccs["size_y"] == 5280
    assert ccs["center_lon"] == -134.5
    assert ccs["center_lat"] == 39.6
    assert ccs["rot"] == 33.3
    assert ccs["N"] == 100
    assert ccs["hc"] == 250
    assert ccs["theta_s"] == 6.0
    assert ccs["theta_b"] == 6.0
    assert ccs["verbose"] is True
    assert ccs["hmin"] == 5.0


def test_load_yaml_params_multiple_documents(tmp_path):
    yaml_path = tmp_path / "multi.yml"
    yaml_path.write_text(
        "---\nroms_tools_version: 3.3.0\n---\nGrid:\n  nx: 10\n",
        encoding="utf-8",
    )
    params = load_yaml_params(yaml_path)
    assert params["roms_tools_version"] == "3.3.0"
    assert params["Grid"]["nx"] == 10


def test_normalize_file_type():
    assert normalize_file_type("roms-tools") == "roms-tools"
    assert normalize_file_type("roms_tools") == "roms-tools"
    with pytest.raises(ValueError, match="Supported file types"):
        normalize_file_type("other")


def test_load_roms_tools_object_grid_only(tmp_path):
    yaml_path = tmp_path / "grid.yml"
    yaml_path.write_text("---\nGrid:\n  nx: 10\n", encoding="utf-8")

    called = {}

    class FakeGrid:
        @staticmethod
        def from_yaml(path):
            called["path"] = path
            return "grid"

    module = SimpleNamespace(Grid=FakeGrid)
    result = load_roms_tools_object(yaml_path, roms_tools_module=module)

    assert result == "grid"
    assert called["path"] == str(yaml_path)


def test_load_roms_tools_object_other_class(tmp_path):
    yaml_path = tmp_path / "forcing.yml"
    yaml_path.write_text("---\nGrid:\n  nx: 10\nTidalForcing:\n  source: test\n", encoding="utf-8")

    called = {}

    class FakeTidal:
        @staticmethod
        def from_yaml(path):
            called["path"] = path
            return "forcing"

    module = SimpleNamespace(TidalForcing=FakeTidal, Grid=object())
    result = load_roms_tools_object(yaml_path, roms_tools_module=module)

    assert result == "forcing"
    assert called["path"] == str(yaml_path)


def test_load_roms_tools_object_requires_grid(tmp_path):
    yaml_path = tmp_path / "invalid.yml"
    yaml_path.write_text("---\nTidalForcing:\n  source: test\n", encoding="utf-8")
    module = SimpleNamespace(TidalForcing=object(), Grid=object())
    with pytest.raises(ValueError, match="must include a 'Grid' section"):
        load_roms_tools_object(yaml_path, roms_tools_module=module)


def test_load_roms_tools_object_multiple_sections(tmp_path):
    yaml_path = tmp_path / "invalid.yml"
    yaml_path.write_text(
        "---\nGrid:\n  nx: 10\nTidalForcing:\n  source: test\nSurfaceForcing:\n  source: test\n",
        encoding="utf-8",
    )
    module = SimpleNamespace(TidalForcing=object(), SurfaceForcing=object(), Grid=object())
    with pytest.raises(ValueError, match="only one non-Grid section"):
        load_roms_tools_object(yaml_path, roms_tools_module=module)


def test_dask_cluster_kwargs_model():
    model = DaskClusterKwargs(
        account="m4632",
        queue_name="premium",
        scheduler_file=None,
    )
    assert model.account == "m4632"
    assert model.queue_name == "premium"
    assert model.scheduler_file is None


def test_notebook_entry_model():
    config = NotebookConfig(
        parameters={"grid_yaml": "tests/_grid.yml", "test": True},
        output_path="executed/domain-sizing/example.ipynb",
    )
    entry = NotebookEntry(
        notebook_name="regional-domain-sizing",
        config=config,
    )
    assert entry.notebook_name == "regional-domain-sizing"
    assert entry.config.parameters["test"] is True


def test_notebook_list_model():
    config = NotebookConfig(
        parameters={"grid_yaml": "tests/_grid.yml"},
        output_path="executed/domain-sizing/example.ipynb",
    )
    entry = NotebookEntry(
        notebook_name="regional-domain-sizing",
        config=config,
    )
    section = NotebookSection(title="Test", children=[entry])
    notebook_list = NotebookList(sections=[section])
    assert next(notebook_list.iter_entries()).notebook_name == "regional-domain-sizing"


def test_parameters_config_model():
    config = NotebookConfig(
        parameters={"grid_yaml": "tests/_grid.yml"},
        output_path="executed/domain-sizing/example.ipynb",
    )
    entry = NotebookEntry(
        notebook_name="regional-domain-sizing",
        config=config,
    )
    section = NotebookSection(title="Test", children=[entry])
    notebook_list = NotebookList(sections=[section])
    dask_kwargs = DaskClusterKwargs(
        account="m4632",
        queue_name="premium",
        scheduler_file=None,
    )
    params = AppConfig(
        dask_cluster_kwargs=dask_kwargs,
        notebook_list=notebook_list,
    )
    assert params.dask_cluster_kwargs.account == "m4632"
    assert next(params.notebook_list.iter_entries()).notebook_name == "regional-domain-sizing"


def test_load_app_config(tmp_path):
    config_path = tmp_path / "parameters.yml"
    config_path.write_text(
        "\n".join(
            [
                "dask_cluster_kwargs:",
                "  account: m4632",
                "  queue_name: premium",
                "  scheduler_file: null",
                "",
                "notebooks:",
                "- title: Test",
                "  children:",
                "  - regional-domain-sizing:",
                "      parameters:",
                "        grid_yaml: tests/_grid.yml",
                "        test: true",
                "        scheduler_file: null",
                "      output_path: executed/domain-sizing/example.ipynb",
                "",
            ]
        ),
        encoding="utf-8",
    )

    app_config = load_app_config(config_path)

    assert app_config.dask_cluster_kwargs.account == "m4632"
    first_entry = next(app_config.notebook_list.iter_entries())
    assert first_entry.notebook_name == "regional-domain-sizing"
    assert first_entry.config.parameters["test"] is True
    assert first_entry.config.parameters["grid_yaml"] == str(
        tmp_path / "tests/_grid.yml"
    )
    assert first_entry.config.output_path == str(
        tmp_path / "executed/domain-sizing/example.ipynb"
    )


def test_load_app_config_requires_notebooks(tmp_path):
    config_path = tmp_path / "parameters.yml"
    config_path.write_text(
        "\n".join(
            [
                "dask_cluster_kwargs:",
                "  account: m4632",
                "  queue_name: premium",
                "  scheduler_file: null",
                "",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="notebooks must be a list"):
        load_app_config(config_path)


def test_parse_notebook_entries_requires_list():
    with pytest.raises(ValueError, match="children must be a list"):
        _parse_notebook_entries({"title": "Bad"}, base_dir=Path("."))


def test_parse_notebook_entries_requires_single_key():
    with pytest.raises(ValueError, match="single-key mapping"):
        _parse_notebook_entries([{"one": {}, "two": {}}], base_dir=Path("."))


def test_parse_notebook_entries_requires_mapping_payload():
    with pytest.raises(ValueError, match="payload must be a mapping"):
        _parse_notebook_entries([{"name": "not-a-mapping"}], base_dir=Path("."))


def test_load_yaml_params_rejects_non_mapping(tmp_path):
    yaml_path = tmp_path / "bad.yml"
    yaml_path.write_text("---\n- 1\n- 2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must be mappings"):
        load_yaml_params(yaml_path)


def test_parse_slurm_job_id():
    """Test parsing SLURM Job ID from log file."""
    fixture_path = Path(__file__).parent / "fixtures" / "cson_roms-marbl_v0-1_ccs-12km_70procs_20240101-20240102.out"
    
    job_id = parse_slurm_job_id(fixture_path)
    
    assert job_id == "48052914"


def test_parse_slurm_job_id_not_found(tmp_path):
    """Test parsing SLURM Job ID when not present in file."""
    log_file = tmp_path / "no_job_id.out"
    log_file.write_text("Some log content\nNo SLURM Job ID here\n", encoding="utf-8")
    
    job_id = parse_slurm_job_id(log_file)
    
    assert job_id is None


def test_parse_slurm_job_id_file_not_exists(tmp_path):
    """Test parsing SLURM Job ID when file doesn't exist."""
    nonexistent_file = tmp_path / "nonexistent.out"
    
    job_id = parse_slurm_job_id(nonexistent_file)
    
    assert job_id is None


def test_parse_slurm_job_id_multiple_matches(tmp_path):
    """Test parsing SLURM Job ID when multiple matches exist (should return first)."""
    log_file = tmp_path / "multiple_job_ids.out"
    log_file.write_text(
        "SLURM Job ID: 12345\n"
        "Some other content\n"
        "SLURM Job ID: 67890\n",
        encoding="utf-8"
    )
    
    job_id = parse_slurm_job_id(log_file)
    
    # Should return the first match
    assert job_id == "12345"
