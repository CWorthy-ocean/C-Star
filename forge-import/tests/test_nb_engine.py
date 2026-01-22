from pathlib import Path
from types import SimpleNamespace

import pytest

from cson_forge import nb_engine
from cson_forge import parsers


def test_run_notebook_calls_papermill(monkeypatch, tmp_path):
    import nbformat

    calls = []

    def fake_execute_notebook(input_path, output_path, parameters, kernel_name=None, **kwargs):
        calls.append((input_path, output_path, parameters, kernel_name, kwargs.get("cwd")))

    monkeypatch.setitem(
        __import__("sys").modules,
        "papermill",
        SimpleNamespace(execute_notebook=fake_execute_notebook),
    )
    notebook_path = tmp_path / "a.ipynb"
    nb = nbformat.v4.new_notebook(cells=[nbformat.v4.new_markdown_cell("No template")])
    nbformat.write(nb, str(notebook_path))
    output_dir = tmp_path / "executed"
    output_path = output_dir / "a.ipynb"
    params = {"nx": 10}

    nb_engine.run_notebook(
        notebook_path,
        output_path=output_path,
        parameters=params,
    )

    assert output_dir.exists()
    assert calls == [
        (
            notebook_path.name,
            str(output_path),
            params,
            "cson-atlas",
            str(notebook_path.parent.resolve()),
        ),
    ]


def test_run_notebook_requires_papermill(monkeypatch, tmp_path):
    import builtins
    import nbformat

    notebook_path = tmp_path / "a.ipynb"
    nb = nbformat.v4.new_notebook(cells=[nbformat.v4.new_markdown_cell("No template")])
    nbformat.write(nb, str(notebook_path))

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "papermill":
            raise ImportError("papermill missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError, match="papermill is required"):
        nb_engine.run_notebook(notebook_path, tmp_path / "out.ipynb", {})


def test_run_notebook_replaces_markdown_placeholders(monkeypatch, tmp_path):
    import nbformat

    notebook_path = tmp_path / "template.ipynb"
    nb = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_markdown_cell("Grid: {{ grid_yaml }}"),
            nbformat.v4.new_code_cell("print('ok')"),
        ]
    )
    nbformat.write(nb, str(notebook_path))
    output_path = tmp_path / "executed.ipynb"
    params = {"grid_yaml": "tests/_grid.yml"}

    captured = {}

    def fake_execute_notebook(input_path, output_path_arg, parameters, kernel_name=None, **kwargs):
        cwd = kwargs.get("cwd")
        read_path = Path(cwd) / input_path if cwd and not Path(input_path).is_absolute() else Path(input_path)
        rendered = nbformat.read(str(read_path), as_version=4)
        captured["input_path"] = input_path
        captured["markdown"] = rendered.cells[0]["source"]
        captured["cwd"] = kwargs.get("cwd")

    monkeypatch.setitem(
        __import__("sys").modules,
        "papermill",
        SimpleNamespace(execute_notebook=fake_execute_notebook),
    )

    nb_engine.run_notebook(notebook_path, output_path, params)

    assert captured["input_path"] != str(notebook_path)
    assert captured["markdown"] == "Grid: tests/_grid.yml"
    assert captured["cwd"] == str(notebook_path.parent.resolve())


def test_main_cli_uses_yaml_file(monkeypatch, tmp_path):
    captured = {}

    def fake_run_notebook(notebook_path, output_path, parameters, **kwargs):
        captured["notebook_path"] = notebook_path
        captured["output_path"] = output_path
        captured["parameters"] = parameters
        return None

    monkeypatch.setattr(nb_engine, "run_notebook", fake_run_notebook)

    config_path = tmp_path / "parameters.yml"
    config_path.write_text(
        "\n".join(
            [
                "notebooks:",
                "- title: Test",
                "  children:",
                "  - regional-domain-sizing:",
                "      parameters:",
                "        grid_yaml: tests/_grid.yml",
                "        test: true",
                "      output_path: executed/domain-sizing/example.ipynb",
                "",
            ]
        ),
        encoding="utf-8",
    )
    args = [str(config_path)]

    nb_engine.main(args)

    assert captured["notebook_path"] == Path("regional-domain-sizing.ipynb")
    assert captured["output_path"] == tmp_path / "executed/domain-sizing/example.ipynb"
    assert captured["parameters"]["grid_yaml"] == str(tmp_path / "tests/_grid.yml")


def test_main_cli_requires_yaml_file():
    args = []
    with pytest.raises(SystemExit):
        nb_engine.main(args)
