import enum
from pathlib import Path, PosixPath

import pytest
import yaml
from typer.testing import CliRunner

from cstar.cli.workplan.check import app
from cstar.entrypoint.utils import ARG_SCHEMA_ONLY
from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.serialization import (
    deserialize,
    enum_representer,
    path_representer,
    register_representer,
    serialize,
)


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
def test_cli_workplan_check_action_tpl(
    workplan_name: str,
    wp_templates_dir: Path,
) -> None:
    """Verify that CLI check action validates the stored templates.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan template to use for workplan creation
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    """
    template_file = f"{workplan_name}.yaml"
    wp_path = wp_templates_dir / template_file

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert " valid" in result.stdout


def test_cli_workplan_check_dne(
    tmp_path: Path,
) -> None:
    """Verify that an invalid path fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan template to use for workplan creation
    wp_templates_dir : Path
        Fixture returning the path to the directory containing workplan template files
    """
    wp_path = tmp_path / "workplan-dne.yaml"

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert " not found" in result.stderr


def test_cli_workplan_check_file_no_content(
    tmp_path: Path,
) -> None:
    """Verify that an empty workplan file fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "empty_workplan.yml"
    wp_path.touch()

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert "is invalid" in result.stderr


@pytest.mark.parametrize(
    "content",
    [" ", "", "\n", '{"foo": "bar"}', "name: Sample Workplan\n"],
)
def test_cli_workplan_check_file_bad_content(
    tmp_path: Path,
    content: str,
) -> None:
    """Verify that an invalid/malformed workplan fails a validity check.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "invalid_workplan.yml"
    wp_path.write_text(content)

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert "is invalid" in result.stderr


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    "repo_relative_path",
    [
        Path("docs/tutorials/workplan_laptop_example.yaml"),
        Path("cstar/additional_files/templates/wp/workplan.yaml"),
    ],
)
def test_cli_workplan_check_valid_input(
    repo_relative_path: Path,
    package_path: Path,
) -> None:
    """Verify that a valid workplan passes the CLI check.

    NOTE: This test also serves a practical purpose of confirming the continued
    validity of tutorial and sample workplans.

    Parameters
    ----------
    repo_relative_path : Path
        Relative path to a workplan within the c-star repo
    package_path : Path
        Absolute path to the c-star package on disk
    """
    wp_path = package_path / repo_relative_path

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix(), ARG_SCHEMA_ONLY], color=False)

    msg = f"`{wp_path}` does not contain a valid workplan"
    assert "is valid" in result.stdout, msg
    assert result.exit_code == 0, msg


@pytest.mark.parametrize(
    ("start_removal", "end_removal"),
    [
        ("name:", None),
        ("description:", None),
        ("steps:", "<EOF>"),
        ("Prepare", "Ensemble X"),
        ("Ensemble X", "Ensemble Y"),
        ("Ensemble Y", "Aggregate"),
        ("blueprint:", None),
        ("segment_length:", None),
    ],
)
def test_workplan_incomplete_input(
    start_removal: str,
    end_removal: str | None,
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that an incomplete workplan fails the CLI check.

    Starts with a sample workplan and removes a piece of required information in each test.

    Parameters
    ----------
    start_removal : Path
        A string that will trigger content skipping to begin when building a test workplan
    end_removal : Path
       A string that will trigger content skipping to end when building a test workplan
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    package_path : Path
        Absolute path to the c-star package on disk
    """
    wp_path = wp_templates_dir / "workplan.yaml"

    content = wp_path.read_text().splitlines()
    remaining_content: list[str] = []
    cutting = False
    cut_once = False

    for line in content:
        if start_removal in line and not cut_once:
            cutting = True
            cut_once = True
        elif end_removal and end_removal in line:
            cutting = False

        if not cutting:
            remaining_content.append(line)

        if end_removal is None or end_removal in line:
            cutting = False

    wp_path = tmp_path / "wp.yaml"
    wp_path.write_text("\n".join(remaining_content))

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    err_msg = f"{wp_path} should not pass validation"
    assert "is invalid" in result.stderr, err_msg


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    ("field_name", "expect_failure"),
    [
        ("name", True),
        ("description", True),
        ("steps", True),
        ("state", False),
        ("compute_environment", False),
        ("runtime_vars", False),
    ],
)
def test_workplan_optional_input(
    field_name: str,
    expect_failure: bool,
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that an incomplete workplan fails the CLI check.

    Starts with a sample workplan and removes a piece of required information in each test.

    Parameters
    ----------
    field_name : Path
        The field from the Workplan that will be removed.
    expect_failure : bool
        If the field missing will cause a deserialization failure.
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : str
        Directory containing workplan templates
    """
    wp_template = wp_templates_dir / "workplan.yaml"

    wp = deserialize(wp_template, Workplan)
    dumped = wp.model_dump(exclude_defaults=True, by_alias=True)

    # remove the attribute that should cause deserialization to fail
    del dumped[field_name]

    wp_path = tmp_path / "wp.yaml"

    dumper = yaml.Dumper
    dumper.add_multi_representer(enum.Enum, enum_representer)
    register_representer(PosixPath, path_representer)

    with wp_path.open("w") as fp:
        yaml.dump(dumped, fp, sort_keys=False)

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    err_msg = f"{wp_path} should not pass validation"
    is_invalid = "is valid" not in result.stdout
    assert is_invalid == expect_failure, err_msg


def test_workplan_check_remote_workplan_dne() -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is not executed if the URL is invalid.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml_XXX"

    runner = CliRunner()
    result = runner.invoke(app, [wp_uri], color=False)

    assert "not found" in result.stderr


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    "wp_uri",
    [
        "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
        "HTTPS://raw.githubusercontent.com/cworthy-ocean/c-star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
    ],
)
def test_workplan_check_remote_workplan(
    wp_uri: str,
) -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is executed.

    Parameters
    ----------
    wp_uri : str
        A working URL referencing a valid workplan
    """
    runner = CliRunner()
    result = runner.invoke(app, [wp_uri], color=False)

    assert "is valid" in result.stdout


def _write_workplan(wp_path: Path, steps: list[Step], **kwargs: object) -> Path:
    """Serialize a minimal workplan containing the supplied steps.

    Parameters
    ----------
    wp_path : Path
        The path to write the workplan to.
    steps : list[Step]
        The steps to include in the workplan.
    **kwargs : object
        Additional `Workplan` fields, e.g. `runtime_vars`.

    Returns
    -------
    Path
        The path to the serialized workplan.
    """
    wp = Workplan(
        name="Deep Check Test Workplan",
        description="A workplan exercising the default deep resolution pass.",
        steps=steps,
        **kwargs,
    )
    assert serialize(wp_path, wp), "serializing test workplan failed"
    return wp_path


def test_deep_check_resolves_hello_world_steps(
    tmp_path: Path,
    hello_world_bp_path: Path,
    hello_world_bp_content: str,
) -> None:
    """Verify the default deep check resolves applications, blueprints and
    overrides for a workplan with multiple valid steps, and writes nothing.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    hello_world_bp_content : str
        Fixture providing the content of a minimal hello-world blueprint
    """
    second_bp_path = tmp_path / "helloworld2.yaml"
    second_bp_path.write_text(hello_world_bp_content)

    steps = [
        Step(
            name="Say Hello", application="hello_world", blueprint=hello_world_bp_path
        ),
        Step(
            name="Say Hello Again",
            application="hello_world",
            blueprint=second_bp_path,
        ),
    ]
    wp_path = _write_workplan(tmp_path / "hw-workplan.yaml", steps)

    before = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    after = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))

    assert result.exit_code == 0, result.stdout
    assert "is valid" in result.stdout
    assert "resolved for 2 step(s)" in result.stdout
    assert before == after, "the deep check must not write to disk"
    assert not list(tmp_path.rglob("*_trx*"))
    assert not list(tmp_path.rglob("*_transformed*"))


def test_deep_check_unknown_application(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify an unresolvable application name fails the deep check and
    names the application in the output.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    step = Step(
        name="Mystery", application="does-not-exist", blueprint=hello_world_bp_path
    )
    wp_path = _write_workplan(tmp_path / "wp.yaml", [step])

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 1
    assert "does-not-exist" in result.stdout


def test_deep_check_invalid_blueprint_override(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a `blueprint_overrides` key the blueprint model rejects fails
    the deep check and names the offending field.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    step = Step(
        name="Say Hello",
        application="hello_world",
        blueprint=hello_world_bp_path,
        blueprint_overrides={"no_such_field": 1},
    )
    wp_path = _write_workplan(tmp_path / "wp.yaml", [step])

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 1
    assert "no_such_field" in result.stdout


def test_deep_check_schema_only_skips_invalid_override(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify `--schema-only` reports only the schema tier, so an override
    the deep check would reject does not fail the command.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    step = Step(
        name="Say Hello",
        application="hello_world",
        blueprint=hello_world_bp_path,
        blueprint_overrides={"no_such_field": 1},
    )
    wp_path = _write_workplan(tmp_path / "wp.yaml", [step])

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix(), ARG_SCHEMA_ONLY], color=False)

    assert result.exit_code == 0
    assert "is valid" in result.stdout


def test_deep_check_missing_blueprint_file(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a blueprint that no longer exists on disk fails the deep check
    and names the missing path.

    Parsing the workplan's serialized YAML does not itself confirm a
    referenced blueprint file exists -- only the deep pass, which actually
    loads it, does -- so this case exercises the deep check specifically.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    step = Step(
        name="Say Hello", application="hello_world", blueprint=hello_world_bp_path
    )
    wp_path = _write_workplan(tmp_path / "wp.yaml", [step])
    hello_world_bp_path.unlink()

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 1
    assert str(hello_world_bp_path) in result.stdout


@pytest.mark.parametrize(
    ("var_args", "expect_success"),
    [
        pytest.param([], False, id="missing"),
        pytest.param(["--var", "alpha=1"], True, id="supplied"),
    ],
)
def test_deep_check_runtime_vars(
    tmp_path: Path,
    hello_world_bp_path: Path,
    var_args: list[str],
    expect_success: bool,
) -> None:
    """Verify a declared runtime variable is required by the deep check
    when unsupplied, and satisfied once passed via `--var`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    var_args : list[str]
        Extra CLI arguments supplying (or omitting) the runtime variable.
    expect_success : bool
        Whether the deep check is expected to pass.
    """
    step = Step(
        name="Say Hello", application="hello_world", blueprint=hello_world_bp_path
    )
    wp_path = _write_workplan(tmp_path / "wp.yaml", [step], runtime_vars=["alpha"])

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix(), *var_args], color=False)

    if expect_success:
        assert result.exit_code == 0, result.stdout
    else:
        assert result.exit_code == 1
        assert "alpha" in result.stdout


def test_deep_check_tutorial_workplan(
    package_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the laptop tutorial workplan resolves cleanly end-to-end.

    Its blueprints are referenced relative to `docs/tutorials`, so the
    check is run with that directory as the working directory, matching how
    a user would invoke it there.

    Parameters
    ----------
    package_path : Path
        Absolute path to the c-star package on disk
    monkeypatch : pytest.MonkeyPatch
        Fixture used to change the working directory for the invocation.
    """
    monkeypatch.chdir(package_path / "docs" / "tutorials")

    runner = CliRunner()
    result = runner.invoke(app, ["workplan_laptop_example.yaml"], color=False)

    assert result.exit_code == 0, result.stdout
    assert "is valid" in result.stdout
    assert "resolved for 2 step(s)" in result.stdout


def test_deep_check_undeclared_placeholder(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a `{{placeholder}}` naming a variable the workplan never
    declares is reported as a resolution problem, not a traceback.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    step = Step(
        name="Say Hello",
        application="hello_world",
        blueprint=hello_world_bp_path,
        blueprint_overrides={"working_dir": "{{beta}}"},
    )
    wp_path = _write_workplan(tmp_path / "wp.yaml", [step])

    runner = CliRunner()
    result = runner.invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 1, result.stdout
    assert "beta" in result.stdout
    assert "Traceback" not in result.stdout
