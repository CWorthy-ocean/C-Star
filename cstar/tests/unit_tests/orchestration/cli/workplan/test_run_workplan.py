import os
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_STATE_HOME
from cstar.cli.workplan.run import app
from cstar.orchestration.dag_runner import cstar_sysmgr, get_launcher
from cstar.orchestration.models import UserDefinedVariables
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT, ENV_CSTAR_SLURM_QUEUE


def test_workplan_run_file_dne(
    tmp_path: Path,
) -> None:
    """Verify that a path to a non-existent workplan fails to be started due
    to validation.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    wp_path = tmp_path / "workplan-dne.yml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path.as_posix()],
        color=False,
    )

    assert "not found" in result.stderr


def test_workplan_run_remote_workplan_dne() -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is not executed if the URL is invalid.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    """
    wp_path = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml_XXX"

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path],
        color=False,
    )

    assert "not found" in result.stderr


@pytest.mark.parametrize(
    "wp_uri",
    [
        "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
        "HTTPS://raw.githubusercontent.com/cworthy-ocean/c-star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml",
    ],
)
def test_workplan_run_remote_workplan(wp_uri: str) -> None:
    """Verify that a URL to a remote workplan is handled properly and the
    workplan is executed.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    wp_uri : str
        A working URL referencing a valid workplan
    """
    mock_build_and_run_dag = mock.AsyncMock(return_value=0)

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", mock_build_and_run_dag
    ) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_uri],
            color=False,
        )

    assert result.exit_code == 0
    mock_exec.assert_called_once()


def test_workplan_run_variable_unknown(
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
    default_blueprint_path: str,
) -> None:
    """Verify that attempting to run a workplan with runtime variables that are
    not declared by the workplan results in a failure.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    bp_path.write_text(bp_tpl_path.read_text())

    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yaml"

    wp_content = wp_template.read_text()
    wp_content = wp_content.replace(default_blueprint_path, bp_path.as_posix())
    wp_path.write_text(wp_content)

    # template `workplan.yaml` declares: `runtime_vars: [var1, var2]`
    runtime_vars = ["--var", "undeclared=AAA"]

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path.as_posix(), *runtime_vars],
        color=False,
    )

    assert result.exit_code != 0
    assert "unknown" in result.stderr


@pytest.mark.parametrize(
    ("var1", "failed_validation"),
    [
        ("", "format"),
        (" ", "format"),
        ("=", "incomplete"),
        ("var1", "format"),
        ("var1=", "empty"),
        ("var1= ", "empty"),
        ("var1=\n", "empty"),
        ("=value", "orphan"),
        (" =value", "orphan"),
        ("\n=value", "orphan"),
    ],
)
def test_workplan_run_variable_validation_single(
    tmp_path: Path,
    wp_templates_dir: Path,
    var1: str,
    failed_validation: str,
) -> None:
    """Verify that formatting issues in user-supplied runtime variables
    are discovered and reported.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    var1 : str
        A `--var` argument.
    failed_validation : str
        A substring uniquely identifying the expected validation failure
    """
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yml"
    wp_path.write_text(wp_template.read_text())

    # template `workplan.yaml` declares: `runtime_vars: [var1, var2]`
    runtime_vars = ["--var", var1]

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path.as_posix(), *runtime_vars],
        color=False,
    )

    assert result.exit_code != 0
    assert failed_validation in result.stderr


def test_workplan_run_variable_validation_multi_value_mismatch(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that a variable key provided multiple times with different
    values causes a validation failure.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yml"
    wp_path.write_text(wp_template.read_text())

    # template `workplan.yaml` declares: `runtime_vars: [var1, var2]`
    var1 = "var2=xxx"
    var2 = "var2=yyy"
    failed_validation = "multiple"

    runtime_vars = ["--var", var1, "--var", var2]

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path.as_posix(), *runtime_vars],
        color=False,
    )

    assert result.exit_code != 0
    assert failed_validation in result.stderr


def test_workplan_run_variable_multiple_sources(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that using the var and varfile parameter together results in a failure.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yml"
    wp_path.write_text(wp_template.read_text())

    varfile_path = tmp_path / "variables.env"
    varfile_path.write_text("key=value")

    # template `workplan.yaml` declares: `runtime_vars: [var1, var2]`
    var1 = "var2=xxx"
    runtime_vars = ["--var", var1, "--varfile", varfile_path.as_posix()]

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path.as_posix(), *runtime_vars],
        color=False,
    )

    assert result.exit_code != 0
    assert "varfile" in result.stderr
    assert "together" in result.stderr


def test_workplan_run_variable_file_dne(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that using an invalid varfile path results in the expected error.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yml"
    wp_path.write_text(wp_template.read_text())

    varfile_path = tmp_path / "variables.env"

    # template `workplan.yaml` declares: `runtime_vars: [var1, var2]`
    var1 = "var2=xxx"
    runtime_vars = ["--var", var1, "--varfile", varfile_path.as_posix()]

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path.as_posix(), *runtime_vars],
        color=False,
    )

    assert result.exit_code != 0
    assert "Invalid" in result.stderr
    assert "varfile" in result.stderr


@pytest.mark.parametrize(
    "content",
    [
        pytest.param("k=", id="no value"),
        pytest.param("=v", id="no key"),
        pytest.param("=", id="no key or value"),
        pytest.param("", id="empty"),
        pytest.param("", id="whitespace"),
    ],
)
def test_workplan_run_variable_file_malformed(
    tmp_path: Path,
    wp_templates_dir: Path,
    content: str,
) -> None:
    """Verify that using a varfile with invalid content

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yml"
    wp_path.write_text(wp_template.read_text())

    varfile_path = tmp_path / "variables.env"
    varfile_path.write_text(content)

    # template `workplan.yaml` declares: `runtime_vars: [var1, var2]`
    var1 = "var2=xxx"
    runtime_vars = ["--var", var1, "--varfile", varfile_path.as_posix()]

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--run-id", "12345", wp_path.as_posix(), *runtime_vars],
        color=False,
    )

    assert result.exit_code != 0
    assert "Invalid" in result.stderr
    assert "varfile" in result.stderr


def test_orch_ctx_runtime_vars_available_mismatch() -> None:
    """Verify that attempting to specify runtime variables that are
    not declared by the workplan results in a failure.
    """
    available = {"yyy"}
    supplied_vars = {"xxx": "XxXx"}

    variables = UserDefinedVariables(
        keys=available,
        mapping=supplied_vars,
    )

    assert variables.error is not None
    assert "unknown" in variables.error


def test_orch_ctx_runtime_vars_none_available() -> None:
    """Verify that the supplied runtime var key-value pairs are stripped of whitespace.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Used to verify outputs from the CLI
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    available = {"a", "b", "c"}
    supplied_vars = {"  a": "  AAA", " b ": " BBB  ", "c": " xxx "}

    # no validation error should occur
    replacements = UserDefinedVariables(
        keys=available,
        mapping=supplied_vars,
    )

    for key in available:
        assert key in replacements.keys


@pytest.mark.parametrize(
    ("mock_env", "exp_missing"),
    [
        (
            {ENV_CSTAR_SLURM_QUEUE: "xxx", ENV_CSTAR_SLURM_ACCOUNT: ""},
            ENV_CSTAR_SLURM_ACCOUNT,
        ),
        (
            {ENV_CSTAR_SLURM_QUEUE: "", ENV_CSTAR_SLURM_ACCOUNT: "xxx"},
            ENV_CSTAR_SLURM_QUEUE,
        ),
        ({ENV_CSTAR_SLURM_QUEUE: "xxx"}, ENV_CSTAR_SLURM_ACCOUNT),
        ({ENV_CSTAR_SLURM_ACCOUNT: "xxx"}, ENV_CSTAR_SLURM_QUEUE),
    ],
)
def test_launcher_preconditions_slurm(
    mock_env: dict[str, str],
    exp_missing: str,
) -> None:
    """Verify that the SLURM launcher precondition check fails when required env vars
    are missing.

    Parameters
    ----------
    var_name : str
        Known, required env vars that should cause the run to fail if not present.
    """
    with (
        mock.patch.dict(os.environ, mock_env),
        mock.patch.object(cstar_sysmgr, "_scheduler", return_value="any-non-null"),
        pytest.raises(ValueError, match=exp_missing),
    ):
        _ = get_launcher()


@pytest.mark.parametrize(
    ("mock_env", "missing_value"),
    [
        (
            {ENV_CSTAR_SLURM_QUEUE: "xxx", ENV_CSTAR_SLURM_ACCOUNT: ""},
            ENV_CSTAR_SLURM_ACCOUNT,
        ),
        (
            {ENV_CSTAR_SLURM_QUEUE: "", ENV_CSTAR_SLURM_ACCOUNT: "xxx"},
            ENV_CSTAR_SLURM_QUEUE,
        ),
        ({ENV_CSTAR_SLURM_QUEUE: "xxx"}, ENV_CSTAR_SLURM_ACCOUNT),
        ({ENV_CSTAR_SLURM_ACCOUNT: "xxx"}, ENV_CSTAR_SLURM_QUEUE),
    ],
)
def test_launcher_preconditions_local(
    mock_env: dict[str, str],
    missing_value: str,
) -> None:
    """Verify that the Local launcher precondition check does not fail if
    SLURM env vars are missing.

    Parameters
    ----------
    mock_env : str
        Known, required env vars that should not cause the launcher to fail if not present.
    """
    with mock.patch.dict(os.environ, mock_env):
        launcher = get_launcher()

    assert launcher, f"LocalLauncher unexpectedly failed without {missing_value}"


def test_workplan_run_nonexistent_runid(
    tmp_path: Path,
) -> None:
    """Verify that attempting to run with no path and an unknown run-id results
    in the expected error.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    state_dir = tmp_path / "state"
    mock_build_and_run_dag = mock.AsyncMock(return_value=0)

    runner = CliRunner()
    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
        mock.patch("cstar.cli.workplan.run.build_and_run_dag", mock_build_and_run_dag),
    ):
        result = runner.invoke(
            app,
            ["--run-id", "12345"],
            color=False,
        )

    assert result.exit_code != 0
    assert "runs with the id" in result.stderr
    assert "could be found" in result.stderr


def test_workplan_run_default_run_id(
    tmp_path: Path,
    wp_templates_dir: Path,
    bp_templates_dir: Path,
    default_blueprint_path: str,
) -> None:
    """Verify that attempting to run without a run-id causes a default run-id to be
    reused.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template blueprints
    default_blueprint_path : str
        The default path found in sample blueprints that must be replaced
    """
    state_dir = tmp_path / "state"
    bp_path = bp_templates_dir / "blueprint.yaml"
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yml"

    content = wp_template.read_text()
    content = content.replace(default_blueprint_path, bp_path.as_posix())
    wp_path.write_text(content)

    runner = CliRunner()

    mock_build_and_run_dag = mock.AsyncMock(return_value=0)

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
        mock.patch("cstar.cli.workplan.run.build_and_run_dag", mock_build_and_run_dag),
    ):
        result = runner.invoke(
            app,
            ["--dry-run", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0
    assert "sample-workplan" in result.stdout
    mock_build_and_run_dag.assert_awaited_once()


def test_workplan_run_invalid_file_content(
    tmp_path: Path,
) -> None:
    """Verify passing an invalid workplan file results in an error.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    state_dir = tmp_path / "state"
    wp_path = tmp_path / "workplan.yml"
    wp_path.touch()

    runner = CliRunner()

    mock_build_and_run_dag = mock.AsyncMock(return_value=0)

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
        mock.patch("cstar.cli.workplan.run.build_and_run_dag", mock_build_and_run_dag),
    ):
        result = runner.invoke(
            app,
            ["--dry-run", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code != 0
    assert "improper" in result.stderr
    assert "formatted" in result.stderr
    mock_build_and_run_dag.assert_not_awaited()


@pytest.mark.asyncio
async def test_workplan_run_reload_prior_run(
    tmp_path: Path,
    wp_templates_dir: Path,
    bp_templates_dir: Path,
    default_blueprint_path: str,
) -> None:
    """Verify that passing a valid run-id and no path causes the prior run to be loaded.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template blueprints
    default_blueprint_path : str
        The default path found in sample blueprints that must be replaced
    """
    state_dir = tmp_path / "state"
    bp_path = bp_templates_dir / "blueprint.yaml"
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yml"

    content = wp_template.read_text()
    content = content.replace(default_blueprint_path, bp_path.as_posix())
    wp_path.write_text(content)

    fake_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=wp_path,
        output_path=wp_path.parent,
        run_id="12345",
        environment={"CSTAR_LOG_LEVEL": "TRACE"},
    )

    repo = TrackingRepository()
    await repo.put_workplan_run(fake_run)

    def typer_exit(*args, **kwargs) -> None:  # noqa: ANN002, ANN003, ARG001
        raise typer.Exit(0)

    mock_get_wp = mock.Mock(return_value=fake_run)

    runner = CliRunner()
    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.get_workplan_run",
            mock_get_wp,
        ),
        mock.patch("cstar.cli.workplan.run.asyncio.run", side_effect=typer_exit),
    ):
        result = runner.invoke(
            app,
            ["--run-id", "12345"],
            color=False,
        )

    assert result.exit_code == 0

    # confirm the attempt to load the old record was made
    mock_get_wp.assert_called()
