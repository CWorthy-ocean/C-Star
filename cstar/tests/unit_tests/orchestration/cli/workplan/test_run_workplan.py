import json
import os
import typing as t
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.applications.hello_world import HelloWorldApplication
from cstar.applications.plotter import (
    APP_PLOTTER_SCHEMA_1_0_0,
    APP_PLOTTER_SCHEMA_2_0_0,
)
from cstar.base.env import (
    ENV_CSTAR_CLI_DRY_RUN,
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_STATE_HOME,
    FLAG_ON,
)
from cstar.base.exceptions import CstarExpectationFailed
from cstar.cli.common import normalize_runid
from cstar.cli.workplan.run import app, auto_compose
from cstar.orchestration.dag_runner import get_launcher
from cstar.orchestration.launch.local import LocalHandle
from cstar.orchestration.launch.slurm import SlurmHandle, SlurmLauncher
from cstar.orchestration.models import (
    DeferredBlueprintRef,
    Step,
    UserDefinedVariables,
    Workplan,
)
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Status
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT, ENV_CSTAR_SLURM_QUEUE
from cstar.system.environment import EnvSettingsBase, SlurmSettingsBase


async def fake_build_and_run_dag(
    wp_path: Path,
    run_id: str,
    user_variables: dict[str, str] | None = None,
    dry_run: bool = False,
    clobber_steps: list[str] | None = None,
) -> WorkplanRun:
    return WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=wp_path,
        output_path=wp_path.parent,
        run_id=run_id,
        environment={},
        user_variables={},
    )


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


@pytest.mark.usefixtures("read_yaml_intercept")
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
    arg_runid = "12345"
    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", arg_runid, wp_uri],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_called_once()

    # confirm the URL is copied local and a file exists
    wp_path = mock_build_and_run_dag.call_args.args[0]
    assert isinstance(wp_path, Path)
    assert wp_path.exists()
    # confirm the run ID passed from CLI args is used
    assert mock_build_and_run_dag.call_args.args[1] == arg_runid


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_clobber_reaches_build_and_run_dag() -> None:
    """Verify repeated `--clobber` options are forwarded to
    `build_and_run_dag`'s `clobber_steps` keyword argument.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "--run-id",
                "12345",
                "--clobber",
                "Prepare",
                "--clobber",
                "Ensemble X",
                wp_uri,
            ],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()
    assert mock_build_and_run_dag.await_args is not None
    assert mock_build_and_run_dag.await_args.kwargs["clobber_steps"] == [
        "Prepare",
        "Ensemble X",
    ]


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_clobber_all_reaches_build_and_run_dag() -> None:
    """Verify `--clobber all` is expanded by the CLI into every step's
    safe_name before reaching `build_and_run_dag`.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "--run-id",
                "12345",
                "--clobber",
                "all",
                wp_uri,
            ],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()
    assert mock_build_and_run_dag.await_args is not None
    assert mock_build_and_run_dag.await_args.kwargs["clobber_steps"] == [
        "prepare",
        "ensemble-x",
        "ensemble-y",
        "aggregate-results",
    ]


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_clobber_unknown_step_fails_fast() -> None:
    """Verify an unresolvable `--clobber` selection exits with a usage error
    before `build_and_run_dag` is invoked.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", "--clobber", "does-not-exist", wp_uri],
            color=False,
        )

    assert result.exit_code == 2
    assert "Unknown step(s)" in result.output
    assert "does-not-exist" in result.output
    mock_build_and_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_clobber_all_with_step_name_fails_fast() -> None:
    """Verify combining `all` with a step name exits with a usage error
    before `build_and_run_dag` is invoked.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", "--clobber", "all", "--clobber", "Prepare", wp_uri],
            color=False,
        )

    assert result.exit_code == 2
    assert "cannot be combined" in result.output
    mock_build_and_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_bare_clobber_fails_fast() -> None:
    """Verify a bare `--clobber` with no value (as the last argv token) exits
    non-zero due to typer's missing-argument error, rather than clobbering
    every step implicitly.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "--run-id",
            "12345",
            wp_uri,
            "--clobber",
        ],
        color=False,
    )

    assert result.exit_code == 2
    assert "requires an argument" in result.stderr


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_clobber_step_defaults_to_empty_list() -> None:
    """Verify `clobber_steps` defaults to an empty list when `--clobber`
    is not supplied.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_uri],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()
    assert mock_build_and_run_dag.await_args is not None
    assert mock_build_and_run_dag.await_args.kwargs["clobber_steps"] == []


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_variable_unknown(
    wp_templates_dir: Path,
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
    wp_path = wp_templates_dir / "workplan.yaml"

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


@pytest.mark.usefixtures("read_yaml_intercept")
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


@pytest.mark.usefixtures("read_yaml_intercept")
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


@pytest.mark.usefixtures("read_yaml_intercept")
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
    wp_path = wp_templates_dir / "workplan.yaml"

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


@pytest.mark.usefixtures("read_yaml_intercept")
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


@pytest.mark.usefixtures("read_yaml_intercept")
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
    wp_path = wp_templates_dir / "workplan.yaml"

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
    ("mock_env", "settings_klass", "exp_missing"),
    [
        pytest.param(
            {
                ENV_CSTAR_SLURM_QUEUE: "xxx",
                ENV_CSTAR_SLURM_ACCOUNT: "",
            },
            SlurmSettingsBase,
            ENV_CSTAR_SLURM_ACCOUNT,
            id=f"{ENV_CSTAR_SLURM_ACCOUNT}::empty",
        ),
        pytest.param(
            {
                ENV_CSTAR_SLURM_QUEUE: "",
                ENV_CSTAR_SLURM_ACCOUNT: "xxx",
            },
            SlurmSettingsBase,
            ENV_CSTAR_SLURM_QUEUE,
            id=f"{ENV_CSTAR_SLURM_QUEUE}::empty",
        ),
        pytest.param(
            {ENV_CSTAR_SLURM_QUEUE: "xxx"},
            SlurmSettingsBase,
            ENV_CSTAR_SLURM_ACCOUNT,
            id=f"{ENV_CSTAR_SLURM_ACCOUNT}::not-provided",
        ),
        pytest.param(
            {ENV_CSTAR_SLURM_ACCOUNT: "xxx"},
            SlurmSettingsBase,
            ENV_CSTAR_SLURM_QUEUE,
            id=f"{ENV_CSTAR_SLURM_QUEUE}::not-provided",
        ),
    ],
)
def test_launcher_preconditions_slurm(
    mock_env: dict[str, str],
    settings_klass: type[EnvSettingsBase],
    exp_missing: str,
) -> None:
    """Verify that the SLURM launcher precondition check fails when required env vars
    are missing.

    Parameters
    ----------
    var_name : str
        Known, required env vars that should cause the run to fail if not present.
    """
    mock_launcher = mock.MagicMock()
    mock_scheduler = mock.PropertyMock(return_value=mock_launcher)

    with (
        mock.patch.dict(os.environ, mock_env, clear=True),
        mock.patch("cstar.system.manager.CStarSystemManager.scheduler", mock_scheduler),
        mock.patch(
            "cstar.system.environment.CStarEnvironment.settings_klass",
            settings_klass,
        ),
        pytest.raises(CstarExpectationFailed, match=exp_missing),
    ):
        _ = get_launcher()


@pytest.mark.parametrize(
    ("mock_env", "missing_value"),
    [
        pytest.param(
            {ENV_CSTAR_SLURM_QUEUE: "xxx", ENV_CSTAR_SLURM_ACCOUNT: ""},
            ENV_CSTAR_SLURM_ACCOUNT,
            id=f"{ENV_CSTAR_SLURM_ACCOUNT}::empty",
        ),
        pytest.param(
            {ENV_CSTAR_SLURM_QUEUE: "", ENV_CSTAR_SLURM_ACCOUNT: "xxx"},
            ENV_CSTAR_SLURM_QUEUE,
            id=f"{ENV_CSTAR_SLURM_QUEUE}::empty",
        ),
        pytest.param(
            {ENV_CSTAR_SLURM_QUEUE: "xxx"},
            ENV_CSTAR_SLURM_ACCOUNT,
            id=f"{ENV_CSTAR_SLURM_ACCOUNT}::not-provided",
        ),
        pytest.param(
            {ENV_CSTAR_SLURM_ACCOUNT: "xxx"},
            ENV_CSTAR_SLURM_QUEUE,
            id=f"{ENV_CSTAR_SLURM_QUEUE}::not-provided",
        ),
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
    mock_scheduler = mock.PropertyMock(return_value=None)
    with (
        mock.patch.dict(os.environ, mock_env, clear=True),
        mock.patch("cstar.system.manager.CStarSystemManager.scheduler", mock_scheduler),
    ):
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
    mock_build_and_run_dag = mock.AsyncMock(
        return_value=mock.MagicMock(
            dry_run=True,
            name="sample-workplan",
            run_id="12345",
            state_dir="/tmp/state",
        )
    )

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


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_default_run_id(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that attempting to run without a run-id causes a default run-id to be
    reused.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    exp_default_run_id = "sample-workplan"

    runner = CliRunner()

    mock_build_and_run_dag = mock.AsyncMock(
        return_value=WorkplanRun(
            workplan_path=wp_path,
            trx_workplan_path=wp_path,
            output_path=wp_path.parent,
            run_id=exp_default_run_id,
            environment={},
            user_variables={},
        )
    )

    with mock.patch("cstar.cli.workplan.run.build_and_run_dag", mock_build_and_run_dag):
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

    mock_build_and_run_dag = mock.AsyncMock(
        return_value=mock.MagicMock(
            dry_run=True,
            name="sample-workplan",
            run_id="12345",
            state_dir="/tmp/state",
        )
    )

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


@pytest.mark.parametrize("status", [Status.Unsubmitted, Status.Submitted, Status.Done])
@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_reload_prior_run(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
    status: Status,
) -> None:
    """Verify that passing a valid run-id and no path causes the prior run to be loaded.

    Ensure that a prior run with successfully completed steps doesn't repeat the step and
    when the sentinels don't reflect "real state" they are updated.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    run_id = mock_run_id
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    lwp_path = tmp_path / f"live-{wp_path.name}"
    assert serialize(lwp_path, lwp), "serializing live workplan failed in test"

    sentinel_paths = set[Path]()
    for i, s in enumerate(lwp.steps):
        n = -len(lwp.steps) + i
        h = LocalHandle(
            pid=str(1000 + n),
            name=s.safe_name,
            run_id=run_id,
            status=status,
            start_at=datetime.now() + timedelta(days=n),
        )
        p = StateRepository.sentinel_path(h)
        assert serialize(p, h), "serializing the mock handles failed in test"
        sentinel_paths.add(p)

    fake_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=lwp_path,
        output_path=lwp_path.parent,
        run_id=run_id,
        environment={"CSTAR_LOG_LEVEL": "TRACE"},
        sentinels=sentinel_paths,
    )

    repo = TrackingRepository()
    repo.put_workplan_run_sync(fake_run)

    def typer_exit(*args, **kwargs) -> None:  # type: ignore # noqa: ANN002, ANN003, ARG001
        raise typer.Exit(1)

    runner = CliRunner()
    with (
        mock.patch(
            "cstar.orchestration.dag_runner.get_launcher",
            SlurmLauncher,
        ),
        mock.patch(
            "cstar.orchestration.launch.slurm.SlurmLauncher.query_status",
            mock.AsyncMock(return_value=Status.Done),
        ) as mock_query_status,
        mock.patch(
            "cstar.orchestration.launch.slurm.SlurmLauncher._submit",
            side_effect=typer_exit,
        ) as mock_submit,
    ):
        result = runner.invoke(
            app,
            ["--run-id", mock_run_id],
            color=False,
        )

    # RC would be 1 if submit was called for any task due to side_effect
    assert result.exit_code == 0

    # confirm the attempt to load the old record was made
    assert mock_query_status.call_count == 4  # always query status for each step
    assert not mock_submit.called

    # confirm the status-change handler fires to update the persisted record
    statuses = {deserialize(p, SlurmHandle).status for p in sentinel_paths}
    assert statuses == {Status.Done}


@pytest.mark.parametrize("status", [Status.Submitted, Status.Running, Status.Ending])
@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_reload_prior_run_in_progress(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
    status: Status,
) -> None:
    """Verify that reloading a run whose jobs are still queued or running
    adopts the in-flight jobs instead of submitting duplicates.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    run_id = mock_run_id
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    lwp_path = tmp_path / f"live-{wp_path.name}"
    assert serialize(lwp_path, lwp), "serializing live workplan failed in test"

    sentinel_paths = set[Path]()
    for i, s in enumerate(lwp.steps):
        n = -len(lwp.steps) + i
        h = LocalHandle(
            pid=str(1000 + n),
            name=s.safe_name,
            run_id=run_id,
            status=Status.Submitted,
            start_at=datetime.now() + timedelta(days=n),
        )
        p = StateRepository.sentinel_path(h)
        assert serialize(p, h), "serializing the mock handles failed in test"
        sentinel_paths.add(p)

    fake_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=lwp_path,
        output_path=lwp_path.parent,
        run_id=run_id,
        environment={"CSTAR_LOG_LEVEL": "TRACE"},
        sentinels=sentinel_paths,
    )

    repo = TrackingRepository()
    repo.put_workplan_run_sync(fake_run)

    def typer_exit(*args, **kwargs) -> None:  # type: ignore # noqa: ANN002, ANN003, ARG001
        raise typer.Exit(1)

    runner = CliRunner()
    with (
        mock.patch(
            "cstar.orchestration.dag_runner.get_launcher",
            SlurmLauncher,
        ),
        mock.patch(
            "cstar.orchestration.launch.slurm.SlurmLauncher.query_status",
            mock.AsyncMock(return_value=status),
        ) as mock_query_status,
        mock.patch(
            "cstar.orchestration.launch.slurm.SlurmLauncher._submit",
            side_effect=typer_exit,
        ) as mock_submit,
    ):
        result = runner.invoke(
            app,
            ["--run-id", mock_run_id],
            color=False,
        )

    # RC would be 1 if submit was called for any task due to side_effect
    assert result.exit_code == 0

    # confirm the attempt to load the old record was made
    assert mock_query_status.call_count == 4  # always query status for each step
    assert not mock_submit.called  # in-flight jobs are adopted, not re-submitted

    # confirm the status-change handler fires to update the persisted record
    statuses = {deserialize(p, SlurmHandle).status for p in sentinel_paths}
    assert statuses == {status}


@pytest.mark.parametrize("status", [Status.Cancelled, Status.Failed])
@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_reload_prior_run_repeat_failures(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
    status: Status,
) -> None:
    """Verify that passing a valid run-id and no path causes the prior run to be loaded.

    Ensure that a prior run with _failed_ steps re-runs the failed steps and updates
    the sentinels to reflect the newly submitted status.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    run_id = mock_run_id
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    lwp_path = tmp_path / f"live-{wp_path.name}"
    assert serialize(lwp_path, lwp), "serializing live workplan failed in test"

    sentinel_paths = set[Path]()
    for i, s in enumerate(lwp.steps):
        n = -len(lwp.steps) + i
        h = LocalHandle(
            pid=str(1000 + n),
            name=s.safe_name,
            run_id=run_id,
            status=Status.Submitted,
            start_at=datetime.now() + timedelta(days=n),
        )
        p = StateRepository.sentinel_path(h)
        assert serialize(p, h), "serializing the mock handles failed in test"
        sentinel_paths.add(p)

    fake_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=lwp_path,
        output_path=lwp_path.parent,
        run_id=run_id,
        environment={"CSTAR_LOG_LEVEL": "TRACE"},
        sentinels=sentinel_paths,
    )

    repo = TrackingRepository()
    repo.put_workplan_run_sync(fake_run)

    submission_results = (
        SlurmHandle(
            pid=str(9996 + i),
            name=s.name,
            run_id=run_id,
            status=Status.Submitted,
        )
        for i, s in enumerate(lwp.steps)
    )
    runner = CliRunner()

    with (
        mock.patch.object(
            SlurmLauncher,
            "query_status",
            mock.AsyncMock(return_value=status),
        ) as mock_query_status,
        mock.patch.object(
            SlurmLauncher,
            "_prune_completed_dependencies",
            mock.AsyncMock(return_value=[]),
        ),
        mock.patch.object(
            SlurmLauncher,
            "_submit",
            mock.AsyncMock(side_effect=submission_results),
        ) as mock_submit,
        mock.patch(
            "cstar.orchestration.dag_runner.get_launcher",
            SlurmLauncher,
        ),
    ):
        result = runner.invoke(
            app,
            ["--run-id", mock_run_id],
            color=False,
        )

    # RC would be 1 if submit was called for any task due to side_effect
    assert result.exit_code == 0

    # confirm the attempt to load the old record was made
    assert mock_query_status.call_count == 4  # always query status for each step
    assert mock_submit.called  # for fail states, expect a new task submission

    # confirm the status-change handler fires to update the persisted record
    statuses = {deserialize(p, SlurmHandle).status for p in sentinel_paths}
    # ... and the old fail states from the sentinel records are replaced
    assert statuses == {Status.Submitted}


@pytest.mark.usefixtures("read_yaml_intercept")
def test_cli_workplan_run_normalizes_mixed_case_runid(
    wp_templates_dir: Path,
    tmp_path: Path,
) -> None:
    """Verify a user-supplied run-id is slugified (lowercased) by the run-id
    callback pipeline before it reaches the environment or the dag runner.

    Mixed-case run-ids previously produced two run directories (one raw, one
    slugified) and mismatched tracking/cache entries because the environment
    variable was slugified while directories and tracking records used the
    raw value.
    """
    wp_path = wp_templates_dir / "workplan.yaml"

    mock_build_and_run_dag = mock.AsyncMock(
        return_value=WorkplanRun(
            workplan_path=wp_path,
            trx_workplan_path=wp_path,
            output_path=tmp_path,
            run_id="not-used",
            environment={},
            user_variables={},
        )
    )

    args: list[str] = ["--run-id", "  MyRun_01  ", str(wp_path)]

    with (
        mock.patch("cstar.cli.workplan.run.build_and_run_dag", mock_build_and_run_dag),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            args,
            color=False,
        )

    assert result.exit_code == 0
    assert os.environ[ENV_CSTAR_RUNID] == "myrun_01"
    assert mock_build_and_run_dag.call_args.args[1] == "myrun_01"


@pytest.mark.parametrize("suffix", [".yaml", ".yml"])
def test_auto_compose_wraps_blueprint(
    tmp_path: Path,
    hello_world_bp_content: str,
    suffix: str,
) -> None:
    """Verify a blueprint path is wrapped in a generated host workplan that
    is written next to the blueprint, preserving the blueprint's suffix.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_content : str
        Fixture providing the content of a minimal hello-world blueprint
    suffix : str
        The file extension of the blueprint being wrapped
    """
    bp_path = tmp_path / f"helloworld{suffix}"
    bp_path.write_text(hello_world_bp_content)

    result = auto_compose(bp_path.as_posix())

    wp_path = Path(result)
    assert wp_path != bp_path
    assert wp_path.parent == bp_path.parent
    assert wp_path.name == f"say-hello-to-my-little-friend-host-workplan{suffix}"
    assert wp_path.exists()

    # the generated host workplan must contain a single step executing the blueprint
    wp = deserialize(wp_path, Workplan)
    assert wp.name == "Say hello to my little friend! Host"
    assert len(wp.steps) == 1

    step = wp.steps[0]
    assert step.application == "hello_world"
    assert Path(step.blueprint_path).resolve() == bp_path.resolve()
    assert "Say hello to my little friend!" in step.name


def test_auto_compose_ignores_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify a workplan path passes through `auto_compose` unchanged and no
    host workplan file is generated.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    wp_templates_dir : Path
        Fixture providing the path to a directory containing template workplans
    """
    wp_template = wp_templates_dir / "workplan.yaml"
    wp_path = tmp_path / "workplan.yaml"
    wp_path.write_text(wp_template.read_text())

    files_before = set(tmp_path.iterdir())

    assert auto_compose(wp_path.as_posix()) == wp_path.as_posix()
    assert set(tmp_path.iterdir()) == files_before


def test_auto_compose_ignores_non_blueprint_content(tmp_path: Path) -> None:
    """Verify a file that is neither a blueprint nor a workplan passes
    through `auto_compose` unchanged and no host workplan file is generated.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    path = tmp_path / "not-a-blueprint.yaml"
    path.write_text("some: [unrelated, content]\n")

    files_before = set(tmp_path.iterdir())

    assert auto_compose(path.as_posix()) == path.as_posix()
    assert set(tmp_path.iterdir()) == files_before


def test_workplan_run_blueprint_auto_composes(hello_world_bp_path: Path) -> None:
    """Verify submitting a blueprint to `cstar workplan run` generates a host
    workplan and passes the generated workplan (not the blueprint) on to
    `build_and_run_dag`.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", hello_world_bp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()

    # confirm the composed host workplan is what continues through the run
    wp_path = mock_build_and_run_dag.call_args.args[0]
    assert isinstance(wp_path, Path)
    assert wp_path.name.endswith("-host-workplan.yaml")
    assert wp_path.exists()

    # confirm the host workplan step executes the submitted blueprint
    wp = deserialize(wp_path, Workplan)
    assert [Path(s.blueprint_path).resolve() for s in wp.steps] == [
        hello_world_bp_path.resolve()
    ]


def test_workplan_run_blueprint_default_run_id(hello_world_bp_path: Path) -> None:
    """Verify that omitting the run-id when submitting a blueprint derives
    the default run-id from the generated host workplan rather than failing
    to parse the blueprint.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [hello_world_bp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()
    assert (
        mock_build_and_run_dag.call_args.args[1] == "say-hello-to-my-little-friend-host"
    )


@pytest.mark.parametrize(
    ("raw_run_id", "expected"),
    [
        ("MyRun", "myrun"),
        ("  MyRun_01  ", "myrun_01"),
        ("My Run!", "my-run"),
        ("already-lowercase", "already-lowercase"),
        ("", ""),
        ("   ", ""),
    ],
)
def test_normalize_runid(raw_run_id: str, expected: str) -> None:
    """Verify the shared run-id normalization callback slugifies non-empty
    values and passes empty values through unchanged (so callers keep their
    own presence/default handling).

    Parameters
    ----------
    raw_run_id : str
        The run-id as typed by the user.
    expected : str
        The normalized run-id expected from the callback.
    """
    ctx = mock.MagicMock(spec=typer.Context)

    assert normalize_runid(ctx, raw_run_id) == expected


def _write_workplan(wp_path: Path, steps: list[Step]) -> Path:
    """Serialize a minimal workplan containing the supplied steps.

    Parameters
    ----------
    wp_path : Path
        The path to write the workplan to.
    steps : list[Step]
        The steps to include in the workplan.

    Returns
    -------
    Path
        The path to the serialized workplan.
    """
    wp = Workplan(
        name="Migration Test Workplan",
        description="A workplan exercising automatic blueprint migration.",
        steps=steps,
    )
    assert serialize(wp_path, wp), "serializing test workplan failed"
    return wp_path


def _expected_migrated_path(bp_path: Path) -> Path:
    """Return the path where an auto-migrated plotter blueprint is persisted.

    Follows the `<original_stem>_<latest_version>.<ext>` convention used when
    no explicit output path is supplied to the migrator.
    """
    state_dir = Path(str(os.getenv(ENV_CSTAR_STATE_HOME, "")))
    return state_dir / f"{bp_path.stem}_{APP_PLOTTER_SCHEMA_2_0_0}{bp_path.suffix}"


@pytest.fixture
def plotter_workplan_path(tmp_path: Path, plotter_v1_0_0_bp: Path) -> Path:
    """Serialize a workplan with a single step referencing an out-of-date
    (schema 1.0.0) plotter blueprint.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_bp : Path
        Fixture providing the path to a plotter blueprint at schema 1.0.0
    """
    step = Step(name="Plot", application="plotter", blueprint=plotter_v1_0_0_bp)
    return _write_workplan(tmp_path / "plotter-workplan.yaml", [step])


def test_workplan_run_migrates_out_of_date_blueprint(
    plotter_workplan_path: Path,
    plotter_v1_0_0_bp: Path,
) -> None:
    """Verify a workplan step referencing an out-of-date blueprint is
    automatically migrated: the migrated blueprint is persisted to the state
    home and the workplan handed to `build_and_run_dag` references it.

    Parameters
    ----------
    plotter_workplan_path : Path
        Fixture providing a workplan referencing an out-of-date blueprint
    plotter_v1_0_0_bp : Path
        Fixture providing the path to a plotter blueprint at schema 1.0.0
    """
    migrated_bp_path = _expected_migrated_path(plotter_v1_0_0_bp)

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", plotter_workplan_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()

    # the migrated blueprint is persisted using the auto-naming convention
    assert migrated_bp_path.exists()
    migrated_content = migrated_bp_path.read_text()
    assert "working_dir" in migrated_content
    assert "output_dir" not in migrated_content

    # the workplan that continues through the run references the migrated
    # blueprint instead of the out-of-date original
    wp_path = mock_build_and_run_dag.call_args.args[0]
    wp = deserialize(wp_path, Workplan)
    assert [Path(str(s.blueprint_path)).resolve() for s in wp.steps] == [
        migrated_bp_path.resolve()
    ]


def test_workplan_run_migration_rewrites_workplan_in_place(
    plotter_workplan_path: Path,
    plotter_v1_0_0_bp: Path,
) -> None:
    """Verify the workplan file itself is updated with the migrated blueprint
    path so re-running the same file does not repeat the migration lookup
    against the stale blueprint.

    Parameters
    ----------
    plotter_workplan_path : Path
        Fixture providing a workplan referencing an out-of-date blueprint
    plotter_v1_0_0_bp : Path
        Fixture providing the path to a plotter blueprint at schema 1.0.0
    """
    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", plotter_workplan_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0

    persisted_wp = deserialize(plotter_workplan_path, Workplan)
    assert [Path(str(s.blueprint_path)).resolve() for s in persisted_wp.steps] == [
        _expected_migrated_path(plotter_v1_0_0_bp).resolve()
    ]


def test_workplan_run_up_to_date_blueprint_not_migrated(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a workplan step referencing a blueprint already at the latest
    schema version passes through unchanged: no migrated copy is written and
    the step continues to reference the original blueprint.

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
    wp_path = _write_workplan(tmp_path / "hw-workplan.yaml", [step])

    # a comment survives only if the file is not re-serialized; re-serializing
    # the model would otherwise reproduce byte-identical content
    wp_path.write_text(f"# user comment\n{wp_path.read_text()}")
    wp_content_before = wp_path.read_text()

    state_dir = Path(str(os.getenv(ENV_CSTAR_STATE_HOME, "")))
    state_files_before = set(state_dir.rglob("*"))

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()

    # no migrated blueprint copy is written to the state home
    new_state_files = set(state_dir.rglob("*")) - state_files_before
    assert not [p for p in new_state_files if hello_world_bp_path.stem in p.name]

    # the step continues to reference the original, up-to-date blueprint
    wp = deserialize(mock_build_and_run_dag.call_args.args[0], Workplan)
    assert [Path(str(s.blueprint_path)).resolve() for s in wp.steps] == [
        hello_world_bp_path.resolve()
    ]

    # nothing was migrated, so the workplan file itself is not rewritten
    assert wp_path.read_text() == wp_content_before


def test_workplan_run_migration_disabled_fails_fast(
    plotter_workplan_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `CSTAR_DISABLE_MIGRATION` blocks a run whose workplan references
    an out-of-date blueprint before any run machinery starts.

    Parameters
    ----------
    plotter_workplan_path : Path
        Fixture providing a workplan referencing an out-of-date blueprint
    monkeypatch : pytest.MonkeyPatch
        Used to set the migration kill-switch environment variable
    """
    monkeypatch.setenv(ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON)

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", plotter_workplan_path.as_posix()],
            color=False,
        )

    assert result.exit_code != 0
    # rich wraps the message at the terminal width, so normalize whitespace
    assert "migration is disabled" in " ".join(result.stdout.split())
    mock_build_and_run_dag.assert_not_awaited()


def test_workplan_run_migration_dry_run_plans_all_steps_without_persisting(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify dry-run mode plans the migration for every out-of-date step
    without persisting migrated blueprints, and the run continues instead of
    exiting after the first planned step.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_model : dict[str, t.Any]
        Fixture providing the raw content of a plotter blueprint at schema 1.0.0
    monkeypatch : pytest.MonkeyPatch
        Used to enable dry-run mode before the eager path callback runs
    """
    monkeypatch.setenv(ENV_CSTAR_CLI_DRY_RUN, FLAG_ON)

    bp_paths: list[Path] = []
    for i in range(2):
        bp_path = tmp_path / f"plotter_{i}.json"
        bp_path.write_text(json.dumps(plotter_v1_0_0_model))
        bp_paths.append(bp_path)

    steps = [
        Step(name=f"Plot {i}", application="plotter", blueprint=bp_path)
        for i, bp_path in enumerate(bp_paths)
    ]
    wp_path = _write_workplan(tmp_path / "dry-run-workplan.yaml", steps)

    # a comment survives only if the file is not re-serialized; re-serializing
    # the model would otherwise reproduce byte-identical content
    wp_path.write_text(f"# user comment\n{wp_path.read_text()}")
    wp_content_before = wp_path.read_text()

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", "--dry-run", wp_path.as_posix()],
            color=False,
        )

    # planning the first step's migration must not exit the run prematurely
    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()

    plan_msg = f"Migrating {APP_PLOTTER_SCHEMA_1_0_0!r}->{APP_PLOTTER_SCHEMA_2_0_0!r}"
    assert result.stdout.count(plan_msg) == len(bp_paths)

    # no migrated blueprints are persisted during a dry run
    for bp_path in bp_paths:
        assert not _expected_migrated_path(bp_path).exists()

    # the steps continue to reference the original blueprints
    wp = deserialize(mock_build_and_run_dag.call_args.args[0], Workplan)
    assert [Path(str(s.blueprint_path)).resolve() for s in wp.steps] == [
        p.resolve() for p in bp_paths
    ]

    # a dry run must not modify the user's workplan file
    assert wp_path.read_text() == wp_content_before


def test_workplan_run_migration_not_registered_leaves_workplan_untouched(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify a workplan referencing a blueprint for an application with no
    registered migration adapters is left byte-for-byte untouched and the run
    proceeds.

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
    wp_path = _write_workplan(tmp_path / "hw-workplan.yaml", [step])

    # a comment survives only if the file is not re-serialized; re-serializing
    # the model would otherwise reproduce byte-identical content
    wp_path.write_text(f"# user comment\n{wp_path.read_text()}")
    content_before = wp_path.read_text()

    with (
        mock.patch.object(HelloWorldApplication, "migrations", ()),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()
    assert wp_path.read_text() == content_before


def test_workplan_run_migration_skips_deferred_blueprint(
    tmp_path: Path,
    plotter_v1_0_0_bp: Path,
) -> None:
    """Verify a step deferring its blueprint to an upstream step is skipped by
    automatic migration while sibling steps with concrete blueprint paths are
    still migrated.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_bp : Path
        Fixture providing the path to a plotter blueprint at schema 1.0.0
    """
    steps = [
        Step(name="Plot", application="plotter", blueprint=plotter_v1_0_0_bp),
        Step(
            name="Replot",
            application="plotter",
            blueprint=DeferredBlueprintRef(from_step="Plot"),
            depends_on=["Plot"],
        ),
    ]
    wp_path = _write_workplan(tmp_path / "deferred-workplan.yaml", steps)

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 0
    mock_build_and_run_dag.assert_awaited_once()

    wp = deserialize(mock_build_and_run_dag.call_args.args[0], Workplan)

    # the concrete step is migrated
    concrete_step = wp.steps[0]
    assert (
        Path(str(concrete_step.blueprint_path)).resolve()
        == _expected_migrated_path(plotter_v1_0_0_bp).resolve()
    )

    # the deferred step is preserved as-is
    deferred_step = wp.steps[1]
    assert deferred_step.is_deferred
    assert isinstance(deferred_step.blueprint_path, DeferredBlueprintRef)
    assert deferred_step.blueprint_path.from_step == "Plot"


def test_workplan_run_step_blueprint_missing_reports_blueprint(
    tmp_path: Path,
) -> None:
    """Verify a workplan step referencing a non-existent blueprint fails with
    an error naming the missing blueprint -- not the workplan, which exists.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    missing_bp = tmp_path / "gone.yaml"
    step = Step(name="Plot", application="plotter", blueprint=missing_bp.as_posix())
    wp_path = _write_workplan(tmp_path / "missing-bp-workplan.yaml", [step])

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 2
    mock_build_and_run_dag.assert_not_awaited()

    stderr_flat = " ".join(result.stderr.replace("│", " ").split())
    assert "Blueprint not found" in stderr_flat
    assert "Workplan not found" not in stderr_flat
    # the message must name the missing blueprint; rich wraps long paths at
    # arbitrary points, so strip all whitespace and decoration before matching
    assert missing_bp.name in "".join(stderr_flat.split())


def test_workplan_run_step_blueprint_migration_invalid(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
) -> None:
    """Verify a workplan step whose blueprint migrates to content that fails
    model validation is rejected with a usage error naming the blueprint,
    instead of a raw traceback.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_model : dict[str, t.Any]
        Fixture providing the raw content of a plotter blueprint at schema 1.0.0
    """
    # drop fields the migrated model requires so post-migration validation fails
    model = {
        k: v
        for k, v in plotter_v1_0_0_model.items()
        if k not in ("input_dir", "grid_file_path")
    }
    bp_path = tmp_path / "plotter_incomplete_1.0.0.json"
    bp_path.write_text(json.dumps(model))

    step = Step(name="Plot", application="plotter", blueprint=bp_path)
    wp_path = _write_workplan(tmp_path / "invalid-migration-workplan.yaml", [step])

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 2
    mock_build_and_run_dag.assert_not_awaited()

    stderr_flat = " ".join(result.stderr.replace("│", " ").split())
    assert "is invalid" in stderr_flat
    assert "Details:" in stderr_flat
    # the failing blueprint (not the workplan) is named in the message
    stderr_squashed = "".join(stderr_flat.split())
    assert bp_path.name in stderr_squashed
    assert wp_path.name not in stderr_squashed
