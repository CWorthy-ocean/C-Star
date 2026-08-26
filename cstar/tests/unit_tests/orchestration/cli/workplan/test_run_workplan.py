import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_RUNID, ENV_CSTAR_STATE_HOME
from cstar.base.exceptions import CstarExpectationFailed
from cstar.cli.common import normalize_runid
from cstar.cli.workplan.run import app
from cstar.orchestration.dag_runner import get_launcher
from cstar.orchestration.launch.local import LocalHandle
from cstar.orchestration.launch.slurm import SlurmHandle, SlurmLauncher
from cstar.orchestration.models import UserDefinedVariables, Workplan
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
