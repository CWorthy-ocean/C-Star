import asyncio
import json
import os
import shutil
import typing as t
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.applications.core import get_application
from cstar.applications.hello_world import HelloWorldApplication
from cstar.base.env import (
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_STATE_HOME,
    FLAG_ON,
    REMOVED_SETTINGS,
    capture_environment,
    load_user_config,
)
from cstar.base.exceptions import CstarExpectationFailed
from cstar.cli.common import normalize_runid
from cstar.cli.workplan.run import (
    COMMAND_SUBMIT,
    app,
    auto_compose,
    get_run_summary_display,
    handle_run_reloading,
    migrate_steps,
    refuse_run_id_collision,
)
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_PRE_RUN, ARG_RESUME, ARG_RUN_ID
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.dag_runner import (
    DagStatus,
    ExecutiveRunSummary,
    NoPreparableStepsError,
    get_launcher,
    original_workplan_backup,
    prepare_workplan,
)
from cstar.orchestration.launch.local import LocalHandle
from cstar.orchestration.launch.slurm import SlurmHandle, SlurmLauncher
from cstar.orchestration.manifest import (
    ATTEMPT_WORKPLAN_NAME,
    RUN_MANIFEST_NAME,
    Attempt,
    RunManifest,
    WorkplanIdentity,
    create_attempt_dir,
    write_run_manifest,
)
from cstar.orchestration.models import (
    KEY_PRE_RUN,
    DeferredBlueprintRef,
    Step,
    UserDefinedVariables,
    Workplan,
)
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Planner, Status
from cstar.orchestration.serialization import (
    deserialize,
    serialize,
)
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.utils import ENV_CSTAR_SLURM_ACCOUNT, ENV_CSTAR_SLURM_QUEUE
from cstar.system.environment import EnvSettingsBase, SlurmSettingsBase
from cstar.tests.unit_tests.orchestration.manifest_runs import write_identity


def _slurm_launcher(
    workplan: Workplan | None = None, *, force_local: bool = False
) -> SlurmLauncher:
    """Stand in for `get_launcher` with a SLURM launcher regardless of the system."""
    return SlurmLauncher()


def _place_transformed(workplan_path: Path, output_path: Path, run_id: str) -> Path:
    """Make `output_path` the run directory of a run of `workplan_path`.

    Its manifest names `workplan_path`, and `<stem>_transformed.yaml` beside it
    is a copy of the workplan when that exists, an empty file when not. A manifest
    the test already wrote there is kept.
    """
    if not (output_path / RUN_MANIFEST_NAME).exists():
        write_identity(output_path, run_id, workplan=workplan_path)
    transformed = output_path / f"{workplan_path.stem}_transformed.yaml"
    if workplan_path.is_file() and not transformed.exists():
        shutil.copy(workplan_path, transformed)
    transformed.touch()
    return transformed


def _scheduled_run(wp_path: Path, run_id: str) -> WorkplanRun:
    """The record of a run of the workplan at `wp_path` scheduled into the data home.

    The run directory holds the manifest naming `wp_path` and its transformed
    workplan, so the record derives its workplan paths.
    """
    output_path = StateDirectoryManager.data_dir(run_id)
    _place_transformed(wp_path, output_path, run_id)
    return WorkplanRun(
        output_path=output_path,
        run_id=run_id,
        environment={},
        user_variables={},
    )


async def fake_build_and_run_dag(
    wp_path: Path,
    run_id: str,
    user_variables: dict[str, str] | None = None,
    clobber_steps: list[str] | None = None,
    pre_run: bool = False,
) -> WorkplanRun:
    return _scheduled_run(wp_path, run_id)


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


def _slurm_workplan(
    tmp_path: Path,
    compute_environment: dict[str, t.Any],
    *step_overrides: dict[str, t.Any],
) -> Workplan:
    """A hello-world workplan with the given compute environment and one step per override."""
    bp = tmp_path / "hello.yaml"
    bp.touch()
    steps = [
        Step(
            name=f"Hello {i}",
            application="hello_world",
            blueprint=bp,
            compute_overrides=o,
        )
        for i, o in enumerate(step_overrides or ({},))
    ]
    return Workplan(
        name="slurm-workplan",
        description="A workplan supplying SLURM settings",
        steps=steps,
        compute_environment=compute_environment,
    )


@pytest.mark.parametrize(
    ("compute_environment", "step_overrides", "exp_missing"),
    [
        pytest.param(
            {"slurm": {"account_name": "acct", "queue_name": "q"}},
            [],
            "",
            id="workplan-wide account and queue",
        ),
        pytest.param(
            {"slurm": {"account_name": "acct"}},
            [],
            f"system: {ENV_CSTAR_SLURM_QUEUE}$",
            id="workplan-wide account only",
        ),
        pytest.param(
            {},
            [{"slurm": {"account_name": "a", "queue_name": "q"}}] * 2,
            "",
            id="every step supplies both",
        ),
        pytest.param(
            {},
            [
                {"slurm": {"account_name": "a", "queue_name": "q"}},
                {"slurm": {"queue_name": "q"}},
            ],
            f"system: {ENV_CSTAR_SLURM_ACCOUNT}$",
            id="one step lacks the account",
        ),
        pytest.param(
            {"slurm": {"account_name": "acct"}},
            [{"slurm": {"queue_name": "q"}}],
            "",
            id="workplan account plus per-step queue",
        ),
        pytest.param(
            {"launcher": "slurm"},
            [],
            f"{ENV_CSTAR_SLURM_ACCOUNT}, {ENV_CSTAR_SLURM_QUEUE}",
            id="nothing supplied names both",
        ),
    ],
)
def test_launcher_preconditions_satisfied_by_workplan(
    tmp_path: Path,
    compute_environment: dict[str, t.Any],
    step_overrides: list[dict[str, t.Any]],
    exp_missing: str,
) -> None:
    """SLURM settings the workplan supplies are not demanded from the environment,
    and the variables it leaves out are all named in one message.
    """
    wp = _slurm_workplan(tmp_path, compute_environment, *step_overrides)

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=mock.MagicMock()),
        ),
        mock.patch(
            "cstar.system.environment.CStarEnvironment.settings_klass",
            SlurmSettingsBase,
        ),
    ):
        if exp_missing:
            with pytest.raises(CstarExpectationFailed, match=exp_missing):
                get_launcher(wp)
        else:
            assert isinstance(get_launcher(wp), SlurmLauncher)


def test_launcher_preconditions_ignore_invalid_overrides_when_env_is_set(
    tmp_path: Path,
) -> None:
    """A step whose SLURM overrides are invalid is scheduling's problem, not the env check's."""
    wp = _slurm_workplan(tmp_path, {}, {"slurm": {"max_walltime": "1h"}})

    with (
        mock.patch.dict(
            os.environ,
            {ENV_CSTAR_SLURM_ACCOUNT: "a", ENV_CSTAR_SLURM_QUEUE: "q"},
            clear=True,
        ),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=mock.MagicMock()),
        ),
        mock.patch(
            "cstar.system.environment.CStarEnvironment.settings_klass",
            SlurmSettingsBase,
        ),
    ):
        assert isinstance(get_launcher(wp), SlurmLauncher)
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                CstarExpectationFailed, match=f"{ENV_CSTAR_SLURM_QUEUE}$"
            ):
                get_launcher(wp)


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
        return_value=_scheduled_run(wp_path, exp_default_run_id)
    )

    with mock.patch("cstar.cli.workplan.run.build_and_run_dag", mock_build_and_run_dag):
        result = runner.invoke(
            app,
            [wp_path.as_posix()],
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
            [wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code != 0
    assert "improper" in result.stderr
    assert "formatted" in result.stderr
    mock_build_and_run_dag.assert_not_awaited()


def test_workplan_run_reload_invokes_run_dag_not_build_and_run_dag(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify the reload path (no workplan path argument, `--run-id` given)
    calls `run_dag` with the original and transformed workplan paths
    recovered by `handle_run_reloading`, instead of `build_and_run_dag`.
    """
    run_id = mock_run_id
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"

    fake_run_result = _recorded_run(run_id, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_reload,
        mock.patch(
            "cstar.cli.workplan.run.run_dag",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag",
            wraps=fake_build_and_run_dag,
        ) as mock_build_and_run_dag,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", run_id],
            color=False,
        )

    assert result.exit_code == 0
    mock_reload.assert_awaited_once_with(run_id)
    mock_build_and_run_dag.assert_not_awaited()

    mock_run_dag.assert_awaited_once()
    assert mock_run_dag.await_args is not None
    call_args = mock_run_dag.await_args.args
    assert call_args[0] == wp_path
    assert call_args[1] == trx_path
    assert call_args[2] == run_id
    assert isinstance(call_args[3], Planner)


def test_workplan_run_reload_refuses_unusable_external_dependency(
    tmp_path: Path,
    mock_run_id: str,
    external_workplan: Workplan,
) -> None:
    """Verify the reload path stops before planning when an external step it
    depends on can no longer be depended upon (e.g. it has since failed).
    """
    live_steps = [LiveStep.from_step(step) for step in external_workplan.steps]
    lwp = LiveWorkplan(
        **external_workplan.model_dump(exclude={"steps"}), steps=live_steps
    )
    trx_path = tmp_path / "consumer_transformed.yaml"
    assert serialize(trx_path, lwp)
    fake_run_result = WorkplanRun(
        output_path=tmp_path,
        run_id=mock_run_id,
    )
    external = mock.Mock()
    external.problem.return_value = "step 'outer' is Failed"

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ),
        mock.patch("cstar.cli.workplan.run.get_launcher"),
        mock.patch(
            "cstar.cli.workplan.run.load_external_runs",
            mock.AsyncMock(return_value=external),
        ),
        mock.patch("cstar.cli.workplan.run.run_dag", mock.AsyncMock()) as mock_run_dag,
    ):
        result = CliRunner().invoke(app, ["--run-id", mock_run_id], color=False)

    assert result.exit_code == 1, result.output
    assert "External dependency outer@spinup: step 'outer' is Failed" in " ".join(
        result.output.split()
    )
    mock_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_with_path_reloads_by_derived_run_id(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `--resume` combined with a workplan path derives the run-id from
    the workplan's name and reloads that run, once the path is verified
    against the recorded original-workplan backup.
    """
    run_id = "sample-workplan"
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"
    assert serialize(original_workplan_backup(wp_path, tmp_path), wp), (
        "serializing the original workplan backup failed in test"
    )

    fake_run_result = _recorded_run(run_id, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_reload,
        mock.patch(
            "cstar.cli.workplan.run.run_dag",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.load_run_state",
            mock.AsyncMock(return_value=DagStatus({})),
        ),
        mock.patch("cstar.cli.workplan.run.refresh_run_manifest"),
        mock.patch(
            "cstar.cli.workplan.run.apply_resume_overrides",
            mock.AsyncMock(return_value=[]),
        ),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag",
            wraps=fake_build_and_run_dag,
        ) as mock_build_and_run_dag,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [wp_path.as_posix(), ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 0, result.output
    mock_reload.assert_awaited_once_with(run_id)
    mock_build_and_run_dag.assert_not_awaited()

    mock_run_dag.assert_awaited_once()
    assert mock_run_dag.await_args is not None
    call_args = mock_run_dag.await_args.args
    assert call_args[0] == wp_path
    assert call_args[1] == trx_path
    assert call_args[2] == run_id


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_with_explicit_run_id_and_path(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify `--resume` combined with an explicit `--run-id` and a workplan
    path reloads that run-id, rather than one derived from the path.
    """
    run_id = mock_run_id
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"
    assert serialize(original_workplan_backup(wp_path, tmp_path), wp), (
        "serializing the original workplan backup failed in test"
    )

    fake_run_result = _recorded_run(run_id, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_reload,
        mock.patch(
            "cstar.cli.workplan.run.run_dag",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.load_run_state",
            mock.AsyncMock(return_value=DagStatus({})),
        ),
        mock.patch("cstar.cli.workplan.run.refresh_run_manifest"),
        mock.patch(
            "cstar.cli.workplan.run.apply_resume_overrides",
            mock.AsyncMock(return_value=[]),
        ),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag",
            wraps=fake_build_and_run_dag,
        ) as mock_build_and_run_dag,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", run_id, wp_path.as_posix(), ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 0, result.output
    mock_reload.assert_awaited_once_with(run_id)
    mock_build_and_run_dag.assert_not_awaited()

    mock_run_dag.assert_awaited_once()
    assert mock_run_dag.await_args is not None
    call_args = mock_run_dag.await_args.args
    assert call_args[0] == wp_path
    assert call_args[1] == trx_path
    assert call_args[2] == run_id


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_with_changed_workplan_fails_fast(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `--resume` combined with a path that no longer matches the
    recorded original-workplan backup exits with a usage error before
    `run_dag` or `build_and_run_dag` is invoked.
    """
    run_id = "sample-workplan"
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"

    changed = wp.model_copy(update={"description": "changed since the run started"})
    assert serialize(original_workplan_backup(wp_path, tmp_path), changed), (
        "serializing the original workplan backup failed in test"
    )

    fake_run_result = _recorded_run(run_id, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ),
        mock.patch(
            "cstar.cli.workplan.run.run_dag",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag",
            wraps=fake_build_and_run_dag,
        ) as mock_build_and_run_dag,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [wp_path.as_posix(), ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 2
    assert "differs" in result.output
    assert "--run-id" in result.output
    mock_run_dag.assert_not_awaited()
    mock_build_and_run_dag.assert_not_awaited()


def _write_carried_workplan(
    output_path: Path, workplan: Workplan, workplan_path: Path | None = None
) -> Path:
    """Record an attempt that carried `workplan` into the run at `output_path`.

    The manifest names `workplan_path` as the authored workplan when given.
    """
    start = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
    attempt_dir = create_attempt_dir(output_path, start)
    carried = attempt_dir / ATTEMPT_WORKPLAN_NAME
    assert serialize(carried, workplan), "serializing the carried workplan failed"
    manifest = RunManifest(
        run_id="sample-workplan",
        name="sample-workplan",
        kind="workplan",
        created_at=start,
        workplan=(
            WorkplanIdentity(
                name="sample-workplan", content_hash="", path=str(workplan_path)
            )
            if workplan_path is not None
            else None
        ),
        attempts=[
            Attempt(
                start_at=start,
                directory=attempt_dir.relative_to(output_path).as_posix(),
            )
        ],
        steps=[],
    )
    write_run_manifest(output_path, manifest)
    return carried


def _resume_with_path(
    tmp_path: Path, wp_path: Path
) -> tuple[t.Any, mock.AsyncMock, WorkplanRun]:
    """Invoke `run <wp_path> --resume` against a recorded run in `tmp_path`."""
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"

    fake_run_result = _recorded_run("sample-workplan", wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ),
        mock.patch(
            "cstar.cli.workplan.run.run_dag",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.load_run_state",
            mock.AsyncMock(return_value=DagStatus({})),
        ),
        mock.patch("cstar.cli.workplan.run.refresh_run_manifest"),
        mock.patch(
            "cstar.cli.workplan.run.apply_resume_overrides",
            mock.AsyncMock(return_value=[]),
        ),
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix(), ARG_RESUME], color=False)

    return result, mock_run_dag, fake_run_result


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_without_a_recorded_workplan_warns_and_resumes(
    tmp_path: Path,
    wp_templates_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify `--resume` combined with a path resumes the run as recorded, with a
    warning, when no copy of the workplan is on disk to verify the path against:
    older attempts may be deleted by hand.
    """
    wp_path = wp_templates_dir / "workplan.yaml"

    with caplog.at_level("WARNING"):
        result, mock_run_dag, _ = _resume_with_path(tmp_path, wp_path)

    assert result.exit_code == 0, result.output
    mock_run_dag.assert_awaited_once()
    assert "No recorded workplan of run 'sample-workplan'" in caplog.text
    assert "resuming the run's recorded plan" in caplog.text


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_refuses_a_run_whose_manifest_cannot_be_read(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `--resume` combined with a path is refused with a usage error when
    the run's manifest cannot be read, rather than resumed unverified.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    (tmp_path / RUN_MANIFEST_NAME).write_text("name: [unclosed\n")

    result, mock_run_dag, _ = _resume_with_path(tmp_path, wp_path)

    assert result.exit_code == 2
    assert "Unable to verify" in result.output
    mock_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_verifies_the_path_against_the_carried_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify `--resume` combined with a path compares it with the workplan the
    latest preparing attempt carried, not with a legacy backup.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    carried = _write_carried_workplan(tmp_path, wp, wp_path)
    # a stale legacy backup never decides the comparison
    changed = wp.model_copy(update={"description": "a stale legacy backup"})
    assert serialize(original_workplan_backup(wp_path, tmp_path), changed)

    result, mock_run_dag, _ = _resume_with_path(tmp_path, wp_path)
    assert result.exit_code == 0, result.output
    mock_run_dag.assert_awaited_once()

    assert serialize(carried, changed)
    result, mock_run_dag, _ = _resume_with_path(tmp_path, wp_path)
    assert result.exit_code == 2
    assert "differs" in result.output
    assert str(carried) in squash_cli_output(result.output)
    mock_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_with_unreadable_backup_fails_fast(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `--resume` combined with a path exits with a usage error, not a
    traceback, when the recorded original-workplan backup cannot be parsed.
    """
    run_id = "sample-workplan"
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"
    original_workplan_backup(wp_path, tmp_path).write_text("name: [truncated")

    fake_run_result = _recorded_run(run_id, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ),
        mock.patch(
            "cstar.cli.workplan.run.run_dag",
            mock.AsyncMock(return_value=fake_run_result),
        ) as mock_run_dag,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [wp_path.as_posix(), ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 2, result.output
    assert "Unable to verify" in result.output
    assert "--run-id" in result.output
    mock_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_with_path_no_record(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `--resume` combined with a path whose derived run-id has no
    recorded run fails with the same "no runs" error as an unknown
    `--run-id`, before `build_and_run_dag` is invoked.
    """
    state_dir = tmp_path / "state"
    wp_path = wp_templates_dir / "workplan.yaml"

    with (
        mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: state_dir.as_posix()}),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag",
            wraps=fake_build_and_run_dag,
        ) as mock_build_and_run_dag,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [wp_path.as_posix(), ARG_RESUME],
            color=False,
        )

    assert result.exit_code != 0
    assert "runs with the id" in result.stderr
    mock_build_and_run_dag.assert_not_awaited()


@pytest.mark.parametrize("flag", ["var", "varfile"])
def test_workplan_run_resume_with_vars_fails_fast(
    tmp_path: Path,
    flag: str,
) -> None:
    """Verify `--resume` combined with `--var` or `--varfile` exits with a
    usage error before `build_and_run_dag` is invoked: `--resume` re-enters
    a prior run with its recorded variables.
    """
    if flag == "var":
        var_args = ["--var", "key=value"]
    else:
        varfile_path = tmp_path / "vars.txt"
        varfile_path.write_text("key=value\n")
        var_args = ["--varfile", varfile_path.as_posix()]

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", ARG_RESUME, *var_args],
            color=False,
        )

    assert result.exit_code == 2
    assert "--resume" in result.output
    assert "--var/--varfile" in result.output
    mock_build_and_run_dag.assert_not_awaited()


def test_workplan_run_resume_and_clobber_fails_fast() -> None:
    """Verify `--resume` combined with `--clobber` exits with a usage error
    before `build_and_run_dag` is invoked.
    """
    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", ARG_RESUME, ARG_CLOBBER, "some-step"],
            color=False,
        )

    assert result.exit_code == 2
    assert "--resume" in result.output
    assert "--clobber" in result.output
    mock_build_and_run_dag.assert_not_awaited()


def test_workplan_run_resume_reload_invokes_apply_resume_overrides(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify `--run-id` with `--resume` (no workplan path) calls
    `apply_resume_overrides` on the reloaded workplan, and never falls
    through to `build_and_run_dag`.
    """
    run_id = mock_run_id
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"

    loaded = DagStatus({"first": Status.Failed})
    fake_run_result = _recorded_run(run_id, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=fake_run_result),
        ),
        mock.patch(
            "cstar.cli.workplan.run.run_dag",
            mock.AsyncMock(return_value=fake_run_result),
        ),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag",
            wraps=fake_build_and_run_dag,
        ) as mock_build_and_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.load_run_state",
            mock.AsyncMock(return_value=loaded),
        ) as mock_load_state,
        mock.patch("cstar.cli.workplan.run.refresh_run_manifest") as mock_refresh,
        mock.patch(
            "cstar.cli.workplan.run.apply_resume_overrides",
            mock.AsyncMock(return_value=[]),
        ) as mock_apply_resume,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", run_id, ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 0, result.output
    mock_build_and_run_dag.assert_not_awaited()
    # the state is loaded once, recorded in the manifest, then applied
    mock_load_state.assert_awaited_once()
    assert mock_load_state.await_args is not None
    assert mock_load_state.await_args.args[0] == run_id
    mock_refresh.assert_called_once_with(tmp_path, run_id, loaded)
    mock_apply_resume.assert_awaited_once()
    assert mock_apply_resume.await_args is not None
    assert mock_apply_resume.await_args.args[1] is loaded


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
    lwp_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
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

    fake_run = _recorded_run(
        run_id,
        wp_path,
        lwp_path.parent,
        environment={"CSTAR_LOG_LEVEL": "TRACE"},
        sentinels=sentinel_paths,
    )

    repo = TrackingRepository()
    repo.put_workplan_run_sync(fake_run)

    def typer_exit(*args, **kwargs) -> None:  # type: ignore
        raise typer.Exit(1)

    runner = CliRunner()
    with (
        mock.patch(
            "cstar.orchestration.dag_runner.get_launcher",
            _slurm_launcher,
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


@pytest.mark.parametrize("resume", [False, True])
def test_workplan_run_reenters_a_json_workplan_run(
    tmp_path: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
    resume: bool,
) -> None:
    """Verify a run started from a workplan written as JSON, which reads as YAML,
    re-enters by run-id, alone or resuming with the authored path, by loading the
    plan prepared in its run root.
    """
    run_id = mock_run_id
    wp = Workplan(
        name="json",
        description="A workplan written as JSON.",
        steps=[
            Step(
                name="hello",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            )
        ],
    )
    wp_path = tmp_path / "plan.json"
    wp_path.write_text(json.dumps(wp.model_dump(mode="json", by_alias=True)))
    prepared, trx_path, _, _ = asyncio.run(prepare_workplan(wp_path, tmp_path / "run"))

    sentinel_paths = set[Path]()
    for step in prepared.steps:
        handle = LocalHandle(
            pid="1000",
            name=LiveStep.from_step(step).safe_name,
            run_id=run_id,
            status=Status.Done,
            start_at=datetime.now(),
        )
        path = StateRepository.sentinel_path(handle)
        assert serialize(path, handle), "serializing the mock handles failed in test"
        sentinel_paths.add(path)

    TrackingRepository().put_workplan_run_sync(
        _recorded_run(run_id, wp_path, trx_path.parent, sentinels=sentinel_paths)
    )

    args = [ARG_RUN_ID, run_id]
    if resume:
        args += [ARG_RESUME, str(wp_path)]

    with (
        mock.patch("cstar.orchestration.dag_runner.get_launcher", _slurm_launcher),
        mock.patch(
            "cstar.orchestration.launch.slurm.SlurmLauncher.query_status",
            mock.AsyncMock(return_value=Status.Done),
        ),
        mock.patch(
            "cstar.orchestration.launch.slurm.SlurmLauncher._submit"
        ) as mock_submit,
    ):
        result = CliRunner().invoke(app, args, color=False)

    assert result.exit_code == 0, result.output
    assert not mock_submit.called


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
    lwp_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
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

    fake_run = _recorded_run(
        run_id,
        wp_path,
        lwp_path.parent,
        environment={"CSTAR_LOG_LEVEL": "TRACE"},
        sentinels=sentinel_paths,
    )

    repo = TrackingRepository()
    repo.put_workplan_run_sync(fake_run)

    def typer_exit(*args, **kwargs) -> None:  # type: ignore
        raise typer.Exit(1)

    runner = CliRunner()
    with (
        mock.patch(
            "cstar.orchestration.dag_runner.get_launcher",
            _slurm_launcher,
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
    lwp_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
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

    fake_run = _recorded_run(
        run_id,
        wp_path,
        lwp_path.parent,
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
            _slurm_launcher,
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
        return_value=_recorded_run(
            "not-used", wp_path, tmp_path, environment={}, user_variables={}
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
    assert wp.name == "say-hello-to-my-little-friend-host"
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
    """Verify submitting a blueprint to `cstar workplan submit` generates a host
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
        name="migration-test-workplan",
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
    latest = get_application("plotter").schema_version
    return state_dir / f"{bp_path.stem}_{latest}{bp_path.suffix}"


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


@pytest.mark.parametrize(
    ("placeholder", "runtime_vars", "runs", "depends_on", "cli_vars"),
    [
        pytest.param(
            "{{bp_dir}}",
            ["bp_dir"],
            {},
            [],
            ["--var", "bp_dir=/filled/at/prepare"],
            id="runtime-variable",
        ),
        pytest.param(
            "{{output_dir: make_inputs@toy}}",
            [],
            {"toy": "wio-toy-smoke"},
            ["make_inputs@toy"],
            [],
            id="external-step-scope",
        ),
    ],
)
def test_workplan_run_placeholder_blueprint_path_skips_preflight_migration(
    tmp_path: Path,
    plotter_v1_0_0_bp: Path,
    placeholder: str,
    runtime_vars: list[str],
    runs: dict[str, str],
    depends_on: list[str],
    cli_vars: list[str],
) -> None:
    """Verify a blueprint path still holding a `{{ }}` placeholder is not
    rejected by the `PATH` argument callback: the file it names only exists
    once the placeholder is filled while preparing the workplan, so preflight
    migration must skip it (claude-docs#45). Migration of the other steps is
    unaffected.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_bp : Path
        Fixture providing the path to a plotter blueprint at schema 1.0.0
    placeholder : str
        The placeholder spelling used in the blueprint path
    runtime_vars : list[str]
        Runtime variables declared by the workplan
    runs : dict[str, str]
        External runs declared by the workplan, keyed by alias
    depends_on : list[str]
        Dependencies of the placeholder step
    cli_vars : list[str]
        `--var` arguments supplied on the command line
    """
    placeholder_bp = f"{placeholder}/helloworld.yaml"
    wp = Workplan(
        name="placeholder-workplan",
        description="A step whose blueprint path is filled at prepare time.",
        runtime_vars=runtime_vars,
        runs=runs,
        steps=[
            Step(name="Plot", application="plotter", blueprint=plotter_v1_0_0_bp),
            Step(
                name="Say Hello",
                application="hello_world",
                blueprint=placeholder_bp,
                depends_on=depends_on,
            ),
        ],
    )
    wp_path = tmp_path / "placeholder-workplan.yaml"
    assert serialize(wp_path, wp), "serializing test workplan failed"

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix(), *cli_vars],
            color=False,
        )

    assert result.exit_code == 0, result.output
    assert "Blueprint not found" not in result.output
    mock_build_and_run_dag.assert_awaited_once()

    # the placeholder path reaches the run untouched while the stale sibling
    # blueprint is still migrated
    handed_over = deserialize(mock_build_and_run_dag.call_args.args[0], Workplan)
    assert [str(s.blueprint_path) for s in handed_over.steps] == [
        _expected_migrated_path(plotter_v1_0_0_bp).as_posix(),
        placeholder_bp,
    ]


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


def test_workplan_run_unsupported_schema_fails(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
) -> None:
    """Verify the run exits non-zero when a step's blueprint declares a schema
    version with no registered migration path, instead of scheduling a
    blueprint that cannot be executed.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_model : dict[str, t.Any]
        Fixture providing the raw content of a plotter blueprint at schema 1.0.0
    """
    # no adapter migrates from 0.5.0, so planning the upgrade path must fail
    model = {**plotter_v1_0_0_model, "schema_version": "0.5.0"}
    bp_path = tmp_path / "plotter_unsupported_0.5.0.json"
    bp_path.write_text(json.dumps(model))

    step = Step(name="Plot", application="plotter", blueprint=bp_path)
    wp_path = _write_workplan(tmp_path / "unsupported-schema-workplan.yaml", [step])

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 1
    output = " ".join(result.stdout.split())
    assert (
        f"Blueprint {bp_path.as_posix()!r}: plotter schema 0.5.0 has no automatic migration"
        in output
    )
    mock_build_and_run_dag.assert_not_awaited()


def test_workplan_run_unsupported_schemas_all_reported(
    tmp_path: Path,
    plotter_v1_0_0_bp: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
) -> None:
    """Verify every step with an unsupported schema is reported in one run, and
    that the workplan is left untouched even though an earlier step migrated.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_bp : Path
        Fixture providing a plotter blueprint that migrates automatically
    plotter_v1_0_0_model : dict[str, t.Any]
        Fixture providing the raw content of a plotter blueprint at schema 1.0.0
    """
    manual_path = tmp_path / "plotter_0.5.0.json"
    manual_path.write_text(
        json.dumps({**plotter_v1_0_0_model, "schema_version": "0.5.0"})
    )
    too_new_path = tmp_path / "plotter_9.9.9.json"
    too_new_path.write_text(
        json.dumps({**plotter_v1_0_0_model, "schema_version": "9.9.9"})
    )

    steps = [
        Step(name="Migrates", application="plotter", blueprint=plotter_v1_0_0_bp),
        Step(name="Manual", application="plotter", blueprint=manual_path),
        Step(name="TooNew", application="plotter", blueprint=too_new_path),
    ]
    wp_path = _write_workplan(tmp_path / "unsupported-schemas-workplan.yaml", steps)
    original = wp_path.read_bytes()

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 1
    output = " ".join(result.stdout.split())
    assert f"Blueprint {manual_path.as_posix()!r}: plotter schema 0.5.0" in output
    assert f"Blueprint {too_new_path.as_posix()!r}: plotter schema 9.9.9" in output
    assert "Upgrade cstar-ocean" in output
    assert wp_path.read_bytes() == original
    mock_build_and_run_dag.assert_not_awaited()


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
    flatten_cli_output: Callable[[str], str],
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify a workplan step referencing a non-existent blueprint fails with
    an error naming the missing blueprint -- not the workplan, which exists.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    flatten_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for phrase matching
    squash_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for path matching
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

    stderr_flat = flatten_cli_output(result.stderr)
    assert "Blueprint not found" in stderr_flat
    assert "Workplan not found" not in stderr_flat
    # the message must name the missing blueprint
    assert missing_bp.name in squash_cli_output(result.stderr)


def test_workplan_run_step_blueprint_migration_invalid(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
    flatten_cli_output: Callable[[str], str],
    squash_cli_output: Callable[[str], str],
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
    flatten_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for phrase matching
    squash_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for path matching
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

    stderr_flat = flatten_cli_output(result.stderr)
    assert "is invalid" in stderr_flat
    assert "Details:" in stderr_flat
    # the failing blueprint (not the workplan) is named in the message
    stderr_squashed = squash_cli_output(result.stderr)
    assert bp_path.name in stderr_squashed
    assert wp_path.name not in stderr_squashed


def test_workplan_run_reports_all_directive_problems_before_submission(
    tmp_path: Path,
    hello_world_bp_path: Path,
) -> None:
    """Verify `workplan submit` rejects a workplan whose directives are
    misconfigured as a usage error listing every problem, and never reaches
    the DAG runner.

    The pipeline is exercised for real up to `prepare_workplan`; only the
    submission stage is patched out so the assertion that it was never
    reached is meaningful.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    wp = Workplan(
        name="bad-directives",
        description="Two steps, two distinct directive problems.",
        steps=[
            Step(
                name="typo",
                application="hello_world",
                blueprint=hello_world_bp_path,
                directives={"continue_from": {"path": "prior/run"}},
            ),
            Step(
                name="not-a-mapping",
                application="hello_world",
                blueprint=hello_world_bp_path,
                directives={"apply-overrides": "oops"},
            ),
        ],
    )
    wp_path = tmp_path / "bad-directives.yaml"
    assert serialize(wp_path, wp)

    with mock.patch("cstar.orchestration.dag_runner.run_dag") as mock_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "bad-directives", wp_path.as_posix()],
            color=False,
        )

    assert result.exit_code == 2, result.output
    # typer renders the usage error in a bordered panel that wraps long lines;
    # strip the border and collapse whitespace before matching phrases
    message = " ".join(result.stderr.replace("│", " ").split())
    assert "2 directive problem(s)" in message
    assert "'continue_from'" in message
    assert "'apply-overrides'" in message
    assert "must be a mapping" in message
    mock_run_dag.assert_not_called()


def test_migrate_steps_skips_inline_step(tmp_path: Path) -> None:
    """Verify an inline step has no blueprint file to migrate and is left alone."""
    wp = Workplan(
        name="inline-workplan",
        description="A workplan with an inline blueprint.",
        steps=[
            Step.model_validate(
                {
                    "name": "inline",
                    "application": "hello_world",
                    "blueprint": "inline",
                    "blueprint_overrides": {"target": "@inline"},
                },
            ),
        ],
    )

    with mock.patch("cstar.cli.workplan.run.localize_and_migrate") as mock_migrate:
        migrate_steps(tmp_path / "workplan.yaml", wp)

    mock_migrate.assert_not_called()
    assert wp.steps[0].is_inline


@pytest.mark.parametrize("pre_run", [True, False])
@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_pre_run_reaches_build_and_run_dag(pre_run: bool) -> None:
    """Verify `--pre-run` is forwarded to `build_and_run_dag` as a parameter,
    and only a pre-run ends with how to continue.
    """
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"
    args = ["--run-id", "12345", wp_uri]

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app, [ARG_PRE_RUN, *args] if pre_run else args, color=False
        )

    assert result.exit_code == 0, result.output
    mock_build_and_run_dag.assert_awaited_once()
    assert mock_build_and_run_dag.await_args is not None
    assert mock_build_and_run_dag.await_args.kwargs["pre_run"] is pre_run

    output = " ".join(result.stdout.split())
    assert ("Pre-run scheduled." in output) is pre_run
    assert ("without --pre-run" in output) is pre_run
    assert "run scheduling has completed" in output


def test_workplan_run_pre_run_and_resume_fails_fast() -> None:
    """Verify `--pre-run` combined with `--resume` exits with one usage error
    before `build_and_run_dag` is invoked.
    """
    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        wraps=fake_build_and_run_dag,
    ) as mock_build_and_run_dag:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["--run-id", "12345", ARG_RESUME, ARG_PRE_RUN],
            color=False,
        )

    assert result.exit_code == 2
    assert "--resume" in result.output
    assert "--pre-run" in result.output
    mock_build_and_run_dag.assert_not_awaited()


def test_workplan_run_pre_run_requires_workplan_path() -> None:
    """Verify `--pre-run` without a workplan path (a `--run-id` reload would
    replay the recorded plan and silently ignore it) is a usage error.
    """
    with mock.patch(
        "cstar.cli.workplan.run.handle_run_reloading", mock.AsyncMock()
    ) as mock_reload:
        runner = CliRunner()
        result = runner.invoke(app, ["--run-id", "12345", ARG_PRE_RUN], color=False)

    assert result.exit_code == 2
    assert "requires a workplan path" in " ".join(
        result.output.replace("│", " ").split()
    )
    mock_reload.assert_not_awaited()


def test_workplan_run_refuses_a_removed_dry_run_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `workplan submit` exits 2 without reloading or launching anything
    while a removed setting that used to request a plan-only run is exported.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to export the setting.
    """
    name = next(n for n, removed in REMOVED_SETTINGS.items() if removed.refused)
    monkeypatch.setenv(name, FLAG_ON)

    with mock.patch(
        "cstar.cli.workplan.run.handle_run_reloading", mock.AsyncMock()
    ) as mock_reload:
        result = CliRunner().invoke(app, ["--run-id", "12345"], color=False)

    assert result.exit_code == 2
    assert name in result.stderr
    mock_reload.assert_not_awaited()


def test_workplan_run_refuses_a_removed_dry_run_setting_in_the_file(
    isolate_user_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `workplan submit` exits 2 without reloading or launching anything
    while the configuration file names a removed setting that used to request a
    plan-only run: the loader skips the line, which must not turn the plan-only
    run into a real one.

    Parameters
    ----------
    isolate_user_config : Path
        The configuration file the loader reads for this test.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to clear the setting from the environment.
    """
    name = next(n for n, removed in REMOVED_SETTINGS.items() if removed.refused)
    monkeypatch.delenv(name, raising=False)
    isolate_user_config.write_text(f"{name}={FLAG_ON}\n")

    with mock.patch(
        "cstar.cli.workplan.run.handle_run_reloading", mock.AsyncMock()
    ) as mock_reload:
        result = CliRunner().invoke(app, ["--run-id", "12345"], color=False)

    assert result.exit_code == 2
    assert f"Refusing to run while {name}" in result.stderr
    assert str(isolate_user_config) in result.stderr.replace("\n", "")
    mock_reload.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_pre_run_with_nothing_to_prepare_exits_cleanly() -> None:
    """Verify a pre-run that leaves no step to prepare reports it and exits 0."""
    wp_uri = "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/cstar/additional_files/templates/wp/workplan.yaml"
    msg = "No step can be prepared in pre-run mode; nothing was scheduled."

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag",
        mock.AsyncMock(side_effect=NoPreparableStepsError(msg)),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app, ["--run-id", "12345", ARG_PRE_RUN, wp_uri], color=False
        )

    assert result.exit_code == 0, result.output
    assert "nothing was scheduled" in " ".join(result.stdout.split())
    assert "run scheduling has completed" not in result.stdout


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_resume_of_pre_run_workplan_fails_fast(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify `--resume` on the reloaded workplan of a pre-run is a usage
    error: resume would mark steps that are also marked for pre-run.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    live_steps = [
        LiveStep.from_step(step, update={"workflow_overrides": {KEY_PRE_RUN: True}})
        for step in wp.steps
    ]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / f"{wp_path.stem}_transformed.yaml"
    assert serialize(trx_path, lwp)

    run = _recorded_run(mock_run_id, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=run),
        ),
        mock.patch("cstar.cli.workplan.run.run_dag", mock.AsyncMock()) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.apply_resume_overrides", mock.AsyncMock()
        ) as mock_apply_resume,
    ):
        runner = CliRunner()
        result = runner.invoke(app, ["--run-id", mock_run_id, ARG_RESUME], color=False)

    assert result.exit_code == 2
    assert "is a pre-run" in " ".join(result.output.replace("│", " ").split())
    mock_apply_resume.assert_not_awaited()
    mock_run_dag.assert_not_awaited()


RUN_LOOKUP = "cstar.cli.workplan.run.TrackingRepository.get_workplan_run"
"""The run-record lookup made by the run command."""


def _recorded_run(
    run_id: str, workplan_path: Path, output_path: Path, **overrides: t.Any
) -> WorkplanRun:
    """A record of a prior run of `run_id` that started from `workplan_path`.

    The run directory holds the manifest that says so, and an empty transformed
    workplan beside it.
    """
    _place_transformed(workplan_path, output_path, run_id)
    return WorkplanRun(output_path=output_path, run_id=run_id, **overrides)


def _write_original_backup(record: WorkplanRun, workplan: Workplan) -> None:
    """Persist `workplan` as the original backup of the run `record` describes."""
    backup = original_workplan_backup(record.workplan_path, record.output_path)
    assert serialize(backup, workplan), "serializing the original backup failed"


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_refuses_derived_run_id_owned_by_another_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
    squash_cli_output: Callable[[str], str],
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a derived run-id that a different workplan, at a different path,
    already uses is refused before anything is built, naming both files, why,
    and the way out.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    other_path = tmp_path / "elsewhere" / "workplan.yaml"
    record = _recorded_run("sample-workplan", other_path, tmp_path)
    other = deserialize(wp_path, Workplan).model_copy(
        update={"description": "a different workplan with the same name"}
    )
    _write_original_backup(record, other)

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)) as mock_lookup,
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 2
    mock_lookup.assert_awaited_once_with("sample-workplan")
    mock_build_and_run_dag.assert_not_awaited()

    squashed = squash_cli_output(result.output)
    assert str(other_path) in squashed
    assert str(wp_path) in squashed
    assert f"{ARG_RUN_ID}<other-id>" in squashed
    assert f"{ARG_RUN_ID}sample-workplan" in squashed
    assert "its content differs from the workplan recorded for that run" in (
        flatten_cli_output(result.output)
    )
    assert "rename" in result.output


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_reentered_by_the_same_workplan_elsewhere(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify the recorded workplan arriving by another path, as every remote
    URI does (each fetch is a fresh temp copy) and a moved checkout does, is not
    a collision: its content matches the recorded original backup.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run(
        "sample-workplan", tmp_path / "earlier-copy" / "workplan.yaml", tmp_path
    )
    _write_original_backup(record, deserialize(wp_path, Workplan))

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 0, result.output
    mock_build_and_run_dag.assert_awaited_once()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_reentered_from_a_legacy_backup(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify a recorded original whose free-form name predates the slug rule
    still compares by content: the same workplan under its slug is not a
    collision.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run(
        "sample-workplan", tmp_path / "earlier-copy" / "workplan.yaml", tmp_path
    )
    _write_original_backup(record, deserialize(wp_path, Workplan))
    backup = original_workplan_backup(record.workplan_path, record.output_path)
    legacy = backup.read_text().replace(
        "name: sample-workplan", "name: Sample Workplan", 1
    )
    assert legacy != backup.read_text()
    backup.write_text(legacy)

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 0, result.output
    mock_build_and_run_dag.assert_awaited_once()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_refused_with_an_unreadable_backup(
    tmp_path: Path,
    wp_templates_dir: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a recorded original that exists but cannot be parsed is refused
    with a message that says so, not that the backup is missing.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run("sample-workplan", tmp_path / "gone.yaml", tmp_path)
    backup = original_workplan_backup(record.workplan_path, record.output_path)
    backup.write_text("name: [unclosed\n")

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 2
    mock_build_and_run_dag.assert_not_awaited()
    output = flatten_cli_output(result.output)
    assert "the recorded workplan for that run could not be read" in output
    assert "no recorded workplan to compare" not in output
    assert f"{ARG_RUN_ID} <other-id> or {ARG_RUN_ID} sample-workplan" in output


def test_refuse_run_id_collision_reports_an_unreadable_supplied_file(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify an error in the supplied file, which is the user's, is reported as
    that file's error rather than blamed on the recorded run.
    """
    record = _recorded_run("sample-workplan", tmp_path / "earlier.yaml", tmp_path)
    _write_original_backup(
        record, deserialize(wp_templates_dir / "workplan.yaml", Workplan)
    )
    supplied = tmp_path / "supplied.yaml"
    supplied.write_text("name: sample-workplan\ndescription: 7\n")

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        pytest.raises(typer.BadParameter, match="could not be read") as error,
    ):
        refuse_run_id_collision(supplied, "sample-workplan", explicit=False)

    assert "supplied.yaml" in error.value.message
    assert "backup" not in error.value.message


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_refused_without_a_backup_to_compare(
    tmp_path: Path,
    wp_templates_dir: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a different path whose recorded run has no original backup is
    refused, since the two workplans cannot be compared, and the way out is
    named.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run("sample-workplan", tmp_path / "gone.yaml", tmp_path)
    assert not original_workplan_backup(
        record.workplan_path, record.output_path
    ).exists()

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 2
    mock_build_and_run_dag.assert_not_awaited()
    output = flatten_cli_output(result.output)
    assert (
        f"no recorded workplan to compare; pass {ARG_RUN_ID} <other-id> or "
        f"{ARG_RUN_ID} sample-workplan to re-enter"
    ) in output


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_compared_with_the_carried_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a derived run-id is compared with the workplan the latest preparing
    attempt carried: the same workplan by another path re-enters the run, a
    different one is refused.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run(
        "sample-workplan", tmp_path / "earlier-copy" / "workplan.yaml", tmp_path
    )
    wp = deserialize(wp_path, Workplan)
    carried = _write_carried_workplan(tmp_path, wp)

    def invoke() -> tuple[t.Any, mock.MagicMock]:
        with (
            mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
            mock.patch(
                "cstar.cli.workplan.run.build_and_run_dag",
                wraps=fake_build_and_run_dag,
            ) as mock_build_and_run_dag,
        ):
            result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)
        return result, mock_build_and_run_dag

    result, mock_build_and_run_dag = invoke()
    assert result.exit_code == 0, result.output
    mock_build_and_run_dag.assert_awaited_once()

    other = wp.model_copy(update={"description": "a different workplan"})
    assert serialize(carried, other)
    result, mock_build_and_run_dag = invoke()
    assert result.exit_code == 2
    assert "its content differs" in flatten_cli_output(result.output)
    mock_build_and_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_refused_when_the_carried_workplan_is_gone(
    tmp_path: Path,
    wp_templates_dir: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a run whose latest carried workplan was deleted is refused under a
    derived run-id even when a matching legacy backup remains: a deleted
    history must not let a different workplan overwrite the run.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run("sample-workplan", tmp_path / "gone.yaml", tmp_path)
    wp = deserialize(wp_path, Workplan)
    _write_carried_workplan(tmp_path, wp).unlink()
    _write_original_backup(record, wp)

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 2
    mock_build_and_run_dag.assert_not_awaited()
    assert "no recorded workplan to compare" in flatten_cli_output(result.output)


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_refused_when_the_manifest_cannot_be_read(
    tmp_path: Path,
    wp_templates_dir: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a derived run-id whose run has an unreadable manifest is refused
    with a message naming the manifest, not one saying nothing is recorded.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run("sample-workplan", tmp_path / "gone.yaml", tmp_path)
    (tmp_path / RUN_MANIFEST_NAME).write_text("name: [unclosed\n")
    _write_original_backup(record, deserialize(wp_path, Workplan))

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 2
    mock_build_and_run_dag.assert_not_awaited()
    output = flatten_cli_output(result.output)
    assert "the manifest of that run could not be read" in output
    assert "no recorded workplan to compare" not in output


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_reentered_by_the_same_file(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify re-running the file a run started from is not a collision,
    however the recorded path is spelled.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    link_path = tmp_path / "linked.yaml"
    link_path.symlink_to(wp_path)
    record = _recorded_run("sample-workplan", link_path, tmp_path)

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 0, result.output
    mock_build_and_run_dag.assert_awaited_once()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_derived_run_id_without_a_prior_run(
    wp_templates_dir: Path,
) -> None:
    """Verify a derived run-id with no recorded run is not a collision."""
    wp_path = wp_templates_dir / "workplan.yaml"

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=None)),
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 0, result.output
    mock_build_and_run_dag.assert_awaited_once()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_explicit_run_id_may_reenter_another_files_run(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify `--run-id` is the user's choice to use the run-id, so the
    collision check is not made at all, even when the id matches the derived one.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    record = _recorded_run("sample-workplan", tmp_path / "other.yaml", tmp_path)

    with (
        mock.patch(RUN_LOOKUP, mock.AsyncMock(return_value=record)) as mock_lookup,
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(
            app, [ARG_RUN_ID, "sample-workplan", wp_path.as_posix()], color=False
        )

    assert result.exit_code == 0, result.output
    mock_lookup.assert_not_awaited()
    mock_build_and_run_dag.assert_awaited_once()


def test_workplan_run_run_id_in_place_of_path_names_the_right_command(
    tmp_path: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify `--resume <run-id>`, which binds the id to the workplan path, is
    refused with the command that was meant.
    """
    repo = TrackingRepository()
    repo.put_workplan_run_sync(
        _recorded_run("my-run", tmp_path / "workplan.yaml", tmp_path)
    )

    result = CliRunner().invoke(app, [ARG_RESUME, "my-run"], color=False)

    assert result.exit_code == 2
    output = flatten_cli_output(result.output)
    assert "'my-run' is a run-id, not a workplan file" in output
    assert f"cstar workplan {COMMAND_SUBMIT} {ARG_RUN_ID} my-run {ARG_RESUME}" in output


@pytest.mark.parametrize("value", ["no-such-run", "typo.yaml", "dir/typo", "!!!"])
def test_workplan_run_missing_path_that_is_not_a_run_id(
    value: str,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a missing path stays a "not found" error when no run has that id,
    that a value that is not a run-id is not looked up as one, and that looking
    one up takes no lock.
    """
    repo = TrackingRepository()
    with mock.patch.object(
        TrackingRepository,
        "latest_path",
        autospec=True,
        side_effect=TrackingRepository.latest_path,
    ) as mock_lookup:
        result = CliRunner().invoke(app, [value], color=False)

    assert result.exit_code == 2
    assert "Workplan not found at path" in flatten_cli_output(result.output)
    assert mock_lookup.call_count == (1 if value == "no-such-run" else 0)
    assert not list(repo.latest_dir.glob("*.lock"))


def test_workplan_run_missing_path_when_the_run_lookup_fails(
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify an unreadable run store leaves the "not found" error as it was,
    instead of raising out of the handler for the missing file.
    """
    with mock.patch.object(
        TrackingRepository, "latest_path", side_effect=PermissionError("denied")
    ):
        result = CliRunner().invoke(app, ["some-run"], color=False)

    assert result.exit_code == 2
    assert "Workplan not found at path" in flatten_cli_output(result.output)


def test_workplan_run_non_slug_name_names_the_slug(
    tmp_path: Path,
    wp_templates_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a workplan whose name is not a slug is refused up front, and the
    slug to use is reported.
    """
    content = (wp_templates_dir / "workplan.yaml").read_text()
    wp_path = tmp_path / "workplan.yaml"
    wp_path.write_text(
        content.replace("name: sample-workplan", "name: Sample Workplan")
    )

    with mock.patch(
        "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
    ) as mock_build_and_run_dag:
        result = CliRunner().invoke(app, [wp_path.as_posix()], color=False)

    assert result.exit_code == 2
    assert "'sample-workplan'" in caplog.text
    mock_build_and_run_dag.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_run_reload_reports_orphans(
    tmp_path: Path,
    wp_templates_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify re-entering a run by id reports the step directories that no step
    of the recorded workplan accounts for.
    """
    run_id = "sample-workplan"
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    run_root = tmp_path / "run-root"
    live_steps = [
        LiveStep.from_step(
            step, update={"working_dir": run_root / "tasks" / step.safe_name}
        )
        for step in wp.steps
    ]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    leftover = run_root / "tasks" / "renamed-step"
    leftover.mkdir(parents=True)
    record = _recorded_run(run_id, wp_path, run_root)
    assert serialize(record.trx_workplan_path, lwp)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=record),
        ),
        mock.patch("cstar.cli.workplan.run.get_launcher"),
        mock.patch(
            "cstar.cli.workplan.run.run_dag", mock.AsyncMock(return_value=record)
        ) as mock_run_dag,
        caplog.at_level("WARNING"),
    ):
        result = CliRunner().invoke(app, [ARG_RUN_ID, run_id], color=False)

    assert result.exit_code == 0, result.output
    mock_run_dag.assert_awaited_once()
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any(str(leftover) in w and "left in place" in w for w in warnings)
    assert leftover.is_dir()


def _persist_transformed_workplan(wp_path: Path, trx_path: Path) -> LiveWorkplan:
    """Persist the live form of the authored workplan at `wp_path` to `trx_path`."""
    wp = deserialize(wp_path, Workplan)
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    assert serialize(trx_path, lwp), "serializing live workplan failed in test"
    return lwp


def test_workplan_run_reenters_the_run_of_a_transformed_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify passing the transformed workplan a run persisted re-enters that
    run as `--run-id <id>` does: the run-id is the workplan name, the record is
    reloaded, and `run_dag` receives the recorded paths instead of the file
    being prepared again.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    trx_path = tmp_path / "workplan_transformed.yaml"
    lwp = _persist_transformed_workplan(wp_path, trx_path)
    record = _recorded_run(lwp.name, wp_path, tmp_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=record),
        ) as mock_reload,
        mock.patch(
            "cstar.cli.workplan.run.run_dag", mock.AsyncMock(return_value=record)
        ) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [trx_path.as_posix()], color=False)

    assert result.exit_code == 0, result.output
    mock_reload.assert_awaited_once_with(lwp.name)
    mock_build_and_run_dag.assert_not_awaited()
    mock_run_dag.assert_awaited_once()
    assert mock_run_dag.await_args is not None
    call_args = mock_run_dag.await_args.args
    assert call_args[0] == wp_path
    assert call_args[1] == trx_path
    assert call_args[2] == lwp.name
    assert isinstance(call_args[3], Planner)


def test_workplan_run_refuses_a_transformed_workplan_its_run_does_not_execute(
    tmp_path: Path,
    wp_templates_dir: Path,
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify a transformed workplan at a path other than the one the run's
    record executes (a copy elsewhere, a rotated attempt) is refused, naming
    both paths and the `--run-id` way in.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    trx_path = tmp_path / "copy" / "workplan_transformed.yaml"
    trx_path.parent.mkdir()
    lwp = _persist_transformed_workplan(wp_path, trx_path)
    recorded_trx = tmp_path / "run" / "workplan_transformed.yaml"
    record = _recorded_run(lwp.name, wp_path, tmp_path / "run")

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=record),
        ),
        mock.patch("cstar.cli.workplan.run.run_dag", mock.AsyncMock()) as mock_run_dag,
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(app, [trx_path.as_posix()], color=False)

    assert result.exit_code == 2, result.output
    mock_run_dag.assert_not_awaited()
    mock_build_and_run_dag.assert_not_awaited()
    squashed = squash_cli_output(result.output)
    assert str(trx_path) in squashed
    assert str(recorded_trx) in squashed
    assert f"{ARG_RUN_ID}{lwp.name}" in squashed


def test_workplan_run_refuses_pre_run_of_a_transformed_workplan(
    tmp_path: Path,
    wp_templates_dir: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify `--pre-run` is refused with a transformed workplan, which replays
    its recorded plan as a reload by run-id does.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    trx_path = tmp_path / "workplan_transformed.yaml"
    _persist_transformed_workplan(wp_path, trx_path)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading", mock.AsyncMock()
        ) as mock_reload,
        mock.patch(
            "cstar.cli.workplan.run.build_and_run_dag", wraps=fake_build_and_run_dag
        ) as mock_build_and_run_dag,
    ):
        result = CliRunner().invoke(
            app, [ARG_PRE_RUN, trx_path.as_posix()], color=False
        )

    assert result.exit_code == 2, result.output
    mock_reload.assert_not_awaited()
    mock_build_and_run_dag.assert_not_awaited()
    assert "needs the authored workplan" in flatten_cli_output(result.output)


def test_workplan_run_reenters_a_run_started_under_an_explicit_run_id(
    wp_templates_dir: Path,
) -> None:
    """Verify a transformed workplan inside a run directory re-enters that run,
    whose id was given explicitly when it started and so is not its name.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    run_id = "custom-id"
    run_dir = StateDirectoryManager.data_dir(run_id)
    run_dir.mkdir(parents=True)
    trx_path = run_dir / "workplan_transformed.yaml"
    lwp = _persist_transformed_workplan(wp_path, trx_path)
    assert lwp.name != run_id
    record = _recorded_run(run_id, wp_path, run_dir)

    with (
        mock.patch(
            "cstar.cli.workplan.run.handle_run_reloading",
            mock.AsyncMock(return_value=record),
        ) as mock_reload,
        mock.patch(
            "cstar.cli.workplan.run.run_dag", mock.AsyncMock(return_value=record)
        ) as mock_run_dag,
    ):
        result = CliRunner().invoke(app, [trx_path.as_posix()], color=False)

    assert result.exit_code == 0, result.output
    mock_reload.assert_awaited_once_with(run_id)
    mock_run_dag.assert_awaited_once()
    assert mock_run_dag.await_args is not None
    assert mock_run_dag.await_args.args[2] == run_id


def _record_run_with_environment(
    tmp_path: Path, run_id: str, environment: dict[str, str]
) -> None:
    """Persist a run record that captured `environment` when it started."""
    (tmp_path / "workplan_transformed.yaml").touch()
    record = WorkplanRun(
        output_path=tmp_path,
        run_id=run_id,
        environment=environment,
    )
    TrackingRepository().put_workplan_run_sync(record)


def test_reload_leaves_the_data_and_state_homes_to_the_shell(
    tmp_path: Path, mock_run_id: str
) -> None:
    """Verify a reload does not pin the run to the homes it was started under, so a
    run whose data home was copied resolves against the current one.
    """
    current_data = os.environ[ENV_CSTAR_DATA_HOME]
    current_state = os.environ[ENV_CSTAR_STATE_HOME]
    _record_run_with_environment(
        tmp_path,
        mock_run_id,
        {
            ENV_CSTAR_DATA_HOME: str(tmp_path / "old-data"),
            ENV_CSTAR_STATE_HOME: str(tmp_path / "old-state"),
            ENV_CSTAR_LOG_LEVEL: "TRACE",
        },
    )

    with mock.patch.dict(os.environ):
        asyncio.run(handle_run_reloading(mock_run_id))

        assert os.environ[ENV_CSTAR_DATA_HOME] == current_data
        assert os.environ[ENV_CSTAR_STATE_HOME] == current_state
        assert os.environ[ENV_CSTAR_LOG_LEVEL] == "TRACE"


def test_reload_re_exports_a_setting_the_config_file_supplied(
    tmp_path: Path,
    mock_run_id: str,
    isolate_user_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a run that captured a setting from the configuration file gets it
    back on a reload into a shell that lacks it.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    isolate_user_config.write_text(f"{ENV_CSTAR_SLURM_ACCOUNT}=m1234\n")
    load_user_config()
    _record_run_with_environment(tmp_path, mock_run_id, capture_environment())

    with mock.patch.dict(os.environ):
        os.environ.pop(ENV_CSTAR_SLURM_ACCOUNT)

        asyncio.run(handle_run_reloading(mock_run_id))

        assert os.environ[ENV_CSTAR_SLURM_ACCOUNT] == "m1234"


def test_reload_warns_once_about_settings_that_differ(
    tmp_path: Path, mock_run_id: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify one warning names every re-exported setting that differs from the
    shell, with both values, and never the homes.
    """
    with mock.patch.dict(os.environ, {ENV_CSTAR_LOG_LEVEL: "INFO"}):
        _record_run_with_environment(
            tmp_path,
            mock_run_id,
            {
                ENV_CSTAR_DATA_HOME: str(tmp_path / "old-data"),
                ENV_CSTAR_LOG_LEVEL: "TRACE",
                ENV_CSTAR_RUNID: mock_run_id,
                "CSTAR_RECORDED_ONLY": "yes",
            },
        )

        with caplog.at_level("WARNING"):
            asyncio.run(handle_run_reloading(mock_run_id))

        warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]

    assert len(warnings) == 1
    assert f"{ENV_CSTAR_LOG_LEVEL} (now 'INFO', recorded 'TRACE')" in warnings[0]
    assert "CSTAR_RECORDED_ONLY (now None, recorded 'yes')" in warnings[0]
    assert ENV_CSTAR_DATA_HOME not in warnings[0]
    assert ENV_CSTAR_RUNID not in warnings[0]


def test_reload_skips_removed_settings(
    tmp_path: Path, mock_run_id: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a reload does not re-export a recorded setting this build no longer
    reads, nor list it among the differing settings, while it still re-exports
    an ordinary recorded setting.
    """
    removed = next(iter(REMOVED_SETTINGS))
    _record_run_with_environment(
        tmp_path,
        mock_run_id,
        {removed: FLAG_ON, ENV_CSTAR_LOG_LEVEL: "TRACE"},
    )

    with mock.patch.dict(os.environ, {ENV_CSTAR_LOG_LEVEL: "INFO"}):
        os.environ.pop(removed, None)

        with caplog.at_level("WARNING"):
            asyncio.run(handle_run_reloading(mock_run_id))

        assert removed not in os.environ
        assert os.environ[ENV_CSTAR_LOG_LEVEL] == "TRACE"

    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert ENV_CSTAR_LOG_LEVEL in warnings[0]
    assert removed not in warnings[0]


def test_reload_is_quiet_when_the_environment_matches(
    tmp_path: Path, mock_run_id: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a reload under the settings the run started with warns about nothing."""
    _record_run_with_environment(
        tmp_path,
        mock_run_id,
        {ENV_CSTAR_RUNID: mock_run_id, ENV_CSTAR_DATA_HOME: str(tmp_path / "other")},
    )

    with caplog.at_level("WARNING"):
        asyncio.run(handle_run_reloading(mock_run_id))

    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_run_summary_points_at_the_run_commands() -> None:
    """Verify the follow-up actions of a run summary use the `cstar run` commands."""
    summary = ExecutiveRunSummary(
        run_id="my-run",
        workplan_name="demo",
        source_workplan="demo.yaml",
        final_workplan="demo_transformed.yaml",
    )

    display = get_run_summary_display(summary)

    assert "cstar run status my-run" in display
    assert "cstar run log my-run <step-name>" in display
    assert "cstar workplan" not in display


def _attached_record(
    tmp_path: Path, wp_templates_dir: Path, run_id: str
) -> WorkplanRun:
    """Track a run that was attached from `tmp_path / "colleague"`."""
    root = tmp_path / "colleague" / run_id
    root.mkdir(parents=True)
    record = WorkplanRun(
        output_path=root,
        run_id=run_id,
        attached=True,
    )
    TrackingRepository().put_workplan_run_sync(record)
    return record


@pytest.mark.parametrize("flags", [[], [ARG_RESUME]])
def test_workplan_run_refuses_to_reload_an_attached_run(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
    squash_cli_output: Callable[[str], str],
    flags: list[str],
) -> None:
    """Verify `--run-id` without a path, and `--resume`, name a run attached from
    elsewhere with a usage error, before anything is scheduled.

    Parameters
    ----------
    flags : list[str]
        Extra flags: none for the plain reload, `--resume` for a resume.
    """
    record = _attached_record(tmp_path, wp_templates_dir, mock_run_id)

    with (
        mock.patch("cstar.cli.workplan.run.build_and_run_dag") as build,
        mock.patch("cstar.cli.workplan.run.run_dag") as run_dag,
    ):
        result = CliRunner().invoke(app, [ARG_RUN_ID, mock_run_id, *flags], color=False)

    assert result.exit_code == 2, result.output
    text = squash_cli_output(result.stderr)
    attached = f"Run '{mock_run_id}' was attached from {record.output_path}"
    assert squash_cli_output(attached) in text
    assert squash_cli_output("--run-id <new>") in text
    assert squash_cli_output("Extending a finished run") in text
    build.assert_not_called()
    run_dag.assert_not_called()


def test_handle_run_reloading_refuses_an_attached_run(
    tmp_path: Path, wp_templates_dir: Path, mock_run_id: str
) -> None:
    """Verify the reload helper refuses an attached record before it re-exports
    the environment the record holds.
    """
    record = _attached_record(tmp_path, wp_templates_dir, mock_run_id)
    record = record.model_copy(update={"environment": {"CSTAR_ATTACHED_TEST": "x"}})
    TrackingRepository().put_workplan_run_sync(record)

    with pytest.raises(typer.BadParameter, match="was attached from"):
        asyncio.run(handle_run_reloading(mock_run_id))

    assert "CSTAR_ATTACHED_TEST" not in os.environ
