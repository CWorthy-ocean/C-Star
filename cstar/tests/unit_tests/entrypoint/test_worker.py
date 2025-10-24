import datetime
import itertools
import logging
import os
import shutil
from copy import deepcopy
from pathlib import Path
from unittest import mock

import pytest
from _pytest.tmpdir import TempPathFactory

from cstar.base.exceptions import BlueprintError, CstarError
from cstar.entrypoint.service import ServiceConfiguration
from cstar.entrypoint.worker.worker import (
    BlueprintRequest,
    JobConfig,
    SimulationRunner,
    SimulationStages,
    _format_date,
    configure_environment,
    create_parser,
    get_request,
    get_service_config,
    main,
)
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.simulation import Simulation

DEFAULT_LOOP_DELAY = 5
DEFAULT_HEALTH_CHECK_FREQUENCY = 10


@pytest.fixture(scope="module", autouse=True)
def clean_up_logs():
    """
    SimulationRunner sets up a log file during init. I could try to mock it out, but
    it's a bit hard to get at. For now, just clean things up after the module is done.
    """
    yield
    shutil.rmtree("temp_out_dir", ignore_errors=True)


@pytest.fixture
def valid_args() -> dict[str, str]:
    """Fixture to provide valid arguments for the SimulationRunner."""
    return {
        "--blueprint-uri": "blueprint.yaml",
        "--log-level": "INFO",
    }


@pytest.fixture
def valid_args_short() -> dict[str, str]:
    """Fixture to provide valid arguments for the SimulationRunner."""
    return {
        "-b": "blueprint.yaml",
        "-l": "INFO",
    }


# todo: try to rectify with Dafydd's patch_romssimulation_init_sourcedata


@pytest.fixture(scope="module")
def sim_runner_prep(
    blueprint_path: Path,
    tmp_path_factory: TempPathFactory,
) -> SimulationRunner:
    """Fixture to create a SimulationRunner instance. Module-scope to avoid slow initialization.

    Returns
    -------
    SimulationRunner
        An initialized instance of SimulationRunner, configured via blueprint.
    """
    request = BlueprintRequest(
        str(blueprint_path),
        stages=tuple(SimulationStages),
    )

    service_config = ServiceConfiguration(
        as_service=False,
        loop_delay=0,
        health_check_frequency=0,
        log_level=logging.DEBUG,
        health_check_log_threshold=10,
        name="test_simulation_runner",
    )
    job_config = JobConfig()
    sim = SimulationRunner(request, service_config, job_config)
    return sim


@pytest.fixture(scope="function")
def sim_runner(sim_runner_prep, tmp_path) -> SimulationRunner:
    """
    Takes the module-level simulation runner fixture, copies it to avoid state changes,
    and creates a new output dir and attaches it for each individual test.
    """
    sim = deepcopy(sim_runner_prep)
    output_path = tmp_path / "output"

    sim._output_root = output_path  # type: ignore[misc]
    sim._output_dir = output_path / sim._output_dir.name  # type: ignore[misc]
    sim._simulation.directory = sim._output_dir

    return sim


def test_create_parser_happy_path() -> None:
    """Verify that a help argument is present in the parser."""
    parser = create_parser()

    # ruff: noqa: SLF001
    assert "--blueprint-uri" in parser._option_string_actions
    assert "--log-level" in parser._option_string_actions


@pytest.mark.parametrize(
    ("log_level", "expected_level", "args_fixture_name"),
    [
        ("DEBUG", logging.DEBUG, "valid_args"),
        ("INFO", logging.INFO, "valid_args"),
        ("WARNING", logging.WARNING, "valid_args"),
        ("ERROR", logging.ERROR, "valid_args"),
        ("DEBUG", logging.DEBUG, "valid_args_short"),
        ("INFO", logging.INFO, "valid_args_short"),
        ("WARNING", logging.WARNING, "valid_args_short"),
        ("ERROR", logging.ERROR, "valid_args_short"),
    ],
)
def test_parser_good_log_level(
    request: pytest.FixtureRequest,
    log_level: str,
    expected_level: int,
    args_fixture_name: str,
) -> None:
    """Verify that a log level is parsed correctly."""
    valid_args: dict[str, str] = request.getfixturevalue(args_fixture_name)
    valid_args = valid_args.copy()
    if "--log-level" in valid_args:
        valid_args["--log-level"] = log_level
    else:
        valid_args["-l"] = log_level

    arg_tuples = [(k, v) for k, v in valid_args.items()]
    args = list(itertools.chain.from_iterable(arg_tuples))

    parser = create_parser()
    parsed_args = parser.parse_args(args)

    assert getattr(parsed_args, "log_level", None) == logging.getLevelName(
        expected_level
    )


@pytest.mark.parametrize(
    "log_level",
    [
        ("debug",),
        ("info",),
        ("warning",),
        ("error",),
        ("critical",),
    ],
)
def test_parser_lowercase_log_level(
    valid_args: dict[str, str],
    log_level: str,
) -> None:
    """Verify that lower-case log levels are parsed correctly."""
    valid_args = valid_args.copy()
    valid_args["--log-level"] = log_level

    arg_tuples = [(k, v) for k, v in valid_args.items()]
    args = list(itertools.chain.from_iterable(arg_tuples))

    parser = create_parser()

    # unable to parse the lower-case log levels
    with pytest.raises(SystemExit):
        parser.parse_args(args)


def test_parser_bad_log_level(valid_args: dict[str, str]) -> None:
    """Verify that a bad log level fails to parse."""
    valid_args = valid_args.copy()
    valid_args["--log-level"] = "INVALID"

    valid_args_tuples = ((k, v) for k, v in valid_args.items())
    args = list(itertools.chain.from_iterable(valid_args_tuples))

    parser = create_parser()

    with pytest.raises(SystemExit):
        _ = parser.parse_args(args)


@pytest.mark.parametrize(
    ("blueprint_uri", "log_level"),
    [
        (
            "-b blueprint1.yaml",
            "-l DEBUG",
        ),
        (
            "--blueprint-uri blueprint2.yaml",
            "--log-level INFO",
        ),
        (
            "--blueprint-uri blueprint3.yaml",
            "--log-level WARNING",
        ),
        (
            "-b blueprint1.yaml",
            "-l ERROR",
        ),
    ],
)
def test_get_service_config(
    blueprint_uri: str,
    log_level: str,
) -> None:
    """Verify that the expected values are set on the service config."""
    arg_b, val_b = blueprint_uri.split(" ", maxsplit=1)
    arg_l, val_l = log_level.split(" ", maxsplit=1)

    parser = create_parser()
    parsed_args = parser.parse_args(
        [
            arg_b,
            val_b,
            arg_l,
            val_l,
        ],
    )

    config = get_service_config(parsed_args)

    # some values are currently hardcoded for the worker service
    assert config.as_service
    assert config.loop_delay == DEFAULT_LOOP_DELAY
    assert config.health_check_frequency == DEFAULT_HEALTH_CHECK_FREQUENCY

    # log level is dynamic. verify.
    assert logging._levelToName[config.log_level] == parsed_args.log_level


@pytest.mark.parametrize(
    ("blueprint_uri",),
    [
        ("blueprint1.yaml",),
        ("blueprint2.yaml",),
        ("blueprint3.yaml",),
    ],
)
def test_get_request(
    blueprint_uri: str,
) -> None:
    """Verify that the expected values are set on the blueprint request."""
    parser = create_parser()
    parsed_args = parser.parse_args(
        [
            "--blueprint-uri",
            blueprint_uri,
            "--log-level",
            "INFO",
        ]
    )

    config = get_request(parsed_args)

    assert config.blueprint_uri == blueprint_uri


def test_configure_environment() -> None:
    """Verify the environment side-effects are as expected."""
    log = logging.getLogger()
    logging.basicConfig(level=logging.DEBUG)

    with mock.patch.dict(
        os.environ,
        {},
        clear=True,
    ):
        configure_environment(log)

        assert "CSTAR_INTERACTIVE" in os.environ
        assert os.environ["CSTAR_INTERACTIVE"] == "0"

        assert "GIT_DISCOVERY_ACROSS_FILESYSTEM" in os.environ
        assert os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] == "1"


def test_configure_environment_prebuilt() -> None:
    """Verify the environment side-effects when in a prebuilt environment.

    There shouldn't be behavioral changes compared to non-prebuilt environment.
    """
    log = logging.getLogger()
    logging.basicConfig(level=logging.DEBUG)

    with mock.patch.dict(
        os.environ,
        {
            "CSTAR_ROMS_PREBUILT": "1",
            "CSTAR_MARBL_PREBUILT": "1",
        },
        clear=True,
    ):
        configure_environment(log)

        assert "CSTAR_INTERACTIVE" in os.environ
        assert os.environ["CSTAR_INTERACTIVE"] == "0"

        assert "GIT_DISCOVERY_ACROSS_FILESYSTEM" in os.environ
        assert os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] == "1"


@pytest.mark.parametrize(
    ("date_str", "expected"),
    [
        ("2021-03-01 08:30:00", datetime.datetime(2021, 3, 1, 8, 30)),  # noqa: DTZ001
        ("2020-09-01 04:06:00", datetime.datetime(2020, 9, 1, 4, 6, 0)),  # noqa: DTZ001
        ("2019-01-01 00:00:00", datetime.datetime(2019, 1, 1, 0, 0)),  # noqa: DTZ001
    ],
)
def test_format_date_for_unique_path(
    date_str: str, expected: datetime.datetime
) -> None:
    """Verify that the date formatting for unique paths is correct.

    Parameters
    ----------
    date_str: str
        A date string to be formatted.
    expected : datetime.datetime
        The expected datetime object after formatting.
    """
    formatted_date = _format_date(date_str)
    assert formatted_date == expected


def test_start_runner(
    blueprint_path: Path,
    tmp_path: Path,
) -> None:
    """Test creating a SimulationRunner and starting it.

    Parameters
    ----------
    blueprint_path: Path
        The path to the blueprint yaml file created by the fixture.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    request = BlueprintRequest(
        str(blueprint_path),
    )

    service_config = ServiceConfiguration(
        as_service=False,
        health_check_frequency=0,
        log_level=logging.DEBUG,
        name="test_start_runner",
        loop_delay=0,
        health_check_log_threshold=10,
    )
    job_config = JobConfig()

    runner = SimulationRunner(request, service_config, job_config)

    assert runner._blueprint_uri == request.blueprint_uri


def test_runner_directory_check(
    tmp_path: Path,
    sim_runner: SimulationRunner,
    dotenv_path: Path,
) -> None:
    """Test the simulation runner's file system preparation.

    Verifies that a non-empty output directory causes an exception
    to be raised.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    dotenv_path : Path
        Path to a temporary location to
    """
    sim_runner._output_dir.mkdir(parents=True, exist_ok=True)
    (sim_runner._output_dir / "somefile.txt").touch()

    with (
        pytest.raises(ValueError),
    ):
        sim_runner._prepare_file_system()


def test_runner_directory_check_ignore_logs(
    tmp_path: Path,
    sim_runner: SimulationRunner,
    dotenv_path: Path,
) -> None:
    """Test the simulation runner's file system preparation.

    Verify that the worker ignores a populated output directory if it only
    contains log files.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    dotenv_path : Path
        Path to a temporary location to
    """
    output_dir = tmp_path / "output"

    # populate the directories that should be cleaned-up
    dotenv_path.parent.mkdir(parents=True, exist_ok=True)
    dotenv_path.touch()

    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=False)

    # A file in the logs directory should be ignored
    (logs_dir / "any-name.txt").touch()

    with mock.patch(
        "cstar.system.environment.CStarEnvironment.user_env_path",
        new_callable=mock.PropertyMock,
        return_value=dotenv_path,
    ):
        sim_runner._prepare_file_system()


def test_runner_directory_prep(
    tmp_path: Path,
    sim_runner: SimulationRunner,
    dotenv_path: Path,
) -> None:
    """Test the simulation runner's file system preparation.

    Verifies that the output directories are created and empty.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    dotenv_path : Path
        Path to a temporary location to
    """
    output_dir = tmp_path / "output"

    # populate the directories that should be cleaned-up
    dotenv_path.parent.mkdir(parents=True, exist_ok=True)
    dotenv_path.touch()

    # an empty output dir should be ok
    output_dir.mkdir(parents=True, exist_ok=True)

    sim_runner._prepare_file_system()

    # Confirm a user env file is not removed
    assert dotenv_path.exists()

    # Confirm the output directory is created...
    assert sim_runner._output_dir.exists()
    assert sim_runner._output_dir.is_dir()
    assert sim_runner._output_dir.parent == sim_runner._output_root

    # ...and is empty so no conflicts will occur.
    output_content = list(sim_runner._output_dir.iterdir())
    assert not output_content, "Output directory should be empty after prep."


@pytest.mark.asyncio
async def test_runner_can_shutdown_as_task(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the shutdown override of the base Service class.

    Verifies that a SimulationRunner configured as a task will
    automatically exit after a single event loop.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    # Configure the SimulationRunner to run as a task
    sim_runner._config.as_service = False

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_on_start", mock.Mock),
        mock.patch.object(sim_runner, "_on_iteration", mock.Mock),
        mock.patch.object(sim_runner, "_on_shutdown", mock.Mock),
        mock.patch.object(sim_runner, "_can_shutdown", mock.Mock(return_value=True)),
    ):
        # And confirm it already says it can exit
        assert sim_runner.can_shutdown

        # but, the (mocked) internal method says it should keep running
        assert sim_runner._can_shutdown()


@pytest.mark.asyncio
async def test_runner_can_shutdown_as_task_null_sim(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the shutdown override of the base Service class.

    Verifies that a SimulationRunner that fails to load
    a simulation blueprint properly will automatically exit.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    # Configure the SimulationRunner to run as a task
    sim_runner._config.as_service = False

    with mock.patch.object(sim_runner, "_simulation", None):
        # and confirm it exits immediately when the simulation is None
        assert sim_runner._can_shutdown()
        assert sim_runner._is_status_complete()


@pytest.mark.asyncio
async def test_runner_can_shutdown_as_service_null_sim(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the shutdown override of the base Service class.

    Verifies that a SimulationRunner that fails to load
    a simulation blueprint properly will automatically exit,
    even if configured to run as a service.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    # Configure the SimulationRunner to run as a task
    sim_runner._config.as_service = True

    with mock.patch.object(sim_runner, "_simulation", None):
        # and confirm it exits immediately when the simulation is None
        assert sim_runner._can_shutdown()
        assert sim_runner._is_status_complete()


@pytest.mark.asyncio
async def test_runner_shutdown_no_update_handler(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the shutdown criteria of the SimulationRunner.

    This test verifies that the SimulationRunner will
    shut down if no update handler is set.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    # Configure the SimulationRunner to run as a service
    sim_runner._config.as_service = True

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_on_start", mock.Mock),
        mock.patch.object(sim_runner, "_on_iteration", mock.Mock),
        mock.patch.object(sim_runner, "_on_shutdown", mock.Mock),
    ):
        # And confirm it already says it can exit
        assert sim_runner.can_shutdown

        # and the (mocked) internal state says it should keep running
        assert sim_runner._can_shutdown()
        assert sim_runner._handler is None


@pytest.mark.parametrize(
    "status",
    [
        ExecutionStatus.COMPLETED,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.FAILED,
        ExecutionStatus.UNKNOWN,
    ],
)
@pytest.mark.asyncio
async def test_runner_shutdown_handler_complete(
    sim_runner: SimulationRunner,
    tmp_path: Path,
    status: ExecutionStatus,
) -> None:
    """Test the shutdown criteria of the SimulationRunner.

    This test verifies that the SimulationRunner will
    shut down if the update handler reports that it is completed.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    status : ExecutionStatus
        The execution status to test the shutdown criteria with.
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    # Configure the SimulationRunner to run as a service
    sim_runner._config.as_service = True

    mock_status_attr = mock.PropertyMock(return_value=status)
    assert mock_status_attr.call_count == 0

    mock_handler = mock.Mock(spec=ExecutionHandler)
    type(mock_handler).status = mock_status_attr

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_on_start", mock.Mock),
        mock.patch.object(sim_runner, "_on_iteration", mock.Mock),
        mock.patch.object(sim_runner, "_on_shutdown", mock.Mock),
        mock.patch.object(sim_runner, "_handler", mock_handler),
    ):
        # and confirm it already says it can exit
        assert sim_runner.can_shutdown

        # ... and confirm the can_shutdown didn't short-circuit before looking
        # the handler.status property by checking the call count
        assert mock_status_attr.call_count > 0


@pytest.mark.parametrize(
    "status",
    [
        ExecutionStatus.RUNNING,
        ExecutionStatus.PENDING,
        ExecutionStatus.UNSUBMITTED,
    ],
)
@pytest.mark.asyncio
async def test_runner_shutdown_handler_not_complete(
    sim_runner: SimulationRunner,
    tmp_path: Path,
    status: ExecutionStatus,
) -> None:
    """Test the shutdown criteria of the SimulationRunner.

    This test verifies that the SimulationRunner will not
    shut down if the update handler reports that it is not done.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    status : ExecutionStatus
        The execution status to test the shutdown criteria with.
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    # Configure the SimulationRunner to run as a service
    sim_runner._config.as_service = True

    mock_status_attr = mock.PropertyMock(return_value=status)
    assert mock_status_attr.call_count == 0

    mock_handler = mock.Mock(spec=ExecutionHandler)
    type(mock_handler).status = mock_status_attr

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_on_start", mock.Mock),
        mock.patch.object(sim_runner, "_on_iteration", mock.Mock),
        mock.patch.object(sim_runner, "_on_shutdown", mock.Mock),
        mock.patch.object(sim_runner, "_handler", mock_handler),
    ):
        # and confirm it already says it can exit
        assert not sim_runner.can_shutdown

        # ... and confirm the can_shutdown didn't short-circuit before looking
        # the handler.status property by checking the call count
        assert mock_status_attr.call_count > 0


@pytest.mark.parametrize(
    "status",
    [
        ExecutionStatus.FAILED,
        ExecutionStatus.UNKNOWN,
        ExecutionStatus.CANCELLED,
    ],
)
@pytest.mark.asyncio
async def test_runner_shutdown_side_effects(
    sim_runner: SimulationRunner,
    tmp_path: Path,
    status: ExecutionStatus,
) -> None:
    """Test the shutdown behavior of the SimulationRunner.

    This test verifies that the SimulationRunner executes the desired
    behaviors during shutdown.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    status : ExecutionStatus
        The execution status to test the shutdown criteria with.
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    mock_handler = mock.Mock(spec=ExecutionHandler, status=status)
    mock_disposition = mock.Mock()
    mock_simulation = mock.Mock()

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_on_start", mock.Mock),
        mock.patch.object(sim_runner, "_on_iteration", mock.Mock),
        mock.patch.object(sim_runner, "_handler", mock_handler),
        mock.patch.object(sim_runner, "_log_disposition", mock_disposition),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # Trigger a run through the lifecycle as a task. This should triger
        # the complete set of shutdown behaviors.
        await sim_runner.execute()

        # Now confirm that my target behaviors were executed
        assert mock_disposition.call_count == 1


@pytest.mark.asyncio
async def test_runner_on_start_without_uri(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the error handling behavior of the SimulationRunner.

    This test verifies that the simulation runner will raise an error
    if a URI for a blueprint is not provided.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    mock_handler = mock.Mock(spec=ExecutionHandler, status=ExecutionStatus.COMPLETED)
    mock_iter = mock.Mock()
    mock_simulation = mock.Mock()
    mock_prep_fs = mock.Mock()
    mock_shutdown = mock.Mock()

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_prepare_file_system", mock_prep_fs),
        mock.patch.object(sim_runner, "_on_iteration", mock_iter),
        mock.patch.object(sim_runner, "_on_shutdown", mock_shutdown),
        mock.patch.object(sim_runner, "_handler", mock_handler),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # clear blueprint URI from the default SimulationRunner from the fixture
        # use `setattr` to force-change the Final
        setattr(sim_runner, "_blueprint_uri", None)  # noqa: B010

        # Trigger a run through the lifecycle as a task. Without a blueprint URI,
        # this should fail but it should still shutdown gracefully.
        await sim_runner.execute()

        # Now confirm that my target start-up behaviors were executed
        assert mock_prep_fs.call_count == 0
        assert mock_iter.call_count == 0
        assert mock_shutdown.call_count == 1


@pytest.mark.asyncio
async def test_runner_on_start_without_simulation(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the error handling behavior of the SimulationRunner.

    This test verifies that the simulation runner will raise an error
    if the Blueprint URI isn't provided, but it will fail gracefully and
    call shutdown to release resources.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    mock_handler = mock.Mock(spec=ExecutionHandler, status=ExecutionStatus.COMPLETED)
    mock_iter = mock.Mock()
    mock_simulation = mock.Mock()
    mock_prep_fs = mock.Mock()
    mock_shutdown = mock.Mock()

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_prepare_file_system", mock_prep_fs),
        mock.patch.object(sim_runner, "_on_iteration", mock_iter),
        mock.patch.object(sim_runner, "_on_shutdown", mock_shutdown),
        mock.patch.object(sim_runner, "_handler", mock_handler),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # simulate a failure to load the simulation blueprint
        # use `setattr` to force-change the Final
        setattr(sim_runner, "_simulation", None)  # noqa: B010

        # Trigger a run through the lifecycle as a task. Without a blueprint URI,
        # this should fail but it should still shutdown gracefully.
        await sim_runner.execute()

        # Now confirm that my target start-up behaviors were executed
        assert mock_prep_fs.call_count == 0
        assert mock_iter.call_count == 0
        assert mock_shutdown.call_count == 1


@pytest.mark.asyncio
async def test_runner_on_start_user_unhandled_setup(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the error handling behavior of the SimulationRunner.

    This test verifies that the simulation runner will raise an error
    if the simulation setup fails, but it will fail gracefully and
    call shutdown to release resources.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    mock_handler = mock.Mock(spec=ExecutionHandler, status=ExecutionStatus.COMPLETED)
    mock_iter = mock.Mock()

    # configure the simulation to raise an exception during SETUP
    mock_simulation = mock.Mock(
        setup=mock.Mock(side_effect=ValueError("Mock setup Failure"))
    )
    mock_prep_fs = mock.Mock()
    mock_shutdown = mock.Mock()

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_prepare_file_system", mock_prep_fs),
        mock.patch.object(sim_runner, "_on_iteration", mock_iter),
        mock.patch.object(sim_runner, "_on_shutdown", mock_shutdown),
        mock.patch.object(sim_runner, "_handler", mock_handler),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # # simulate a failure to load the simulation blueprint
        # sim_runner._simulation = None

        # Trigger a run through the lifecycle as a task. Without a blueprint URI,
        # this should fail but it should still shutdown gracefully.
        await sim_runner.execute()

        # Now confirm that my target start-up behaviors were executed
        assert mock_prep_fs.call_count == 1
        assert mock_simulation.setup.call_count == 1
        assert mock_iter.call_count == 0
        assert mock_shutdown.call_count == 1


@pytest.mark.asyncio
async def test_runner_on_start_user_unhandled_build(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the error handling behavior of the SimulationRunner.

    This test verifies that the simulation runner will raise an error
    if the simulation setup fails, but it will fail gracefully and
    call shutdown to release resources.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    mock_handler = mock.Mock(spec=ExecutionHandler, status=ExecutionStatus.COMPLETED)
    mock_iter = mock.Mock()

    # configure the simulation to raise an exception during BUILD
    mock_simulation = mock.Mock(
        build=mock.Mock(side_effect=RuntimeError("Mock build Failure"))
    )
    mock_prep_fs = mock.Mock()

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_prepare_file_system", mock_prep_fs),
        mock.patch.object(sim_runner, "_on_iteration", mock_iter),
        mock.patch.object(sim_runner, "_handler", mock_handler),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # # simulate a failure to load the simulation blueprint
        # sim_runner._simulation = None

        # Trigger a run through the lifecycle as a task. Without a blueprint URI,
        # this should fail but it should still shutdown gracefully.
        await sim_runner.execute()

        # Now confirm that my target start-up behaviors were executed
        assert mock_prep_fs.call_count == 1
        assert mock_simulation.setup.call_count == 1
        assert mock_iter.call_count == 0
        # assert mock_shutdown.call_count == 1


@pytest.mark.asyncio
async def test_runner_on_start_user_unhandled_pre_run(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the error handling behavior of the SimulationRunner.

    This test verifies that the simulation runner will raise an error
    if the simulation setup fails, but it will fail gracefully and
    call shutdown to release resources.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    mock_handler = mock.Mock(spec=ExecutionHandler, status=ExecutionStatus.COMPLETED)
    mock_iter = mock.Mock()

    # configure the simulation to raise an exception during PRE-RUN
    mock_simulation = mock.Mock(
        pre_run=mock.Mock(side_effect=Exception("Mock pre-run Failure"))
    )
    mock_prep_fs = mock.Mock()

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_prepare_file_system", mock_prep_fs),
        mock.patch.object(sim_runner, "_on_iteration", mock_iter),
        mock.patch.object(sim_runner, "_handler", mock_handler),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # # simulate a failure to load the simulation blueprint
        # sim_runner._simulation = None

        # Trigger a run through the lifecycle as a task. Without a blueprint URI,
        # this should fail but it should still shutdown gracefully.
        await sim_runner.execute()

        # Now confirm that my target start-up behaviors were executed
        assert mock_prep_fs.call_count == 1
        assert mock_simulation.setup.call_count == 1
        assert mock_iter.call_count == 0
        # assert mock_shutdown.call_count == 1


@pytest.mark.asyncio
async def test_runner_on_iteration(
    sim_runner: SimulationRunner,
    tmp_path: Path,
) -> None:
    """Test the main iteration behavior of the SimulationRunner.

    This test verifies that the SimulationRunner triggers
    execution of the simulation.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    output_dir = tmp_path / "output"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "somefile.txt").touch()

    mock_simulation = mock.Mock()
    mock_prep_fs = mock.Mock()
    mock_shutdown = mock.Mock()

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_start_healthcheck", mock.Mock()),
        mock.patch.object(sim_runner, "_prepare_file_system", mock_prep_fs),
        mock.patch.object(sim_runner, "_on_shutdown", mock_shutdown),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # Trigger a run through the lifecycle as a task.
        await sim_runner.execute()

        # Now confirm that my target lifecycle behaviors were all executed
        assert mock_prep_fs.call_count == 1
        assert mock_simulation.setup.call_count == 1
        assert mock_simulation.build.call_count == 1
        assert mock_simulation.pre_run.call_count == 1
        assert mock_simulation.run.call_count == 1
        assert mock_shutdown.call_count == 1


@pytest.mark.xfail(
    reason="some of these fail now that we raise an error on unknown return status"
)
@pytest.mark.parametrize(
    ("setup", "build", "pre_run", "run", "post_run"),
    [
        (0, 0, 0, 0, 0),
        (1, 0, 0, 0, 0),
        (0, 1, 0, 0, 0),
        (0, 0, 1, 0, 0),
        (0, 0, 0, 1, 0),
        (0, 0, 0, 1, 1),
        (1, 1, 0, 0, 0),
        (0, 0, 1, 1, 0),
        (1, 0, 1, 0, 0),
        (1, 0, 0, 1, 0),
        (0, 1, 1, 0, 0),
        (0, 1, 0, 1, 0),
        (0, 1, 0, 1, 1),
        (0, 1, 1, 1, 0),
        (0, 1, 1, 1, 1),
        (1, 0, 0, 1, 1),
        (1, 1, 0, 1, 1),
        (1, 0, 1, 1, 1),
        (1, 1, 1, 1, 1),
    ],
)
@pytest.mark.asyncio
async def test_runner_setup_stage(
    sim_runner: SimulationRunner,
    blueprint_path: Path,
    setup: bool,
    build: bool,
    pre_run: bool,
    run: bool,
    post_run: bool,
) -> None:
    """Test conditional stage execution.

    Verifies that each conditionally stage is executed when configured to do so.

    WARNING: executing post-run without run is currently not tested/supported due to
    the way the simulation creates and relies on a stateful execution handler.

    Parameters
    ----------
    sim_runner: SimulationRunner
        An instance of SimulationRunner to be used for the test.
    tmp_path : Path
        A temporary path to store simulation output and logs
    """
    mock_prep_fs = mock.Mock()

    stages = []
    if setup:
        stages.append(SimulationStages.SETUP)
    if build:
        stages.append(SimulationStages.BUILD)
    if pre_run:
        stages.append(SimulationStages.PRE_RUN)
    if run:
        stages.append(SimulationStages.RUN)
    if post_run:
        stages.append(SimulationStages.POST_RUN)

    request = BlueprintRequest(
        str(blueprint_path),
        stages=tuple(stages),
    )

    setattr(sim_runner, "_stages", tuple(request.stages))
    mock_simulation = mock.Mock(spec=Simulation)

    def _mock_run(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202, ARG001
        return mock.Mock(spec=ExecutionHandler, status=ExecutionStatus.COMPLETED)

    mock_simulation.run.configure_mock(side_effect=_mock_run)

    # don't let it perform any real work
    with (
        mock.patch.object(sim_runner, "_start_healthcheck", mock.Mock()),
        mock.patch.object(sim_runner, "_prepare_file_system", mock_prep_fs),
        mock.patch.object(sim_runner, "_simulation", mock_simulation),
        mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}),
    ):
        # Trigger a run through the lifecycle as a task.
        await sim_runner.execute()

        # Now confirm that my target lifecycle behaviors were conditionally executed
        assert mock_prep_fs.call_count == 1
        assert mock_simulation.setup.call_count == (1 if setup else 0)
        assert mock_simulation.build.call_count == (1 if build else 0)
        assert mock_simulation.pre_run.call_count == (1 if pre_run else 0)
        assert mock_simulation.run.call_count == (1 if run else 0)
        assert mock_simulation.post_run.call_count == (1 if post_run else 0)


@pytest.mark.asyncio
async def test_worker_main(tmp_path: Path) -> None:
    """Test the main entrypoint of the worker service.

    This test verifies that the the main function will fail to run when called without
    explicit arguments.
    """
    mock_execute = mock.Mock()

    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    output_path = tmp_path / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    args = [
        "--blueprint-uri",
        str(bp_path),
        "--output-dir",
        str(output_path),
        "--log-level",
        "DEBUG",
        "--start-date",
        "2024-01-01 00:00:00",
        "--end-date",
        "2024-02-01 00:00:00",
    ]

    with mock.patch.dict(os.environ, {}):
        # don't let it perform any real work
        return_code = await main(args)

    # Confirm an error code is returned
    assert return_code > 0

    # Confirm the runner didn't try to run a simulation without arguments
    assert mock_execute.call_count == 0


@pytest.mark.asyncio
async def test_worker_main_exec(
    blueprint_path: Path,
    tmp_path: Path,
) -> None:
    """Test the main entrypoint of the worker service.

    This test verifies that the main function will run the simulation when called
    with properly formatted arguments.
    """
    mock_execute = mock.AsyncMock(return_code=0)

    args = [
        "--blueprint-uri",
        str(blueprint_path),
        "--log-level",
        "DEBUG",
    ]

    # don't let it perform any real work; mock out runner.execute
    with (
        mock.patch(
            "cstar.entrypoint.worker.SimulationRunner.execute",
            mock_execute,
        ),
        mock.patch.dict(
            os.environ,
            {
                "CSTAR_INTERACTIVE": "0",
                "GIT_DISCOVERY_ACROSS_FILESYSTEM": "1",
            },
            clear=True,
        ),
    ):
        # This should run the simulation and return a success code
        return_code = await main(args)

    # Confirm an error code is returned
    assert return_code == 0

    # Confirm the runner ran the simulation
    assert mock_execute.call_count == 1


@pytest.mark.parametrize("exception_type", [CstarError, BlueprintError, Exception])
@pytest.mark.asyncio
async def test_worker_main_cstar_error(
    blueprint_path: Path,
    tmp_path: Path,
    exception_type: type[Exception],
) -> None:
    """Test the main entrypoint of the worker service.

    This test verifies that the main function catches CStarXxxErrors and shuts down
    gracefully.
    """
    args = [
        "--blueprint-uri",
        str(blueprint_path),
        "--log-level",
        "DEBUG",
    ]

    def return_mocked_sim_runner(*args, **kwargs):
        raise exception_type("Mock error")

    # don't let it perform any real work; mock out runner.execute
    with (
        mock.patch(
            "cstar.entrypoint.worker.SimulationRunner.__new__",
            return_mocked_sim_runner,
        ),
        mock.patch.dict(
            os.environ,
            {
                "CSTAR_INTERACTIVE": "0",
                "GIT_DISCOVERY_ACROSS_FILESYSTEM": "1",
            },
            clear=True,
        ),
    ):
        # This should run the simulation and return a failure code.
        return_code = await main(args)

    assert return_code > 0
