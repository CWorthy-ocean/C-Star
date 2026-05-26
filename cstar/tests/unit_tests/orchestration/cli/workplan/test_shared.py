from collections.abc import Mapping
from pathlib import Path
from unittest import mock

import pytest
import typer

from cstar.cli.workplan.shared import (
    autocomplete_step_list,
    check_and_capture_kvp,
    check_and_capture_kvps,
    list_steps,
)
from cstar.orchestration.models import Workplan


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        pytest.param("key=value", ("key", "value"), id="ideal"),
        pytest.param("k=v", ("k", "v"), id="key and value::single character"),
        pytest.param(" k = v ", ("k", "v"), id="key and value::surrounding whitespace"),
        pytest.param(" k=v", ("k", "v"), id="key::leading whitespace"),
        pytest.param("k =v", ("k", "v"), id="key::trailing whitespace"),
        pytest.param(" k =v", ("k", "v"), id="key::surrounding whitespace"),
        pytest.param(" k k =v", ("k k", "v"), id="key::internal whitespace"),
        pytest.param("k= v", ("k", "v"), id="value::leading whitespace"),
        pytest.param("k=v ", ("k", "v"), id="value::trailing whitespace"),
        pytest.param("k= v ", ("k", "v"), id="value::surrounding whitespace"),
        pytest.param("k= v v ", ("k", "v v"), id="value::internal whitespace"),
        pytest.param("k\n\t=v", ("k", "v"), id="key::escape sequences"),
    ],
)
def test_capture_kvp_happy_path(entry: str, expected: tuple[str, str]) -> None:
    """Verify an ideal input for `check_and_capture_kvp` is handled properly.

    Parameters
    ----------
    entry : str
        The entry value to attempt to parse
    expected : tuple[str, str]
        The expected output
    """
    actual = check_and_capture_kvp(entry)

    assert actual == expected


@pytest.mark.parametrize(
    ("entry", "error"),
    [
        pytest.param("=value", "without key", id="missing::key"),
        pytest.param(" =value", "without key", id="whitespace::key"),
        pytest.param("key=", "empty value", id="missing::value"),
        pytest.param("key= ", "empty value", id="whitespace::value"),
        pytest.param("=", "key and value", id="missing::key and value"),
        pytest.param(" = ", "key and value", id="whitespace::key and value"),
        pytest.param("", "expected format", id="format::empty"),
        pytest.param(" ", "expected format", id="format::whitespace"),
        pytest.param("xxx", "expected format", id="format::no kvp delimiter"),
    ],
)
def test_capture_kvp_invalid_inputs(entry: str, error: str) -> None:
    """Verify incorrectly formatted input for `check_and_capture_kvp` result
    in the expected exceptions.

    Parameters
    ----------
    entry : str
        The entry value to attempt to parse
    error : str
        The error string that should be matched if the proper error is raised
    """
    with pytest.raises(typer.BadParameter, match=error):
        _ = check_and_capture_kvp(entry)


@pytest.mark.parametrize(
    ("entries", "expected"),
    [
        pytest.param(
            ["var1=value1", "var2=value2"],
            {"var1": "value1", "var2": "value2"},
            id="ideal",
        ),
        pytest.param(
            [],
            {},
            id="empty list",
        ),
        pytest.param(
            None,
            {},
            id="null",
        ),
    ],
)
def test_capture_kvps_happy_path(
    entries: list[str],
    expected: Mapping[str, str],
) -> None:
    """Verify that valid inputs produce the expected output.

    Parameters
    ----------
    entries : list[str]
        The list of items to parse
    expected : t.Mapping[str, str]
        The expected output
    """
    actual = check_and_capture_kvps(entries)
    assert expected == actual


@pytest.mark.parametrize(
    "entries",
    [
        pytest.param(
            ["x=X", "x=X"],
            id="sequential::value match",
        ),
        pytest.param(
            ["x=A", "x=B"],
            id="sequential::value mismatch",
        ),
        pytest.param(
            ["x=X", "y=Y", "x=X"],
            id="non-sequential::value match",
        ),
        pytest.param(
            ["x=A", "y=Y", "x=B"],
            id="non-sequential::value mismatch",
        ),
    ],
)
def test_capture_kvps_duplicate(
    entries: list[str],
) -> None:
    """Verify that receiving the same key more than once results in an exception.

    NOTE: the current implementation does not ignore duplicate keys, even
    if the values are matching.

    Parameters
    ----------
    entries : list[str]
        The list of items to parse
    expected : t.Mapping[str, str]
        The expected output
    """
    with pytest.raises(typer.BadParameter, match="multiple"):
        _ = check_and_capture_kvps(entries)


@pytest.mark.asyncio
@pytest.mark.usefixtures("mock_state_dir")
async def test_list_steps_empty_run_id() -> None:
    """Verify that listing steps with no filter returns all available steps
    in the results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    steps = await list_steps(run_id="", incomplete="")

    # confirm that an empty list is returned
    assert not steps


@pytest.mark.asyncio
@pytest.mark.usefixtures("mock_state_dir")
async def test_list_steps_unfiltered(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that listing steps with no filter returns all available steps
    in the results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan
    num_steps = len(wp.steps)

    # confirm that a step is discovered when not using a filter
    steps = await list_steps(fake_run_id, incomplete="")
    assert len(steps) == num_steps
    assert set(steps) == {s.name for s in wp.steps}


@pytest.mark.asyncio
@pytest.mark.usefixtures("mock_state_dir")
async def test_list_steps_filtered(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that listing steps with a filter returns a subset of available results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    for step in wp.steps:
        # confirm that using a step name as the filter returns that single result
        steps = await list_steps(fake_run_id, step.name)
        assert len(steps) == 1
        assert step.name in steps

        # confirm case-insensitivity
        steps = await list_steps(fake_run_id, step.name.lower())
        assert len(steps) == 1
        assert step.name in steps


@pytest.mark.asyncio
@pytest.mark.usefixtures("mock_state_dir")
async def test_list_steps_filter_all(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that listing steps with an filter that has no matches results in
    no results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, _, fake_run_id = executed_workplan

    # confirm a nonsense name is not found
    steps = await list_steps(fake_run_id, incomplete="xyzpqr")
    assert not steps


@pytest.mark.asyncio
@pytest.mark.usefixtures("mock_state_dir")
async def test_autocomplete_step_list_no_run_id() -> None:
    """Verify that a missing run-id parameter raises a typer exception."""
    mock_typer_ctx = mock.Mock(params={"no-run-id-param-to-locate": 0})

    with (
        mock.patch("typer.Context", mock_typer_ctx),
        pytest.raises(typer.BadParameter, match="run-id is required"),
    ):
        autocomplete_step_list(mock_typer_ctx, incomplete="")


@pytest.mark.asyncio
@pytest.mark.usefixtures("mock_state_dir")
async def test_autocomplete_step_list_empty_run_id() -> None:
    """Verify that a missing run-id parameter raises a typer exception."""
    mock_typer_ctx = mock.Mock(params={"run-id": ""})

    with (
        mock.patch("typer.Context", mock_typer_ctx),
        pytest.raises(typer.BadParameter, match="run-id is required"),
    ):
        autocomplete_step_list(mock_typer_ctx, incomplete="")


@pytest.mark.usefixtures("mock_state_dir")
def test_autocomplete_step_list_happy_path(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that a valid run-id locates a run.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    mock_typer_ctx = mock.Mock(params={"run_id": fake_run_id})

    with mock.patch("typer.Context", mock_typer_ctx):
        steps = autocomplete_step_list(mock_typer_ctx, incomplete="")
        assert len(steps) == len(wp.steps)
