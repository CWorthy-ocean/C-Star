import typing as t

import pytest
import typer

from cstar.cli.workplan.shared import check_and_capture_kvp, check_and_capture_kvps


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
    expected: t.Mapping[str, str],
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
