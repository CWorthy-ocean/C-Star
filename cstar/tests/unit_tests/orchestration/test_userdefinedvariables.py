import typing as t

import pytest

from cstar.orchestration.models import UserDefinedVariables


@pytest.mark.parametrize(
    ("keys", "mapping"),
    [
        pytest.param(set(), {}, id="empty keys and mapping"),
        pytest.param({"a"}, {"a": "A"}, id="single key"),
        pytest.param({"a", "b"}, {"a": "A", "b": "B"}, id="multi key"),
    ],
)
def test_namedruntimeconfiguration_valid_inputs(
    keys: set[str],
    mapping: t.Mapping[str, str],
) -> None:
    """Verify that a valid input does not result in an error.

    Parameters
    ----------
    keys : set[str]
        A valid set of "declared keys"
    mapping : t.Mapping[str, str]
        A valid mapping of key-value pairs
    """
    replacements = UserDefinedVariables(
        keys=keys,
        mapping=mapping,
        require_coverage=True,
        require_declaration=True,
    )

    assert not replacements.error


@pytest.mark.parametrize(
    ("keys", "mapping", "matches"),
    [
        pytest.param(
            {"x"},
            {"a": "AAA"},
            {"x"},
            id="no coverage::single declared",
        ),
        pytest.param(
            {"x", "y"},
            {"a": "AAA"},
            {"x", "y"},
            id="no coverage::multiple declared",
        ),
        pytest.param(
            {"x", "y"},
            {"x": "AAA"},
            {"y"},
            id="partial coverage::single uncovered",
        ),
        pytest.param(
            {"x", "y", "z"},
            {"y": "AAA"},
            {"x", "z"},
            id="partial coverage::multiple uncovered",
        ),
    ],
)
def test_namedruntimeconfiguration_coverage_fail(
    keys: set[str],
    mapping: t.Mapping[str, str],
    matches: set[str],
) -> None:
    """Verify that mappings that do not cover all declared keys result in an error
    if `require_coverage` is `True` and `require_declaration` is `False`

    Parameters
    ----------
    keys : set[str]
        A valid set of "declared keys"
    mapping : t.Mapping[str, str]
        A valid mapping of key-value pairs
    matches : list[str]
        Strings that must be found in the error output
    """
    replacements = UserDefinedVariables(
        keys=keys,
        mapping=mapping,
        require_coverage=True,
        require_declaration=False,
    )

    # confirm all the expected match terms are in the error (e.g. all unconfigured keys)
    items_reported = replacements.error.split(":", maxsplit=1)[1]
    not_matched = matches.difference(x.strip() for x in items_reported.split(","))
    assert "missing" in replacements.error
    assert not not_matched

    # confirm that if require_coverage is False, the same input does not emit the error
    forgiving_replacements = UserDefinedVariables(
        keys=keys,
        mapping=mapping,
        require_coverage=False,
        require_declaration=False,
    )
    assert not forgiving_replacements.error


def test_namedruntimeconfiguration_coverage_impossible() -> None:
    """Verify that an empty declared variables set does not result
    in a coverage error if `require_declaration` is `False`.

    Parameters
    ----------
    keys : set[str]
        A valid set of "declared keys"
    mapping : t.Mapping[str, str]
        A valid mapping of key-value pairs
    matches : list[str]
        Strings that must be found in the error output
    """
    keys: set[str] = set()
    mapping = {"a": "AA"}

    replacements = UserDefinedVariables(
        keys=keys,
        mapping=mapping,
        require_coverage=True,
        require_declaration=False,
    )

    assert not replacements.error


def test_namedruntimeconfiguration_no_declarations() -> None:
    """Verify that an empty declared variables set results
    in a declaration error if `require_declaration` is `True`.

    Parameters
    ----------
    keys : set[str]
        A valid set of "declared keys"
    mapping : t.Mapping[str, str]
        A valid mapping of key-value pairs
    matches : list[str]
        Strings that must be found in the error output
    """
    keys: set[str] = set()
    mapping = {"a": "AA"}

    replacements = UserDefinedVariables(
        keys=keys,
        mapping=mapping,
        require_coverage=False,
        require_declaration=True,
    )

    assert "unknown" in replacements.error


@pytest.mark.parametrize(
    ("keys", "mapping", "matches"),
    [
        pytest.param(
            {"x"},
            {"a": "AAA"},
            {"a"},
            id="no declarations satisfied::single",
        ),
        pytest.param(
            {"x", "y"},
            {"a": "AAA", "b": "BBB"},
            {"a", "b"},
            id="no declarations satisfied::multi",
        ),
        pytest.param(
            {"x", "y"},
            {"a": "AAA", "x": "XX"},
            {"a"},
            id="partial declarations satisfied::single",
        ),
        pytest.param(
            {"x", "y", "z"},
            {"a": "AAA", "x": "XX", "b": "BBB"},
            {"a", "b"},
            id="partial declarations satisfied::multi",
        ),
    ],
)
def test_namedruntimeconfiguration_declaration_fail(
    keys: set[str],
    mapping: t.Mapping[str, str],
    matches: set[str],
) -> None:
    """Verify that mappings including undeclared keys result in an error
    if `require_declaration` is `True`

    Parameters
    ----------
    keys : set[str]
        A valid set of "declared keys"
    mapping : t.Mapping[str, str]
        A valid mapping of key-value pairs
    matches : list[str]
        Strings that must be found in the error output
    """
    replacements = UserDefinedVariables(
        keys=keys,
        mapping=mapping,
        require_coverage=False,
        require_declaration=True,
    )

    # confirm all the expected match terms are in the error (e.g. all undeclared keys)
    items_reported = replacements.error.split(":")[1]
    not_matched = matches.difference(x.strip() for x in items_reported.split(","))
    assert not not_matched

    # confirm that if require_declaration is False, the same input does not emit the error
    forgiving_replacements = UserDefinedVariables(
        keys=keys,
        mapping=mapping,
        require_coverage=False,
        require_declaration=False,
    )
    assert not forgiving_replacements.error
