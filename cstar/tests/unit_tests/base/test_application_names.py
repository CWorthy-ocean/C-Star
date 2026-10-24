import pytest

from cstar.applications.core import get_application
from cstar.base.application_names import (
    APPLICATION_ALIASES,
    canonical_application,
    legacy_names,
)


def test_canonical_application_maps_alias_to_current_name() -> None:
    """An earlier name resolves to the name that replaced it."""
    assert canonical_application("roms_marbl") == "ucla_roms"


@pytest.mark.parametrize(
    "name", ["ucla_roms", "forge", "hello_world", "", "unknown_app"]
)
def test_canonical_application_leaves_other_names_unchanged(name: str) -> None:
    """A current, unknown or empty name passes through."""
    assert canonical_application(name) == name


@pytest.mark.parametrize(
    "name", ["ROMS_MARBL", "Roms_Marbl", " roms_marbl", "roms_marbl "]
)
def test_canonical_application_is_an_exact_match(name: str) -> None:
    """Application names are module names: no case folding or stripping."""
    assert canonical_application(name) == name


def test_legacy_names_lists_aliases_of_the_current_name() -> None:
    """The aliases of a renamed application are returned."""
    assert legacy_names("ucla_roms") == ("roms_marbl",)


@pytest.mark.parametrize("name", ["roms_marbl", "forge", "unknown_app"])
def test_legacy_names_is_empty_without_aliases(name: str) -> None:
    """A name nothing maps to, an alias included, has no earlier names."""
    assert legacy_names(name) == ()


def test_alias_table_is_read_only() -> None:
    """The table cannot be edited by a consumer."""
    with pytest.raises(TypeError):
        APPLICATION_ALIASES["extra"] = "ucla_roms"  # type: ignore[index]


@pytest.mark.parametrize(("alias", "target"), list(APPLICATION_ALIASES.items()))
def test_alias_table_agrees_with_registry(alias: str, target: str) -> None:
    """Each alias resolves, through the registry, to the application it names."""
    assert get_application(target).name == target
    assert target not in APPLICATION_ALIASES, "aliases must not chain"
    assert type(get_application(alias)) is type(get_application(target))
