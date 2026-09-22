"""Field/section label glossary for the wizard UI, loaded from bundled YAML.

Deviation from a strict `labels.py` + `labels/__init__.py` layout: a module and
a package of the same name cannot coexist in one directory (the package would
shadow the module), so this package's `__init__.py` carries the code directly
and `blueprint-wizard.yaml` sits beside it -- `importlib.resources.files(__package__)`
then resolves both from the same package.

Each "page" (currently only "blueprint-wizard") is a YAML file with two top-level
keys: ``sections`` (mapping a card/subsection id to a title/desc/required) and
``fields`` (mapping a widget key to a label/symbol/unit/hint/required). Keys
use dotted namespaces -- bare wizard attribute names (``model_dd``), grid
kwargs shared by the main/child/parent grids (``grid.nx``), per-row forcing
widgets (``forcing.row.name``), the initial-conditions/boundary-forcing scalar
panes (``ic.ic_name``, ``boundary.boundary_name``), Advanced settings
(``settings.<section>``, ``settings.<section>.<field>``), and button captions
(``buttons.<name>``).

Lookups (:func:`label_for`, :func:`section_for`) never raise for a missing
key -- they fall back to a default derived from the key itself -- so a YAML
gap degrades to a plain label instead of breaking the UI. Loading the YAML
itself (:func:`glossary`) is strict: an unknown per-entry key, a missing
``label``/``title``, or a non-mapping entry raises ``ValueError`` naming the
offending key, so a typo in the YAML fails loudly in tests rather than
silently falling back everywhere.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from importlib import resources
from typing import Any

import yaml

#: Per-entry keys allowed in a ``fields`` mapping entry.
_ALLOWED_FIELD_KEYS = {"label", "symbol", "unit", "hint", "required"}
#: Per-entry keys allowed in a ``sections`` mapping entry.
_ALLOWED_SECTION_KEYS = {"title", "desc", "required"}


@dataclass(frozen=True)
class Label:
    """Display metadata for one wizard field."""

    label: str
    symbol: str | None = None
    unit: str | None = None
    hint: str | None = None
    required: bool = False


@dataclass(frozen=True)
class Section:
    """Display metadata for one wizard card or subsection."""

    title: str
    desc: str = ""
    required: bool = False


def _as_mapping(value: Any, msg: str) -> dict[str, Any]:
    """Return ``value`` unchanged if it's a mapping, else raise ``ValueError(msg)``.

    ``ValueError`` (not ``TypeError``) is deliberate: this validates untrusted
    YAML content, not a Python call's argument types.
    """
    if isinstance(value, dict):
        return value
    raise ValueError(msg)


def _parse(data: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize raw YAML content into parsed glossary.

    Returns ``{"sections": {id: Section}, "fields": {key: Label}}``. Raises
    ``ValueError`` (naming the offending key) for any unknown per-entry key, a
    missing ``label``/``title``, or an entry that isn't itself a mapping.
    """
    sections: dict[str, Section] = {}
    fields: dict[str, Label] = {}

    raw_sections = _as_mapping(
        data.get("sections") or {},
        "'sections' must be a mapping of id -> {title, desc?, required?}",
    )
    for key, entry in raw_sections.items():
        entry = _as_mapping(
            entry, f"sections.{key} must be a mapping, got {type(entry).__name__}"
        )
        unknown = set(entry) - _ALLOWED_SECTION_KEYS
        if unknown:
            raise ValueError(f"sections.{key} has unknown key(s): {sorted(unknown)}")
        if "title" not in entry:
            raise ValueError(f"sections.{key} is missing required 'title'")
        sections[key] = Section(
            title=str(entry["title"]),
            desc=str(entry.get("desc", "")),
            required=bool(entry.get("required", False)),
        )

    raw_fields = _as_mapping(
        data.get("fields") or {},
        "'fields' must be a mapping of key -> "
        "{label, symbol?, unit?, hint?, required?}",
    )
    for key, entry in raw_fields.items():
        entry = _as_mapping(
            entry, f"fields.{key} must be a mapping, got {type(entry).__name__}"
        )
        unknown = set(entry) - _ALLOWED_FIELD_KEYS
        if unknown:
            raise ValueError(f"fields.{key} has unknown key(s): {sorted(unknown)}")
        if "label" not in entry:
            raise ValueError(f"fields.{key} is missing required 'label'")
        fields[key] = Label(
            label=str(entry["label"]),
            symbol=entry.get("symbol"),
            unit=entry.get("unit"),
            hint=entry.get("hint"),
            required=bool(entry.get("required", False)),
        )

    return {"sections": sections, "fields": fields}


@functools.cache
def glossary(page: str = "blueprint-wizard") -> dict[str, Any]:
    """Load and cache the parsed glossary for ``page`` (e.g. ``"blueprint-wizard"``).

    ``page`` names a YAML file (``<page>.yaml``) bundled next to this module.
    """
    raw_text = resources.files(__package__).joinpath(f"{page}.yaml").read_text()
    data = yaml.safe_load(raw_text) or {}
    return _parse(data)


def label_for(
    key: str, default: str | None = None, *, page: str = "blueprint-wizard"
) -> Label:
    """Look up the :class:`Label` for ``key``.

    Never raises for a missing key: falls back to ``Label(label=default or
    the last dotted segment of key)``.
    """
    fields = glossary(page)["fields"]
    if key in fields:
        return fields[key]
    return Label(label=default or key.rsplit(".", 1)[-1])


def section_for(
    key: str, default_title: str = "", *, page: str = "blueprint-wizard"
) -> Section:
    """Look up the :class:`Section` for ``key``.

    Never raises for a missing key: falls back to ``Section(title=
    default_title or key)``.
    """
    sections = glossary(page)["sections"]
    if key in sections:
        return sections[key]
    return Section(title=default_title or key)


def known_keys(page: str = "blueprint-wizard") -> set[str]:
    """The set of field keys defined for ``page``."""
    return set(glossary(page)["fields"])


def known_sections(page: str = "blueprint-wizard") -> set[str]:
    """The set of section ids defined for ``page``."""
    return set(glossary(page)["sections"])
