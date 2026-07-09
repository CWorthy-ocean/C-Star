"""Process-global PyYAML insurance for Forge's custom enums.

Forge hands its item configs to roms-tools, which serializes objects with its own
``NoAliasDumper(yaml.SafeDumper)``. ``SafeDumper`` cannot represent a Forge enum
(``class X(str, Enum)``) even though it subclasses ``str`` — it fails with
``('cannot represent an object', <SurfaceType.PHYSICS: 'physics'>)``.

The primary defense is coercing enums to their values at the Forge/roms-tools boundary
(``spec_config_engine._item`` / ``_ic`` dump with ``mode="json"``). This module is the
belt-and-suspenders: a global ``Enum`` representer registered on ``yaml.SafeDumper`` so any
enum that reaches a SafeDumper by some *other* path still serializes as its value. Because
``add_multi_representer`` matches via the type's MRO, a subclass such as roms-tools'
``NoAliasDumper`` inherits it. Imported for its side effect from ``cstar_forge.forge``.

Note: this does NOT fix filename construction (a Forge f-string no dumper touches) — that is
the primary fix's job.
"""

from __future__ import annotations

import enum

import yaml


def _represent_enum(dumper: yaml.Dumper, data: enum.Enum):
    """Represent any Enum as its underlying value (str/int/…)."""
    return dumper.represent_data(data.value)


def register() -> None:
    """Register the global Enum representer on ``yaml.SafeDumper`` (idempotent)."""
    yaml.SafeDumper.add_multi_representer(enum.Enum, _represent_enum)


register()
