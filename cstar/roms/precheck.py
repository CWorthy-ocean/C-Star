"""Reusable Python mirror of ucla-roms' ``src/precheck.F90::do_precheck`` output
stream checks.

ucla-roms >= 0.5.0 aborts at startup (``check_output_divides_rst``) if any
*enabled* output stream's file-rollover frequency (``nrpf * output_period``)
is not positive, or does not evenly divide ``output_period_rst`` -- writing a
restart mid-file would otherwise leave a partial output file. The whole check
is gated on ``basic_output_settings.wrt_file_rst``; four of the per-stream
groups are further gated on a ucla-roms compile-time cppdef (``DIAGNOSTICS``,
and the three MARBL/BGC-diagnostics groups).

:func:`check_output_streams_divide_rst` reproduces this in Python so C-Star
(and consumers building ucla-roms run-time settings, e.g. C-Star Forge) can
fail fast -- at blueprint-authoring time, or when a user overrides run-time
settings directly on a `ROMSSimulation` -- instead of waiting for the run to
abort. It is a plain function, not a Pydantic ``model_validator`` -- it must
not run on every model construction (old blueprints/tests would break) and it
needs an explicit, caller-supplied set of active cppdefs to know which
cppdef-gated groups actually apply.

Operates on C-Star's CANONICAL namelist vocabulary: a mapping of
``RomsNamelistBase`` group field name (the ``&<group>`` header in
``namelist.nml``, e.g. ``"frc_output_settings"``) to that group's real Fortran
namelist keys (e.g. ``"output_period_frc"``, ``"nrpf_frc"``) -- exactly the
shape of ``RomsNamelistBase.model_dump()`` (or an individual group's
``model_dump(by_alias=True)``, dict-assembled). This is deliberately NOT forge's
settings-dict vocabulary (which renames several of these via
``serialization_alias``, e.g. forge's ``frc_output.output_period`` ->
``output_period_frc``) -- a consumer with a forge-shaped dict must first
convert it (see ``cstar_forge.forge.namelist_model.build_namelist`` /
``canonical_output_sections_for_precheck``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


def _get(section: Any, key: str) -> Any:
    """Read ``key`` off ``section``, which is either a plain dict (e.g. a
    ``RomsNamelistBase.model_dump()`` value) or a typed pydantic section model
    (e.g. a live ``RomsNamelistBase`` group instance) -- mirrors
    ``cstar_forge.forge.namelist_model.check_extract_divides_rst``.
    """
    if section is None:
        return None
    return (
        section.get(key) if isinstance(section, dict) else getattr(section, key, None)
    )


@dataclass(frozen=True)
class _StreamCheck:
    """One row of the ``do_precheck`` call list -- kept 1:1 with a line in
    ucla-roms' ``precheck.F90`` so this table stays diffable against the
    Fortran source.

    Parameters
    ----------
    label
        Stream name, matches the Fortran ``label`` argument.
    section
        ``RomsNamelistBase`` group field name (the ``&<group>`` header in
        ``namelist.nml``) the stream's fields live in.
    gate_fields
        Real Fortran namelist key(s), read off ``section``, that gate whether
        ucla-roms writes this stream at all. Combined with ``.or.`` when
        there is more than one (``sflx``: ``wrt_smflx .or. wrt_stflx``;
        ``diagnostics``: ``diag_uv .or. diag_trc``).
    period_field
        Real Fortran namelist key (on ``section``) holding the stream's
        output period.
    nrpf_field
        Real Fortran namelist key (on ``section``) holding the stream's
        records-per-file.
    cppdef_guard
        Cppdef name(s) that must all be active (looked up in the ``cppdefs``
        mapping) for ucla-roms to even compile this stream's write calls.
        ``()`` for the seven groups with no cppdef guard.
    """

    label: str
    section: str
    gate_fields: tuple[str, ...]
    period_field: str
    nrpf_field: str
    cppdef_guard: tuple[str, ...] = ()


# The complete `do_precheck` call list (ucla-roms `src/precheck.F90`), keyed
# on C-Star's canonical namelist vocabulary: `section` is the RomsNamelistBase
# group field name (the `&<group>` header in namelist.nml), and
# `gate_fields`/`period_field`/`nrpf_field` are the real Fortran namelist keys
# within that group. Verified against `cstar/roms/namelist.py` (the group
# classes) and `cstar/tests/unit_tests/roms/fixtures/example_namelist_v0_6_0.nml`
# (the `&<group>` headers).
_STREAM_CHECKS: tuple[_StreamCheck, ...] = (
    _StreamCheck(
        "extract",
        "extract_data_settings",
        ("do_extract",),
        "output_period_extract",
        "nrpf_extract",
    ),
    _StreamCheck(
        "his",
        "basic_output_settings",
        ("wrt_file_his",),
        "output_period_his",
        "nrpf_his",
    ),
    _StreamCheck(
        "avg",
        "basic_output_settings",
        ("wrt_file_avg",),
        "output_period_avg",
        "nrpf_avg",
    ),
    _StreamCheck(
        "frc", "frc_output_settings", ("wrt_frc",), "output_period_frc", "nrpf_frc"
    ),
    _StreamCheck(
        "random",
        "random_output_settings",
        ("do_random",),
        "output_period_random",
        "nrpf_random",
    ),
    _StreamCheck(
        "zslice",
        "zslice_settings",
        ("do_zslice",),
        "output_period_zslice",
        "nrpf_zslice",
    ),
    _StreamCheck(
        "sflx",
        "surf_flx_output_settings",
        ("wrt_smflx", "wrt_stflx"),
        "output_period_sflx",
        "nrpf_sflx",
    ),
    _StreamCheck(
        "particles",
        "particles_settings",
        ("floats",),
        "output_period_particles",
        "nrpf_particles",
    ),
    _StreamCheck(
        "sponge",
        "sponge_tune_settings",
        ("wrt_sponge",),
        "output_period_sponge",
        "nrpf_sponge",
    ),
    _StreamCheck(
        "diagnostics",
        "diagnostics_settings",
        ("diag_uv", "diag_trc"),
        "output_period_diag",
        "nrpf_diag",
        cppdef_guard=("diagnostics",),
    ),
    _StreamCheck(
        "cdr",
        "cdr_output_settings",
        ("do_cdr_output",),
        "output_period_cdr",
        "nrpf_cdr",
        cppdef_guard=("marbl", "marbl_diags", "cdr_forcing"),
    ),
    _StreamCheck(
        "upscale",
        "upscale_settings",
        ("do_upscale",),
        "output_period_uscl",
        "nrpf_uscl",
        cppdef_guard=("marbl", "marbl_diags", "upscaling"),
    ),
    _StreamCheck(
        "bgc_his",
        "bgc_settings",
        ("wrt_bgc_his",),
        "output_period_bgc_his",
        "nrpf_bgc_his",
        cppdef_guard=("marbl_or_bec2",),
    ),
    _StreamCheck(
        "bgc_avg",
        "bgc_settings",
        ("wrt_bgc_avg",),
        "output_period_bgc_avg",
        "nrpf_bgc_avg",
        cppdef_guard=("marbl_or_bec2",),
    ),
    _StreamCheck(
        "bgc_his_dia",
        "bgc_settings",
        ("wrt_bgc_dia_his",),
        "output_period_bgc_his_dia",
        "nrpf_bgc_his_dia",
        cppdef_guard=("marbl_or_bec2",),
    ),
    _StreamCheck(
        "bgc_avg_dia",
        "bgc_settings",
        ("wrt_bgc_dia_avg",),
        "output_period_bgc_avg_dia",
        "nrpf_bgc_avg_dia",
        cppdef_guard=("marbl_or_bec2",),
    ),
)


def _cppdef_guard_satisfied(guard: tuple[str, ...], cppdefs: Mapping[str, Any]) -> bool:
    """True if every name in ``guard`` is active in ``cppdefs``.

    The synthetic name ``"marbl_or_bec2"`` stands in for the Fortran
    ``defined(MARBL) || defined(BIOLOGY_BEC2)`` guard (the four bgc_* groups);
    every other name is looked up directly (``AND``-ed together), matching the
    Fortran `#if defined X && defined Y && defined Z` guards for `cdr` and
    `upscale`. A guard name absent from ``cppdefs`` is treated as inactive.
    """
    for name in guard:
        if name == "marbl_or_bec2":
            if not (cppdefs.get("marbl", False) or cppdefs.get("biology_bec2", False)):
                return False
        elif not cppdefs.get(name, False):
            return False
    return True


def check_output_streams_divide_rst(
    settings: Mapping[str, Any], cppdefs: Mapping[str, Any] | None = None
) -> None:
    """Raise ``ValueError`` if any enabled ucla-roms output stream's file
    rollover frequency (``nrpf * output_period``) would not evenly divide the
    restart period -- mirrors ucla-roms >= 0.5.0's ``check_output_divides_rst``
    (``src/precheck.F90``), which aborts the run at startup otherwise.

    Parameters
    ----------
    settings
        A canonical namelist dump: mapping of ``RomsNamelistBase`` group field
        name (e.g. ``"basic_output_settings"``, ``"frc_output_settings"``) to
        that group, itself either a plain dict of its real Fortran namelist
        keys (e.g. ``RomsNamelistBase.model_dump()``, or a partial dict
        assembled the same way) or a live typed group instance (both forms
        support the same field lookups via :func:`_get`). Must include
        ``basic_output_settings``; every other group is read only if that
        stream applies.
    cppdefs
        Mapping of cppdef name -> whether it is active in this build (e.g.
        ``{"marbl": True, "cdr_forcing": True}``). Governs the four
        cppdef-gated groups (``diagnostics``; the three MARBL/BGC-diagnostics
        groups: ``cdr``, ``upscale``, and the four ``bgc_*`` streams, gated on
        ``MARBL || BIOLOGY_BEC2``). ``None``/absent names are treated as
        inactive, so a caller that doesn't build MARBL/BIOLOGY_BEC2 at all can
        just omit them -- the cppdef-gated groups are then always skipped,
        exactly as ucla-roms itself would (it never compiles their write
        calls).

    Notes
    -----
    - Global gate: if ``basic_output_settings.wrt_file_rst`` is false/missing,
      this function returns immediately without checking anything (mirrors
      ``if (.not. wrt_file_rst) return`` in the Fortran).
    - Each stream is additionally skipped if: its cppdef guard (if any) is
      not fully satisfied; its own enable gate reads as false (a missing gate
      field counts as false -- for a two-field ``.or.`` gate like ``sflx`` or
      ``diagnostics``, one field present-and-true still enables the stream
      even if the other is absent); or its ``nrpf``/period field is missing
      (partial settings dicts -- field *presence* is owned by schema
      validation upstream, not this check).
    - ``output_period_rst == 0`` (the monthly-restart convention) passes
      trivially for every stream, since ``mod(0, x) == 0``.
    - Raises on the *first* violating stream found (in ``do_precheck``'s call
      order), matching :func:`cstar_forge.forge.namelist_model.check_extract_divides_rst`'s
      fail-fast contract -- it does not aggregate every violation.
    """
    basic_output = (
        settings.get("basic_output_settings") if isinstance(settings, Mapping) else None
    )
    if not _get(basic_output, "wrt_file_rst"):
        return
    output_period_rst = _get(basic_output, "output_period_rst")
    if output_period_rst is None:
        return

    cppdefs = cppdefs or {}

    for check in _STREAM_CHECKS:
        if check.cppdef_guard and not _cppdef_guard_satisfied(
            check.cppdef_guard, cppdefs
        ):
            continue

        section = settings.get(check.section) if isinstance(settings, Mapping) else None
        if section is None:
            continue

        # A missing gate field reads as False (disabled), not "skip this
        # stream": for an .or. gate (sflx, diagnostics) one field present-and-
        # True with the other absent must still enable the stream -- `None`
        # is falsy in `any(...)`, so this gives correct OR semantics for both
        # the single-gate and two-gate rows.
        gate_values = [_get(section, f) for f in check.gate_fields]
        if not any(gate_values):
            continue

        nrpf = _get(section, check.nrpf_field)
        period = _get(section, check.period_field)
        if nrpf is None or period is None:
            continue

        newfile_freq = nrpf * period
        ratio = output_period_rst / newfile_freq if newfile_freq > 0 else None
        if ratio is None or abs(ratio - round(ratio)) > 1e-9:
            raise ValueError(
                f"{check.section}.{check.nrpf_field} ({nrpf}) * "
                f"{check.section}.{check.period_field} ({period} s) = "
                f"{newfile_freq} s must be positive and evenly divide "
                f"basic_output_settings.output_period_rst ({output_period_rst} s): "
                f"ucla-roms >= 0.5.0 aborts at startup otherwise "
                f"(check_output_divides_rst, stream '{check.label}', "
                f"partial-file prevention). Adjust {check.section}."
                f"{check.nrpf_field} or {check.section}.{check.period_field}."
            )
