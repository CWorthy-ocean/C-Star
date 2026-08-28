"""Reusable Python mirror of ucla-roms' ``src/precheck.F90::do_precheck`` output
stream checks.

ucla-roms >= 0.5.0 aborts at startup (``check_output_divides_rst``) if any
*enabled* output stream's file-rollover frequency (``nrpf * output_period``)
is not positive, or does not evenly divide ``output_period_rst`` -- writing a
restart mid-file would otherwise leave a partial output file. The whole check
is gated on ``ocean_vars.wrt_file_rst``; four of the per-stream groups are
further gated on a ucla-roms compile-time cppdef (``DIAGNOSTICS``, and the
three MARBL/BGC-diagnostics groups).

:func:`check_output_streams_divide_rst` reproduces this in Python so C-Star
Forge (and any other consumer building ucla-roms run-time settings) can fail
fast at blueprint-authoring time instead of waiting for the run to abort.
It is a plain function, not a Pydantic ``model_validator`` -- it must not run
on every model construction (old blueprints/tests would break) and it needs
an explicit, caller-supplied set of active cppdefs to know which cppdef-gated
groups actually apply.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


def _get(section: Any, key: str) -> Any:
    """Read ``key`` off ``section``, which is either a plain dict (the
    resolver's world) or a typed pydantic section model (the validator's
    world) -- mirrors ``cstar_forge.forge.namelist_model.check_extract_divides_rst``.
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
        Name of the settings section the stream's fields live in.
    gate_fields
        Field name(s), read off ``section``, that gate whether ucla-roms
        writes this stream at all. Combined with ``.or.`` when there is more
        than one (``sflx``: ``wrt_smflx .or. wrt_stflx``; ``diagnostics``:
        ``diag_uv .or. diag_trc``).
    period_field
        Field name (on ``section``) holding the stream's output period.
    nrpf_field
        Field name (on ``section``) holding the stream's records-per-file.
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


# The complete `do_precheck` call list (ucla-roms `src/precheck.F90`).
# Field names below are C-Star Forge's settings-dict vocabulary (the same
# vocabulary `check_extract_divides_rst` already uses for `extract_data`),
# not the raw Fortran namelist keys -- e.g. `frc_output.nrpf` is the
# `NRPF_FRC` namelist key under `serialization_alias`.
_STREAM_CHECKS: tuple[_StreamCheck, ...] = (
    _StreamCheck("extract", "extract_data", ("do_extract",), "extract_period", "nrpf"),
    _StreamCheck(
        "his", "ocean_vars", ("wrt_file_his",), "output_period_his", "nrpf_his"
    ),
    _StreamCheck(
        "avg", "ocean_vars", ("wrt_file_avg",), "output_period_avg", "nrpf_avg"
    ),
    _StreamCheck("frc", "frc_output", ("wrt_frc",), "output_period", "nrpf"),
    _StreamCheck("random", "random_output", ("do_random",), "output_period", "nrpf"),
    _StreamCheck("zslice", "zslice", ("do_zslice",), "output_period", "nrpf"),
    _StreamCheck(
        "sflx", "surf_flux", ("wrt_smflx", "wrt_stflx"), "output_period", "nrpf"
    ),
    _StreamCheck("particles", "particles", ("floats",), "output_period", "nrpf"),
    _StreamCheck("sponge", "sponge_tune", ("wrt_sponge",), "output_period", "nrpf"),
    _StreamCheck(
        "diagnostics",
        "diagnostics",
        ("diag_uv", "diag_trc"),
        "output_period",
        "nrpf",
        cppdef_guard=("diagnostics",),
    ),
    _StreamCheck(
        "cdr",
        "cdr_output",
        ("do_cdr_output",),
        "output_period",
        "nrpf",
        cppdef_guard=("marbl", "marbl_diags", "cdr_forcing"),
    ),
    _StreamCheck(
        "upscale",
        "upscale_output",
        ("do_upscale",),
        "output_period_uscl",
        "nrpf_uscl",
        cppdef_guard=("marbl", "marbl_diags", "upscaling"),
    ),
    _StreamCheck(
        "bgc_his",
        "bgc",
        ("wrt_his",),
        "output_period_his",
        "nrpf_his",
        cppdef_guard=("marbl_or_bec2",),
    ),
    _StreamCheck(
        "bgc_avg",
        "bgc",
        ("wrt_avg",),
        "output_period_avg",
        "nrpf_avg",
        cppdef_guard=("marbl_or_bec2",),
    ),
    _StreamCheck(
        "bgc_his_dia",
        "bgc",
        ("wrt_his_dia",),
        "output_period_his_dia",
        "nrpf_his_dia",
        cppdef_guard=("marbl_or_bec2",),
    ),
    _StreamCheck(
        "bgc_avg_dia",
        "bgc",
        ("wrt_avg_dia",),
        "output_period_avg_dia",
        "nrpf_avg_dia",
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
        Mapping of section name -> section, where each section is either a
        plain dict or a typed pydantic settings model (both forms support the
        same field lookups via :func:`_get`). Must include ``ocean_vars``;
        every other section is read only if that stream applies.
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
    - Global gate: if ``ocean_vars.wrt_file_rst`` is false/missing, this
      function returns immediately without checking anything (mirrors
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
    ocean_vars = settings.get("ocean_vars") if isinstance(settings, Mapping) else None
    if not _get(ocean_vars, "wrt_file_rst"):
        return
    output_period_rst = _get(ocean_vars, "output_period_rst")
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
                f"ocean_vars.output_period_rst ({output_period_rst} s): "
                f"ucla-roms >= 0.5.0 aborts at startup otherwise "
                f"(check_output_divides_rst, stream '{check.label}', "
                f"partial-file prevention). Adjust {check.section}."
                f"{check.nrpf_field} or {check.section}.{check.period_field}."
            )
