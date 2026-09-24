"""The single home for C-Star's ROMS namelist-consistency rules -- Python
mirrors of checks ucla-roms itself enforces (or, for
:func:`check_restart_period_divisible_by_dt`, a stricter convention C-Star
layers on top), reproduced here so callers can fail fast at blueprint-
authoring time or when a user overrides run-time settings directly, instead
of waiting for the run to abort.

Rule A -- :func:`check_output_streams_divide_rst` mirrors ucla-roms'
``src/precheck.F90::do_precheck``/``check_output_divides_rst``: ucla-roms >=
0.5.0 aborts at startup if any *enabled* output stream's file-rollover
frequency (``nrpf * output_period``) is not positive, or does not evenly
divide ``output_period_rst`` -- writing a restart mid-file would otherwise
leave a partial output file. The whole check is gated on
``basic_output_settings.wrt_file_rst``; six of the per-stream groups are
further gated on a ucla-roms compile-time cppdef (``DIAGNOSTICS``, and the
five MARBL/BGC-diagnostics groups: ``cdr``, ``cdrtrc``, ``cdrgas``,
``upscale``, and ``bgc``). :func:`applies_to` owns the ">= 0.5.0" schema gate.

Rule B -- :func:`check_restart_period_divisible_by_dt` is not a ucla-roms
abort (ucla-roms' restart trigger is a running-clock threshold, not a
step-count division -- a non-multiple period just drifts the actual restart
time by up to one ``dt``); it is a reproducibility convention, ungated by
schema version.

Both are plain functions, not Pydantic ``model_validator``s -- they must not
run on every model construction (old blueprints/tests would break), and Rule
A needs an explicit, caller-supplied set of active cppdefs to know which
cppdef-gated groups actually apply.

Both operate on C-Star's CANONICAL namelist vocabulary: a mapping (or a live
``RomsNamelistBase``) of group field name (the ``&<group>`` header in
``namelist.nml``, e.g. ``"frc_output_settings"``) to that group's real Fortran
namelist keys (e.g. ``"output_period_frc"``, ``"nrpf_frc"``) -- exactly the
shape of ``RomsNamelistBase.model_dump()`` (or an individual group's
``model_dump(by_alias=True)``, dict-assembled), or the live group instance
itself. This is deliberately NOT forge's settings-dict vocabulary (which
renames several of these via ``serialization_alias``, e.g. forge's
``frc_output.output_period`` -> ``output_period_frc``) -- a consumer with a
forge-shaped dict must first convert it (see
``cstar.applications.forge.namelist_model.build_namelist`` /
``canonical_output_sections_for_precheck``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cstar.roms.namelist import RomsNamelist, RomsNamelistBase


class NamelistConsistencyError(ValueError):
    """A namelist-consistency rule in this module was violated.

    Subclasses ``ValueError`` so existing ``pytest.raises(ValueError, ...)``
    call sites (and any ``except ValueError`` caller) keep working unchanged.
    Carries the canonical location of the violation -- ``section`` (a
    ``RomsNamelistBase`` group field name) and ``keys`` (the real Fortran
    namelist key(s) within that group) -- so a consumer that edits a
    different vocabulary (e.g. Forge's settings dict) can point a user at the
    field it actually exposes, without this module knowing that vocabulary
    exists. ``rule`` names which check raised (``"output_streams_divide_rst"``
    or ``"restart_period_divisible_by_dt"``).
    """

    def __init__(
        self, message: str, *, rule: str, section: str, keys: tuple[str, ...]
    ) -> None:
        super().__init__(message)
        self.rule = rule
        self.section = section
        self.keys = keys


def applies_to(schema: type[RomsNamelistBase]) -> bool:
    """True if Rule A (:func:`check_output_streams_divide_rst`) applies to
    namelist schema ``schema``.

    ucla-roms' ``check_output_divides_rst`` (``src/precheck.F90``) was added
    in 0.5.0: ``schema`` is checked against the legacy, unversioned
    ``RomsNamelist`` (< 0.5.0) -- every other registered schema
    (``RomsNamelistV0_5_0``, ``RomsNamelistV0_6_0``, ``RomsNamelistV0_7_0``)
    subclasses ``RomsNamelistBase`` directly (or a later version), never
    ``RomsNamelist``, so ``issubclass`` here is exactly the >= 0.5.0 gate.
    Rule B is ungated -- it is not version-specific (see the module
    docstring) -- so it has no ``applies_to`` counterpart.
    """
    return not issubclass(schema, RomsNamelist)


def _section(container: Any, name: str) -> Any:
    """Read group/field ``name`` off ``container``, which is either a plain
    mapping (e.g. a ``RomsNamelistBase.model_dump()`` value, or the whole
    settings mapping itself) or a live pydantic model (a ``RomsNamelistBase``
    instance, or one of its group instances). One helper for both the
    top-level group lookup and the per-group field lookup, since both are the
    same operation: read an attribute off a dict-or-object.
    """
    if container is None:
        return None
    return (
        container.get(name)
        if isinstance(container, Mapping)
        else getattr(container, name, None)
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
# / `example_namelist_v0_7_0.nml` (the `&<group>` headers).
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
        "cdrtrc",
        "cdr_tracer_output_settings",
        ("do_cdr_tracer_output",),
        "output_period_cdr_trc",
        "nrpf_cdr_trc",
        cppdef_guard=("marbl", "cdr_forcing"),
    ),
    _StreamCheck(
        "cdrgas",
        "cdr_gas_exch_output_settings",
        ("do_cdr_gas_exch_output",),
        "output_period_cdr_gas",
        "nrpf_cdr_gas",
        cppdef_guard=("marbl", "cdr_forcing"),
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
    settings: RomsNamelistBase | Mapping[str, Any],
    cppdefs: Mapping[str, Any] | None = None,
) -> None:
    """Raise ``NamelistConsistencyError`` if any enabled ucla-roms output
    stream's file rollover frequency (``nrpf * output_period``) would not
    evenly divide the restart period -- mirrors ucla-roms >= 0.5.0's
    ``check_output_divides_rst`` (``src/precheck.F90``), which aborts the run
    at startup otherwise.

    Parameters
    ----------
    settings
        A canonical namelist dump: either a live ``RomsNamelistBase``
        instance, or a mapping of ``RomsNamelistBase`` group field name (e.g.
        ``"basic_output_settings"``, ``"frc_output_settings"``) to that
        group, itself either a plain dict of its real Fortran namelist keys
        (e.g. ``RomsNamelistBase.model_dump()``, or a partial dict assembled
        the same way) or a live typed group instance (all forms support the
        same field lookups via :func:`_section`). Must include
        ``basic_output_settings``; every other group is read only if that
        stream applies.
    cppdefs
        Mapping of cppdef name -> whether it is active in this build (e.g.
        ``{"marbl": True, "cdr_forcing": True}``). Governs the six
        cppdef-gated groups (``diagnostics``; the five MARBL/BGC-diagnostics
        groups: ``cdr``, ``cdrtrc``, ``cdrgas``, ``upscale``, and the four
        ``bgc_*`` streams, gated on ``MARBL || BIOLOGY_BEC2``). ``None``/absent
        names are treated as
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
      order) -- it does not aggregate every violation.
    - This includes the ``extract`` stream (nesting extraction) like every
      other row in ``_STREAM_CHECKS``; a caller that wants a more actionable
      message for that stream specifically (e.g. naming the authoring-time
      knob that produced it) can catch ``NamelistConsistencyError`` and check
      ``exc.section == "extract_data_settings"``.
    """
    basic_output = _section(settings, "basic_output_settings")
    if not _section(basic_output, "wrt_file_rst"):
        return
    output_period_rst = _section(basic_output, "output_period_rst")
    if output_period_rst is None:
        return

    cppdefs = cppdefs or {}

    for check in _STREAM_CHECKS:
        if check.cppdef_guard and not _cppdef_guard_satisfied(
            check.cppdef_guard, cppdefs
        ):
            continue

        section = _section(settings, check.section)
        if section is None:
            continue

        # A missing gate field reads as False (disabled), not "skip this
        # stream": for an .or. gate (sflx, diagnostics) one field present-and-
        # True with the other absent must still enable the stream -- `None`
        # is falsy in `any(...)`, so this gives correct OR semantics for both
        # the single-gate and two-gate rows.
        gate_values = [_section(section, f) for f in check.gate_fields]
        if not any(gate_values):
            continue

        nrpf = _section(section, check.nrpf_field)
        period = _section(section, check.period_field)
        if nrpf is None or period is None:
            continue

        newfile_freq = nrpf * period
        ratio = output_period_rst / newfile_freq if newfile_freq > 0 else None
        if ratio is None or abs(ratio - round(ratio)) > 1e-9:
            raise NamelistConsistencyError(
                f"{check.section}.{check.nrpf_field} ({nrpf}) * "
                f"{check.section}.{check.period_field} ({period} s) = "
                f"{newfile_freq} s must be positive and evenly divide "
                f"basic_output_settings.output_period_rst ({output_period_rst} s): "
                f"ucla-roms >= 0.5.0 aborts at startup otherwise "
                f"(check_output_divides_rst, stream '{check.label}', "
                f"partial-file prevention). Adjust {check.section}."
                f"{check.nrpf_field} or {check.section}.{check.period_field}.",
                rule="output_streams_divide_rst",
                section=check.section,
                keys=(check.nrpf_field, check.period_field),
            )


def check_restart_period_divisible_by_dt(
    settings: RomsNamelistBase | Mapping[str, Any],
) -> None:
    """Raise ``NamelistConsistencyError`` if
    ``basic_output_settings.output_period_rst`` isn't an integer multiple of
    ``time_stepping.dt`` -- restart writes must land on a timestep (see the
    module docstring: this is a C-Star reproducibility convention, not a
    ucla-roms abort).

    Enforced only when restarts are written on a fixed period
    (``basic_output_settings.wrt_file_rst`` True and
    ``basic_output_settings.monthly_restarts`` False); otherwise
    ``output_period_rst`` is unused and any value is accepted. ``dt``
    missing/non-positive skips the check (other validation owns ``dt``
    sanity).

    Parameters
    ----------
    settings
        A canonical namelist dump: either a live ``RomsNamelistBase``
        instance, or a mapping of ``"time_stepping"``/``"basic_output_settings"``
        to that group (a plain dict of its real Fortran namelist keys, or a
        live typed group instance -- see :func:`check_output_streams_divide_rst`
        for the full shape). Missing sections/fields skip the check.
    """
    time_stepping = _section(settings, "time_stepping")
    basic_output = _section(settings, "basic_output_settings")
    dt = _section(time_stepping, "dt")
    wrt_file_rst = _section(basic_output, "wrt_file_rst")
    monthly_restarts = _section(basic_output, "monthly_restarts")
    output_period_rst = _section(basic_output, "output_period_rst")

    if not wrt_file_rst or monthly_restarts:
        return
    if dt is None or output_period_rst is None or dt <= 0:
        return
    ratio = output_period_rst / dt
    if abs(ratio - round(ratio)) > 1e-9:
        raise NamelistConsistencyError(
            f"basic_output_settings.output_period_rst ({output_period_rst} s) is "
            f"not an integer multiple of time_stepping.dt ({dt} s): restart "
            "writes must land on a timestep",
            rule="restart_period_divisible_by_dt",
            section="basic_output_settings",
            keys=("output_period_rst",),
        )
