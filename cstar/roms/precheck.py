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
five MARBL/BGC-diagnostics groups: ``cdr``, the 0.7/0.8-only ``cdrtrc``,
``cdrgas``, ``upscale``, and ``bgc``). ucla-roms >= 0.9.0 compiles its
replacement for ``cdrtrc`` (``&CDR_LITE_OUTPUT_SETTINGS``) unconditionally, so
it has a second, unguarded ``cdrtrc`` row. ucla-roms >= 0.13.0 writes, and so
checks, the surface-flux file for any of its four switches, not only
``wrt_smflx``/``wrt_stflx``, so ``sflx`` has one row per schema range.
:func:`applies_to` owns the ">= 0.5.0" schema gate.

Rule B -- :func:`check_restart_period_divisible_by_dt` is not a ucla-roms
abort (ucla-roms' restart trigger is a running-clock threshold, not a
step-count division -- a non-multiple period just drifts the actual restart
time by up to one ``dt``); it is a reproducibility convention, ungated by
schema version.

Rule C -- :func:`check_averages_end_on_marbl_step` mirrors ucla-roms >= 0.13.0's
``check_avg_ends_on_marbl_step`` (``src/precheck.F90``, PR #400): the averaged
``_cdr`` and ``_bgc_dia_avg`` streams must average over whole MARBL timesteps.
Forge gates it on the pinned release with
:func:`averages_end_on_marbl_step_applies_to`, so that an unresolved commit hash,
which selects the newest schema, is not checked; the ``ucla_roms`` path, which has
resolved the schema, gates on it with
:func:`averages_end_on_marbl_step_applies_to_schema`.

Rule D -- :func:`check_file_name_lengths` mirrors ucla-roms' fixed-size file-name
buffers (``max_name_size = 256`` in ``src/param.F90``): ucla-roms >= 0.13.0 aborts at
startup if ``grdname``, ``inifile``, a ``frcfiles`` entry, ``cdr_file``,
``extract_file`` or ``marbl_config_file`` fills its buffer (256 characters or more,
which it takes to be cut off), or if ``output_root_name`` or ``extract_root_name``
is longer than 220 characters (the longest output-file tail ROMS appends is 36);
earlier releases cut a longer name off silently. C-Star applies the same limits to
every release, with headroom for the rank suffix a non-PIO run appends, which
ROMS's own check does not allow for. Ungated by schema version.

All are plain functions, not Pydantic ``model_validator``s -- they must not
run on every model construction (old blueprints/tests would break), and Rules
A and C need an explicit, caller-supplied set of active cppdefs to know which
cppdef-gated groups actually apply.

All operate on C-Star's CANONICAL namelist vocabulary: a mapping (or a live
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

from cstar.roms.namelist import (
    UCLA_ROMS_0_13_0,
    RomsNamelist,
    RomsNamelistBase,
    RomsNamelistV0_13_0,
    is_commit_hash,
    roms_version_from_ref,
)

AVERAGES_END_ON_MARBL_STEP_MIN_ROMS: tuple[int, int, int] = UCLA_ROMS_0_13_0
"""ucla-roms release from which ROMS checks Rule C at startup (PR #400)."""

MAX_NAMELIST_FILE_NAME_LENGTH: int = 250
"""Longest file name C-Star writes into a namelist key ROMS reads into a
``character(len=max_name_size)`` buffer. Mirrors ``max_name_size = 256`` in ucla-roms
``src/param.F90``, less headroom for the rank suffix a non-PIO ``PARALLEL_FILES`` run
appends, which ROMS's own check (names of 256 or more characters) misses."""

MAX_OUTPUT_ROOT_NAME_LENGTH: int = 220
"""Longest ``output_root_name`` / ``extract_root_name``. Mirrors the limit ucla-roms
>= 0.13.0 checks: ``max_name_size`` less 36, the longest tail ROMS appends."""

_FILE_NAME_KEYS: tuple[tuple[str, str, int], ...] = (
    ("grid_settings", "grdname", MAX_NAMELIST_FILE_NAME_LENGTH),
    ("initial_conditions", "inifile", MAX_NAMELIST_FILE_NAME_LENGTH),
    ("forcing_files", "frcfiles", MAX_NAMELIST_FILE_NAME_LENGTH),
    ("cdr_frc_settings", "cdr_file", MAX_NAMELIST_FILE_NAME_LENGTH),
    ("extract_data_settings", "extract_file", MAX_NAMELIST_FILE_NAME_LENGTH),
    (
        "marbl_biogeochemistry_settings",
        "marbl_config_file",
        MAX_NAMELIST_FILE_NAME_LENGTH,
    ),
    ("simulation_name_settings", "output_root_name", MAX_OUTPUT_ROOT_NAME_LENGTH),
    ("extract_data_settings", "extract_root_name", MAX_OUTPUT_ROOT_NAME_LENGTH),
)
"""``(group, key, longest allowed)`` for every namelist value ROMS reads into a
``max_name_size`` buffer. A key that holds several values (``frcfiles``, and the
per-child ``extract_*`` keys of ucla-roms >= 0.12.0) is checked entry by entry."""


class NamelistConsistencyError(ValueError):
    """A namelist-consistency rule in this module was violated.

    Subclasses ``ValueError`` so existing ``pytest.raises(ValueError, ...)``
    call sites (and any ``except ValueError`` caller) keep working unchanged.
    Carries the canonical location of the violation -- ``section`` (a
    ``RomsNamelistBase`` group field name) and ``keys`` (the real Fortran
    namelist key(s) within that group) -- so a consumer that edits a
    different vocabulary (e.g. Forge's settings dict) can point a user at the
    field it actually exposes, without this module knowing that vocabulary
    exists. ``rule`` names which check raised (``"output_streams_divide_rst"``,
    ``"restart_period_divisible_by_dt"`` or ``"averages_end_on_marbl_step"``).
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
    ``RomsNamelist`` (< 0.4.0), which ``RomsNamelistV0_4_0`` (< 0.5.0)
    subclasses -- every >= 0.5.0 schema (``RomsNamelistV0_5_0``,
    ``RomsNamelistV0_6_0``, ``RomsNamelistV0_7_0``, ``RomsNamelistV0_9_0``,
    ``RomsNamelistV0_11_0``, ``RomsNamelistV0_12_0``, ``RomsNamelistV0_13_0``) subclasses
    ``RomsNamelistBase`` directly (or a later version), never ``RomsNamelist``,
    so ``issubclass`` here is exactly the >= 0.5.0 gate.
    Rule B is ungated -- it is not version-specific (see the module
    docstring) -- so it has no ``applies_to`` counterpart.
    """
    return not issubclass(schema, RomsNamelist)


def averages_end_on_marbl_step_applies_to(roms_ref: str | None) -> bool:
    """True if Rule C (:func:`check_averages_end_on_marbl_step`) applies to the
    ucla-roms pin ``roms_ref``.

    That is a release tag at or above :data:`AVERAGES_END_ON_MARBL_STEP_MIN_ROMS`,
    or a branch or ``None``, which counts as the latest release, as in Forge's
    other release-gated checks. A commit hash is not checked: the rule refuses
    runs, and a hash from before 0.13.0 must not be refused for one its release
    accepts. A caller that can resolve the hash (``process_forge_blueprint``,
    ``configure_build``) passes the release tag it resolves to, as the resolver
    leaves the hash-dependent ``cdr_nz_chd`` default to them.
    """
    if is_commit_hash(roms_ref):
        return False
    version = roms_version_from_ref(roms_ref)
    return version is None or version >= AVERAGES_END_ON_MARBL_STEP_MIN_ROMS


def averages_end_on_marbl_step_applies_to_schema(
    schema: type[RomsNamelistBase],
) -> bool:
    """True if Rule C (:func:`check_averages_end_on_marbl_step`) applies to
    namelist schema ``schema``.

    The ``ucla_roms`` path gates on the schema where Forge gates on the ref
    (:func:`averages_end_on_marbl_step_applies_to`): by the time it reads the
    namelist it has resolved a commit-hash pin to a schema through the clone or
    the build record, so a hash from before 0.13.0 is not refused for a rule its
    release does not have. (Without either, a hash falls back to the newest schema
    with a warning, as it does for Rule A.) Every schema from
    ``RomsNamelistV0_13_0`` on subclasses it.
    """
    return issubclass(schema, RomsNamelistV0_13_0)


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
        there is more than one (``sflx``: ``wrt_smflx .or. wrt_stflx`` before
        ucla-roms 0.13.0, ``do_sflx``'s four switches since;
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
        ``()`` for the groups with no cppdef guard.
    since, before
        For a row whose gate changed between releases: the namelist schema
        must subclass ``since`` and must not subclass ``before``. ``None``
        leaves that side open.
    """

    label: str
    section: str
    gate_fields: tuple[str, ...]
    period_field: str
    nrpf_field: str
    cppdef_guard: tuple[str, ...] = ()
    since: type[RomsNamelistBase] | None = None
    before: type[RomsNamelistBase] | None = None

    def applies_to(self, schema: type[RomsNamelistBase] | None) -> bool:
        """True if this row is in ``schema``'s call list; ``None`` is the newest."""
        if schema is None:
            return self.before is None
        if self.since is not None and not issubclass(schema, self.since):
            return False
        return self.before is None or not issubclass(schema, self.before)


# The `do_precheck` call list (ucla-roms `src/precheck.F90`), keyed on C-Star's
# canonical namelist vocabulary: `section` is the RomsNamelistBase group field
# name (the `&<group>` header in namelist.nml), and
# `gate_fields`/`period_field`/`nrpf_field` are the real Fortran namelist keys
# within that group. Verified against `cstar/roms/namelist.py` (the group
# classes) and `cstar/tests/unit_tests/roms/fixtures/example_namelist_v0_6_0.nml`
# / `example_namelist_v0_7_0.nml` / `example_namelist_v0_9_0.nml` (the
# `&<group>` headers). The `cdrtrc` label appears twice, once per namelist
# generation (see the two rows below); `check_output_streams_divide_rst` skips
# a section absent from the settings, so only the row matching the schema in
# use ever fires.
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
        before=RomsNamelistV0_13_0,
    ),
    # ucla-roms >= 0.13.0 (PR #407): `do_sflx`, set by any of the four switches
    _StreamCheck(
        "sflx",
        "surf_flx_output_settings",
        ("wrt_smflx", "wrt_stflx", "wrt_swflx", "wrt_rstflx"),
        "output_period_sflx",
        "nrpf_sflx",
        since=RomsNamelistV0_13_0,
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
    # ucla-roms 0.7.0 - 0.8.x (`&CDR_TRACER_OUTPUT_SETTINGS`)
    _StreamCheck(
        "cdrtrc",
        "cdr_tracer_output_settings",
        ("do_cdr_tracer_output",),
        "output_period_cdr_trc",
        "nrpf_cdr_trc",
        cppdef_guard=("marbl", "cdr_forcing"),
    ),
    # ucla-roms >= 0.9.0 (`&CDR_LITE_OUTPUT_SETTINGS`, compiled unconditionally)
    _StreamCheck(
        "cdrtrc",
        "cdr_lite_output_settings",
        ("do_cdr_lite_output",),
        "output_period_cdr_lite",
        "nrpf_cdr_lite",
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
    *,
    schema: type[RomsNamelistBase] | None = None,
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
        groups: ``cdr``, the 0.7/0.8 ``cdrtrc``, ``cdrgas``, ``upscale``, and
        the four ``bgc_*`` streams, gated on ``MARBL || BIOLOGY_BEC2``).
        ucla-roms >= 0.9.0's ``cdrtrc`` (``cdr_lite_output_settings``) is
        unguarded. ``None``/absent names are treated as
        inactive, so a caller that doesn't build MARBL/BIOLOGY_BEC2 at all can
        just omit them -- the cppdef-gated groups are then always skipped,
        exactly as ucla-roms itself would (it never compiles their write
        calls).
    schema
        The namelist schema of the pinned release, which selects the rows whose
        gate changed between releases (``sflx``). Defaults to
        ``type(settings)`` for a live ``RomsNamelistBase``; ``None`` otherwise
        counts as the newest schema, as a branch pin does.

    Notes
    -----
    - Global gate: if ``basic_output_settings.wrt_file_rst`` is false/missing,
      this function returns immediately without checking anything (mirrors
      ``if (.not. wrt_file_rst) return`` in the Fortran).
    - Each stream is additionally skipped if: its row is not in ``schema``'s
      call list; its cppdef guard (if any) is
      not fully satisfied; its section is absent from ``settings`` (a
      namelist carries only one of the two ``cdrtrc`` groups, which is what
      lets both rows coexist); its own enable gate reads as false (a missing gate
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
    if schema is None and isinstance(settings, RomsNamelistBase):
        schema = type(settings)

    for check in _STREAM_CHECKS:
        if not check.applies_to(schema):
            continue
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


def marbl_step(dt: float, marbl_timestep: float) -> float:
    """The MARBL timestep ucla-roms uses: ``marbl_timestep`` rounded down to a whole
    multiple of ``dt``, at least one ``dt`` (``marbldrv_configure_tracers``).
    """
    return dt * max(1, int(marbl_timestep / dt))


def is_whole_multiple(value: float, step: float) -> bool:
    """Whether ``value`` is a whole multiple of ``step`` (0 included), to round-off."""
    ratio = value / step
    return abs(ratio - round(ratio)) <= 1e-9


@dataclass(frozen=True)
class _AverageCheck:
    """One ``check_avg_ends_on_marbl_step`` call in ucla-roms' ``do_precheck``.

    Parameters
    ----------
    section
        ``RomsNamelistBase`` group field name the stream's fields live in.
    gate_fields
        Real Fortran namelist keys that must all be true for ROMS to check the
        stream (``.and.``-ed, as in the Fortran call).
    off_fields
        Real Fortran namelist keys that must all be false for ROMS to check it.
    period_field
        Real Fortran namelist key holding the averaging period.
    cppdef_guard
        Cppdef names that must all be active for ROMS to compile the call.
    """

    section: str
    gate_fields: tuple[str, ...]
    off_fields: tuple[str, ...]
    period_field: str
    cppdef_guard: tuple[str, ...]


_AVERAGE_CHECKS: tuple[_AverageCheck, ...] = (
    _AverageCheck(
        "cdr_output_settings",
        ("do_cdr_output", "wrt_cdr_avg"),
        ("cdr_monthly_averages",),
        "output_period_cdr",
        ("marbl", "marbl_diags", "cdr_forcing"),
    ),
    _AverageCheck(
        "bgc_settings",
        ("wrt_bgc_dia_avg",),
        (),
        "output_period_bgc_avg_dia",
        ("marbl", "marbl_diags"),
    ),
)


def check_averages_end_on_marbl_step(
    settings: RomsNamelistBase | Mapping[str, Any],
    cppdefs: Mapping[str, Any] | None = None,
) -> None:
    """Raise ``NamelistConsistencyError`` if an averaged MARBL stream's period is
    not a whole multiple of the MARBL timestep ROMS uses -- mirrors ucla-roms >=
    0.13.0's ``check_avg_ends_on_marbl_step`` (``src/precheck.F90``), which
    aborts the run at startup otherwise.

    ROMS rounds ``marbl_timestep`` down to a whole multiple of ``dt`` (at least
    one ``dt``) and checks the ``_cdr`` average (``do_cdr_output`` and
    ``wrt_cdr_avg`` on, ``cdr_monthly_averages`` off; built with MARBL,
    MARBL_DIAGS and CDR_FORCING) and the ``_bgc_dia_avg`` average
    (``wrt_bgc_dia_avg`` on; built with MARBL and MARBL_DIAGS) against it.
    Unlike Rule A, it does not depend on ``wrt_file_rst``. Gate the call on the
    pinned release with :func:`averages_end_on_marbl_step_applies_to`, or on the
    namelist schema with :func:`averages_end_on_marbl_step_applies_to_schema`.

    Parameters
    ----------
    settings
        A canonical namelist dump, as for :func:`check_output_streams_divide_rst`.
        Reads ``time_stepping.dt``, ``marbl_biogeochemistry_settings.marbl_timestep``
        and the two streams' groups; a missing group or field skips what needs it.
    cppdefs
        Mapping of cppdef name -> whether it is active in this build, as for
        :func:`check_output_streams_divide_rst`.

    Notes
    -----
    Raises on the first violating stream, in ``do_precheck``'s call order.
    """
    dt = _section(_section(settings, "time_stepping"), "dt")
    marbl_timestep = _section(
        _section(settings, "marbl_biogeochemistry_settings"), "marbl_timestep"
    )
    if dt is None or marbl_timestep is None or dt <= 0:
        return
    marbl_dt = marbl_step(dt, marbl_timestep)

    cppdefs = cppdefs or {}
    for check in _AVERAGE_CHECKS:
        if not _cppdef_guard_satisfied(check.cppdef_guard, cppdefs):
            continue
        section = _section(settings, check.section)
        if section is None:
            continue
        if not all(_section(section, f) for f in check.gate_fields) or any(
            _section(section, f) for f in check.off_fields
        ):
            continue
        period = _section(section, check.period_field)
        if period is None:
            continue
        if not is_whole_multiple(period, marbl_dt):
            minimum = ".".join(
                str(part) for part in AVERAGES_END_ON_MARBL_STEP_MIN_ROMS
            )
            raise NamelistConsistencyError(
                f"{check.section}.{check.period_field} ({period} s) is not a whole "
                f"multiple of the MARBL timestep ROMS uses, {marbl_dt} s "
                "(marbl_biogeochemistry_settings.marbl_timestep "
                f"{marbl_timestep} s rounded down to a multiple of "
                f"time_stepping.dt {dt} s): ucla-roms >= {minimum} aborts at startup "
                "otherwise (check_avg_ends_on_marbl_step). Adjust "
                f"{check.section}.{check.period_field} or "
                "marbl_biogeochemistry_settings.marbl_timestep.",
                rule="averages_end_on_marbl_step",
                section=check.section,
                keys=(check.period_field,),
            )


def check_file_name_lengths(settings: RomsNamelistBase | Mapping[str, Any]) -> None:
    """Raise ``NamelistConsistencyError`` if a file name in the namelist is too long
    for ROMS to read -- mirrors ucla-roms' ``max_name_size`` buffers (see Rule D in
    the module docstring).

    Parameters
    ----------
    settings
        A canonical namelist dump, as for :func:`check_output_streams_divide_rst`.
        Reads the keys in ``_FILE_NAME_KEYS``; a missing group or key is skipped.

    Notes
    -----
    Reports every offending key, with its value and length, in one error.
    """
    too_long: list[tuple[str, str, str]] = []
    for group, key, limit in _FILE_NAME_KEYS:
        value = _section(_section(settings, group), key)
        names = [value] if isinstance(value, str) else value or []
        for i, name in enumerate(names):
            if len(name) > limit:
                label = f"{group}.{key}" + (f"[{i}]" if len(names) > 1 else "")
                too_long.append(
                    (group, key, f"{label} ({len(name)} > {limit}): {name}")
                )
    if too_long:
        raise NamelistConsistencyError(
            "ROMS reads these namelist file names into fixed-size buffers and "
            "the names below are too long for them (ucla-roms >= 0.13.0 stops at "
            "startup; earlier releases cut the name off silently):\n  "
            + "\n  ".join(detail for _, _, detail in too_long)
            + "\nUse a shorter run directory or shorter input file names.",
            rule="file_name_lengths",
            section=", ".join(dict.fromkeys(group for group, _, _ in too_long)),
            keys=tuple(dict.fromkeys(key for _, key, _ in too_long)),
        )
