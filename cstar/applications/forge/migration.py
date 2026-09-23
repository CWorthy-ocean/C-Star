"""``ForgeBlueprint`` on-disk-data migrations, split out of ``blueprint.py`` so that
importing the blueprint schema (a light, frequently-hit path -- the wizard's
load-back of a saved ``forge_blueprint.yaml``, ``cstar blueprint schemas``, workplan
deserialization) doesn't need to import this module too, and vice versa: nothing here
is needed to define the schema itself.

``migrate_forge_blueprint_data`` is called lazily (imported inside the method body,
not at module level) from ``ForgeBlueprint._migrate_and_clean`` in ``blueprint.py``,
so a plain ``import cstar.applications.forge.blueprint`` never pulls this module in.
This module, in turn, imports ``blueprint.py`` at module level -- ``FORGE_BLUEPRINT_VERSION``,
``sanitize_name``, ``infer_cdr_mode``, and the ``BgcSourceItem``/``BoundaryForcing``
models (for their ``model_fields``) -- which is safe in that direction since
``blueprint.py`` is fully defined before anything imports this module.
"""

from __future__ import annotations

import warnings
from typing import Any

from cstar.applications.forge.blueprint import (
    FORGE_BLUEPRINT_VERSION,
    BgcSourceItem,
    BoundaryForcing,
    infer_cdr_mode,
    sanitize_name,
)


def migrate_forge_blueprint_data(data: dict[str, Any] | None) -> dict[str, Any]:
    """Version-check + forward-migrate a parsed ``forge_blueprint.yaml`` dict.

    Rejects a file declaring a *newer* version than this build understands.

    **v2 -> v3**: pre-v3 ``identity`` shape (``model_name``/``grid_name``/
    ``ensemble_id`` instead of a single ``name``) is rewritten to a single derived
    name and ``grid_name`` is moved onto ``domain``, reproducing the exact old
    derived name (including the ``_{ensemble_id:03d}`` suffix) so ``name``/
    ``casename``/``working_dir`` are preserved bit-for-bit for existing files.

    **v3 -> v4**: the ``identity`` sub-model (``name``/``description``) is flattened
    onto the blueprint's own top-level ``name``/``description`` fields -- required by
    ``ForgeBlueprint``'s ``cstar.orchestration.models.Blueprint`` base, which declares
    ``name``/``description`` as its own top-level fields.

    **v4 -> v5**: ``model_settings.cdr_output.do_cdr`` is renamed ``do_cdr_output``
    (matches the ROMS namelist key; ``CdrOutputCfg`` also carries a
    ``validation_alias`` for this spelling, but the migration keeps the on-disk
    dict itself current).

    **v5 -> v6**: no-op beyond the version bump -- the new user-provided-file
    fields (``domain.grid_file``, ``forcing.river[*].custom_file``,
    ``forcing.cdr_forcing_file``) are purely additive and default to ``None``, so
    a v5 file already validates against the v6 schema unchanged.

    **v6 -> v7**: ``forcing.cdr_forcing``/``forcing.cdr_forcing_file`` (if
    present) are popped and moved onto a new top-level ``cdr`` dict, with
    ``mode`` inferred from which one (if either) was populated: a non-null
    ``cdr_forcing`` dict -> ``"yaml"``; else a non-null ``cdr_forcing_file`` ->
    ``"netcdf"``; else -> ``"none"``. A no-op when ``cdr`` is already present
    (e.g. direct keyword construction passing ``cdr=`` explicitly -- never
    overwrite an explicit value with an inferred one).
    **v7 -> v8**: ``forcing.initial_conditions.bgc_source`` (a single ``SourceSpec``
    dict) is rewrapped as ``forcing.initial_conditions.bgc_sources`` (a one-item
    list of ``{"source": <old dict>}``); absent/``None`` becomes an empty list. If
    both keys are present in the same dict (an inconsistent, likely hand-edited
    file -- genuinely already-migrated data would only ever have ``bgc_sources``),
    raises rather than silently discarding ``bgc_source``.
    Also v7 -> v8: ``forcing.boundary`` collapses from a flat, ``type``-
    discriminated list into a single ``BoundaryForcing`` section -- the
    ``type: physics`` entry supplies ``source`` plus the section's plain fields,
    and every ``type: bgc`` entry becomes a ``bgc_sources`` item. An empty list
    becomes ``None`` (a child domain with no boundary forcing).

    Idempotent and a no-op on already-current data (e.g. direct keyword
    construction, ``ForgeBlueprint(name=..., ...)``) -- called automatically from a
    ``model_validator(mode="before")`` so it fires on every entry point
    (``from_yaml``, ``from_yaml_data``, and C-Star's own ``deserialize``/
    ``model_validate``), not just ``from_yaml``.

    Note: a migrated file's *recorded* ``provenance.content_hash`` was computed
    without ``domain.grid_name`` in the hashed data, so it no longer matches the
    recomputed hash post-migration -- ``verify_content_hash`` only warns on a
    mismatch, and re-saving via ``to_yaml`` recomputes the hash.
    """
    data = dict(data or {})
    version = data.get("forge_blueprint_version")
    if version is not None and version > FORGE_BLUEPRINT_VERSION:
        raise ValueError(
            f"forge_blueprint_version {version} is newer than this build supports "
            f"({FORGE_BLUEPRINT_VERSION}); upgrade cstar-forge to read this file."
        )

    if version is None or version < 3:
        identity = dict(data.get("identity") or {})
        model_name = identity.get("model_name")
        grid_name = identity.get("grid_name")
        if model_name is not None and grid_name is not None:
            ensemble_id = identity.get("ensemble_id")
            domain = dict(data.get("domain") or {})
            partitioning = domain.get("partitioning") or {}
            n_procs = int(partitioning.get("n_procs_x", 1)) * int(
                partitioning.get("n_procs_y", 1)
            )
            name = f"{model_name}_{grid_name}_{n_procs}procs"
            if ensemble_id is not None:
                name += f"_{int(ensemble_id):03d}"
            domain["grid_name"] = grid_name
            data["domain"] = domain
            data["identity"] = {
                "name": sanitize_name(name),
                "description": identity.get("description", "Generated blueprint"),
            }
        # else: already v3-shaped identity (or no identity at all) -- nothing to do.

    if version is None or version < 4:
        identity = data.pop("identity", None)
        if identity is not None:
            data.setdefault("name", identity.get("name"))
            data.setdefault(
                "description", identity.get("description", "Generated blueprint")
            )

    if version is None or version < 5:
        model_settings = data.get("model_settings")
        if isinstance(model_settings, dict):
            cdr_output = model_settings.get("cdr_output")
            if isinstance(cdr_output, dict) and (
                "do_cdr" in cdr_output and "do_cdr_output" not in cdr_output
            ):
                cdr_output["do_cdr_output"] = cdr_output.pop("do_cdr")

    if (version is None or version < 7) and "cdr" not in data:
        forcing = data.get("forcing")
        if isinstance(forcing, dict):
            old_cdr_forcing = forcing.pop("cdr_forcing", None)
            old_cdr_forcing_file = forcing.pop("cdr_forcing_file", None)
            # Mode inference shared with the resolver's convenience kwargs and
            # ForgeExecutor's direct-construction back-compat (defined below,
            # next to CdrSpec) -- the three entry points can't drift apart.
            data["cdr"] = {
                "mode": infer_cdr_mode(old_cdr_forcing, old_cdr_forcing_file)
            }
            if old_cdr_forcing is not None:
                data["cdr"]["cdr_forcing"] = old_cdr_forcing
            elif old_cdr_forcing_file is not None:
                data["cdr"]["cdr_forcing_file"] = old_cdr_forcing_file
        # else: ``forcing`` isn't a plain dict (e.g. an already-built ``Forcing``
        # instance passed via direct keyword construction) -- nothing to migrate
        # off of it; ``cdr`` stays absent so the field default (mode="none")
        # applies.

    if version is None or version < 8:
        forcing = data.get("forcing")
        if isinstance(forcing, dict):
            migrate_forcing_inputs(forcing.get("initial_conditions"), forcing)

    data["forge_blueprint_version"] = FORGE_BLUEPRINT_VERSION
    return data


def migrate_forcing_inputs(
    initial_conditions: dict[str, Any] | None, forcing: dict[str, Any] | None
) -> None:
    """Migrate the pre-v8 forcing-input shapes IN PLACE (idempotent).

    Shared by :func:`migrate_forge_blueprint_data` (where ``initial_conditions``
    lives inside ``forcing``) and by the wizard's ForcingSpec loader (where a
    catalog ``Forcing.yaml`` keeps ``initial_conditions`` and ``forcing`` as
    top-level siblings and is otherwise never migrated) -- one function, so
    the two entry points cannot drift.

    * ``initial_conditions.bgc_source`` (a single source dict) is rewrapped as a
      one-item ``bgc_sources`` list; absent/``None`` becomes ``[]``. Both keys
      present at once is an inconsistent, hand-edited file and raises.
    * ``forcing.boundary`` as a flat, ``type``-discriminated list collapses into
      the single ``BoundaryForcing`` section: the ``type: physics`` item supplies
      ``source`` plus the section's plain fields, every ``type: bgc`` item becomes
      a ``bgc_sources`` entry, and an empty list becomes ``None``.

    Already-current data (a ``bgc_sources`` list, a dict-shaped ``boundary``) is
    left untouched, so calling this on migrated input is a no-op.
    """
    ic = initial_conditions
    if isinstance(ic, dict) and "bgc_source" in ic:
        old_bgc_source = ic.pop("bgc_source")
        if "bgc_sources" in ic:
            # Both the pre-v6 singular key and the v6+ list key are present in
            # the same dict -- an inconsistent/hand-edited file, not "already
            # migrated" data (which would only ever have `bgc_sources`).
            # Silently discarding `old_bgc_source` here would lose a real
            # source with no trace; fail loudly instead.
            raise ValueError(
                "initial_conditions has both the pre-v6 'bgc_source' and the "
                "v6+ 'bgc_sources' -- remove whichever is stale before loading "
                "(this file was likely hand-edited)."
            )
        ic["bgc_sources"] = [{"source": old_bgc_source}] if old_bgc_source else []

    # `forcing.boundary` collapsed from a flat, `type`-discriminated list into a
    # single BoundaryForcing section. Main released v6/v7 with the list shape, so
    # real files carry it and DO need converting: the `type: physics` item
    # becomes the section's own source + plain fields, each `type: bgc` item
    # becomes a `bgc_sources` entry.
    if isinstance(forcing, dict) and isinstance(forcing.get("boundary"), list):
        boundary_list = forcing["boundary"]
        non_dict = [b for b in boundary_list if not isinstance(b, dict)]
        if non_dict:
            # The old code silently filtered these out -- an all-non-dict list
            # became `None` (boundary forcing silently vanishing) and a mixed
            # list silently lost whichever entries weren't dicts. Both are data
            # loss with no trace; a malformed file should fail loudly instead.
            raise ValueError(
                f"forcing.boundary has {len(non_dict)} non-dict entr"
                f"{'y' if len(non_dict) == 1 else 'ies'} -- each pre-v8 "
                "boundary item must be a mapping with a 'type' key; fix the "
                "file (this is not a migratable shape)."
            )
        items = boundary_list
        if not items:
            forcing["boundary"] = None
        else:
            phys = next((b for b in items if b.get("type") in (None, "physics")), None)
            if phys is None:
                raise ValueError(
                    "forcing.boundary has no type='physics' entry to convert "
                    "into the v8 BoundaryForcing section source."
                )
            bgc_keys = set(BgcSourceItem.model_fields)
            sect_keys = set(BoundaryForcing.model_fields) - {
                "source",
                "bgc_sources",
            }
            section: dict[str, Any] = {"source": phys.get("source")}
            for k, v in phys.items():
                if k in sect_keys:
                    section[k] = v
            bgc_sources = []
            for idx, b in enumerate(items):
                if b.get("type") != "bgc":
                    continue
                # Per-item keys that a pre-v8 flat boundary item could carry
                # (e.g. per-item regrid overrides) but `BgcSourceItem` has no
                # slot for -- BgcSourceItem only has source/use_vars/
                # bgc_interpolation_method/serialize_dask, the rest of the old
                # per-item shape lived on the section itself. Warn rather than
                # silently drop so a hand-authored override doesn't vanish
                # without a trace.
                dropped = {
                    k: v
                    for k, v in b.items()
                    if k not in bgc_keys
                    and k != "type"
                    and not (v is None or v is False or v == {} or v == [])
                }
                if dropped:
                    src = b.get("source")
                    src_name = src.get("name") if isinstance(src, dict) else src
                    warnings.warn(
                        f"forcing.boundary[{idx}] (source={src_name!r}) sets "
                        f"{sorted(dropped)}, which are being dropped: per-item "
                        "regrid options are not representable on BgcSourceItem; "
                        "set them on the boundary section instead.",
                        stacklevel=2,
                    )
                bgc_sources.append({k: v for k, v in b.items() if k in bgc_keys})
            section["bgc_sources"] = bgc_sources
            forcing["boundary"] = section
