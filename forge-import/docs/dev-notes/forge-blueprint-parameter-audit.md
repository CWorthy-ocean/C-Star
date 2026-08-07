# ForgeBlueprint parameter audit

**Status: living document — update this alongside any refactoring that touches
`ForgeBlueprint`, the catalog Spec pieces, the wizard, or the executor/input_data
pipeline.** Last verified against branch `refactor` (2026-07-16), against live code —
not against `docs/dev-notes/forge-blueprint-inventory.md` (superseded, see `docs/developer-guide.md`).

## How to read this

Per the developer guide (`docs/developer-guide.md`), a value's life cycle is:

```
 ModelSpec + DomainSpec + ForcingSpec + OutputSpec   (catalog pieces, hand-edits)
              │  wizard UI (ForgeBlueprintWizard)
              ▼
      build_forge_blueprint()            resolver — forge_blueprint_resolve.py
              │
              ▼
       ForgeBlueprint (.yaml)             the reviewable, portable file
              │
              ▼
   ForgeExecutor.generate_inputs()        executor (generate) — input_data.py (roms-tools calls)
              │
              ▼
   ForgeExecutor.configure_build()        executor (configure) — overlays cfg.model_settings,
              │                            renders cppdefs.opt + namelist.nml
              ▼
   cppdefs.opt · namelist.nml · NetCDFs · roms_marbl blueprint (B_*.yaml)
```

Each table below covers one section of `ForgeBlueprint`. Columns:

1. **Piece(s)** — which catalog Spec (ModelSpec / DomainSpec / ForcingSpec / OutputSpec)
   contributes the *default* value, or "derived" / "per-run" if no catalog piece owns it.
   "multiple" is flagged explicitly with an explanation.
2. **Wizard UI** — where `ForgeBlueprintWizard` (`forge_blueprint_wizard.py`) lets a
   user set/change it. Most `model_settings` fields (§4-§6) share one answer: reachable
   only through the generic "Advanced settings" `_SettingsEditor` accordion, which
   auto-builds one widget per field for every section rather than offering bespoke
   per-section layouts — see the note after §4's table for the mechanism.
3. **Executor destination** — the roms-tools class, `cppdefs.opt`, `namelist.nml`
   group, or "not consumed" it feeds at processing time.
4. **Overridden?** — using this legend:
   - **(R)** resolver (`build_forge_blueprint`) force-derives/overwrites the
     ModelSpec value regardless of what's in `model.yaml`
   - **(O)** OutputSpec deep-merges over the ModelSpec default (resolve time, before overrides)
   - **(W)** a `run_time_overrides`/`compile_time_overrides` layer (wizard manual edit,
     applied last at resolve time — wins over R and O)
   - **(G)** `input_data.py` (`generate_inputs`, the executor's generate step) re-derives
     the value from the *live* roms-tools object, independent of what the resolver stored
   - **(E)** `ForgeExecutor.configure_build` (the executor's configure step) overlays the
     *stored* `ForgeBlueprint.model_settings` back on top of whatever `generate_inputs`
     just derived, via a
     **per-leaf-key deep merge** (`_deep_merge_settings_dict`, executor.py:126): for any
     section present in `model_settings`, every leaf key the resolver set wins over
     whatever `generate_inputs` just computed live — usually harmless, since the resolver
     and `generate_inputs` typically compute the identical value from the identical domain
     description (e.g. `param.llm`). The exception is the handful of leaf keys that are only ever
     meaningfully known post-generation (`river_frc.*`, `cdr_frc.{cdr_source,cdr_file,
     ncdr_parm,forcing_parameterized,cdr_volume}`, `cdr_output.do_cdr`,
     `tides.{ntides,bry_tides,pot_tides,ana_tides}`): `split_model_settings`
     (`GENERATION_DERIVED_LEAF_KEYS`, `forge_blueprint_engine.py`) excludes these from the
     overlay, so the live-generated value is what actually lands in the namelist. See the
     appendix at the end of this document for how that was found and fixed.

---

## 1. Identity, run window, working_dir

| Field | Piece(s) | Wizard UI | Executor destination | Overridden? |
|---|---|---|---|---|
| `identity.model_name` | ModelSpec (selects it) | `self.model_dd` Dropdown (`forge_blueprint_wizard.py`:1262), from `catalog.model_names` | `ForgeExecutor`/`RomsMarblInputData` constructor identity; feeds `name`/`casename` properties | — |
| `identity.grid_name` | **multiple**: DomainSpec's `Domain.yaml` has its own `grid_name` field (e.g. `"ccs-12km"`), which the wizard/resolver passes through as `identity.grid_name` | **two places**: `self.grid_name` Text (1274, manual) AND auto-overwritten by `_on_domain` (1820) when a catalog Domain is picked | feeds `name`/`casename`/output filenames | — |
| `identity.ensemble_id` | per-run (not cataloged) | `self.ensemble` Text, optional int (1432) | appended to `name` (`_{:03d}`) | — |
| `identity.description` | **multiple**: `ForgeBlueprint` default is `"Generated blueprint"`; DomainSpec's `Domain.yaml` also carries a `description` | `self.description` Text (1425) — a plain manual field, not auto-filled from the DomainSpec's own description on domain pick | descriptive only; not consumed by namelist/cppdefs | — |
| `run.start_date` / `run.end_date` | DomainSpec (`Domain.yaml` `start_time`/`end_time` prefill) — editable per-run | **two places**: `self.start`/`self.end` DatePickers (1407, 1413, manual) AND auto-set from a catalog Domain's `start_time`/`end_time` in `_on_domain` (1830) | drives `time_stepping.ntimes` (R, via CFL/duration calc); roms-tools `start_time`/`end_time`/`ini_time` kwargs (all forcing generators) | (R) `ntimes` is pure-derived from `(end-start).days` and `dt` |
| `run.model_reference_date` | per-run (default `2000-01-01`, the roms-tools default) | `self.model_ref_date` DatePicker (1419) | passed to every roms-tools constructor that accepts it (IC, Surface/Boundary/Tidal/CDR forcing); also `reference_date_settings` namelist group | — |
| `model_settings.time_stepping.dt` (authored via a `run`-context wizard control, though it lives in `model_settings`) | derived (CFL) — see §5 | **two entry paths**: `self.dt` FloatText (1473, manual) OR `self.dt_btn` "Compute dt (CFL)" (1480), which builds a real `roms_tools.Grid` and overwrites `self.dt.value` | `TimeStepping` namelist group | (R) — see §5 |
| `working_dir` | derived — host/location only | not exposed in the wizard at all; not even a `build_forge_blueprint` parameter | executor artifact root; excluded from `content_hash` | (R) sentinel `~/cstar-forge-run` expands to `<root>/<name>` on validation |
| `datasets` (top-level list) | derived from `forcing.resolved_datasets` | not directly editable (recomputed from forcing selections) | `forge_blueprint_to_builder_kwargs` → `source_dataset_keys=` → `ensure_source_data()` — drives *which* datasets `SourceData.prepare_all()` stages | (R) resolver recomputes fully from the resolved forcing/topography sources — never hand-authored |

---

## 2. Domain / grid geometry

| Field | Piece(s) | Wizard UI | Executor destination | Overridden? |
|---|---|---|---|---|
| `domain.grid_kwargs` (`nx, ny, size_x, size_y, center_lon, center_lat, rot, N, hc, theta_s, theta_b, verbose, hmin`) | DomainSpec (`Domain.yaml`) | **two places**: typed `IntText`/`FloatText` widgets in `self.grid_w` (1282-1298) + `self.hmin` (1442), `theta_s/theta_b/hc` gated by `self.scoord_chk` (1299) — AND catalog prefill via `domain_dd` → `_on_domain` (1812), which does not lock the fields (still hand-editable after). `verbose` is **not exposed** anywhere in the wizard. `close_narrow_channels`/`mask_shapefile` are separate widgets (`self.close_narrow_chk` 1449, `self.mask_shapefile` 1455) injected into `grid_kwargs` in `_gather()` (1986-1992) | `rt.Grid(**grid_kwargs)` directly in `ForgeExecutor.model_post_init` (executor.py ~L410-457); `hc`/`theta_s`/`theta_b` round-trip back out as the *generated* `s_coord` namelist group (read from `self.grid.hc/theta_b/theta_s`, not from the stored kwargs — see §7 processing-filled sections) | (G) `s_coord.{theta_s,theta_b,tcline}` is always re-read from the live `rt.Grid` object, not copied from `grid_kwargs` |
| `domain.topography_source` | **DomainSpec** (as of 2026-07-16) — a top-level `build_forge_blueprint(topography_source=...)` kwarg, threaded into `Domain(...)` and into `_build_forcing`'s `resolved_datasets` snapshot. Previously rode along inside a ForcingSpec's `Forcing.yaml` `grid.topography_source` block; that block has been removed from the one existing ForcingSpec | dedicated `self.topo_source` Dropdown (`forge_blueprint_wizard.py`, options `["ETOPO5","SRTM15"]`), laid out in the Grid section next to `topo_path`; prefilled from a catalog Domain pick via `_on_domain` | merged into `grid_kwargs["topography_source"]` as `{"name":…, "path":…}` before `rt.Grid()` construction (executor.py `_resolve_topography_source`, ~L346-403) | — |
| `domain.topography_path` | per-run override (explicit custom file) | `self.topo_path` Text (1463) | short-circuits `_resolve_topography_source` — used verbatim for any source, not just non-ETOPO5 | — |
| `domain.open_boundaries` (n/s/e/w bools) | DomainSpec (`Domain.yaml`) | **two places**: `self.bnd` dict of Checkboxes (1308) + catalog prefill via `_on_domain` (1825) | (1) `cppdefs.obc_{west,east,north,south}` → `cppdefs.opt.j2` `#define OBC_*`; (2) `self.boundaries` passed to `rt.BoundaryForcing(boundaries=...)` | (R) resolver sets `cppdefs.obc_*` from this at config time; **(G)** input_data.py sets the *same* keys again from `self.boundaries` at generation time (executor.py/input_data.py ~L696-699) — same value, redundant not conflicting |
| `domain.partitioning` (`n_procs_x`, `n_procs_y`) | DomainSpec (`Domain.yaml`) | **two places**: `self.npx`/`self.npy` IntText (1317, 1324) + catalog prefill via `_on_domain` (1827-1829) | `param.{np_xi,np_eta}` (namelist); `n_procs` property (`n_procs_x*n_procs_y`, used only for blueprint/output **naming**, e.g. `..._{n}procs` — confirmed **no** queue-selection or PEs-per-node logic in executor.py is keyed on partitioning; `prep_cstar_environment` resolves `account`/`queue`/`walltime` from machine config with precedence `explicit arg > env var > mc.account/mc.queues.get("default") > hardcoded default`, never reading `n_procs`/`pes_per_node` — MPI-rank/PEs-per-node reconciliation is delegated downstream to C-Star, not validated by Forge); `rt.partition_netcdf(np_eta=…, np_xi=…)` | (R) resolver sets `param.np_xi/np_eta` at config time; **(G)** input_data.py re-sets identically from `self.partitioning` at generation |
| `domain.grid_kwargs_parent` / `grid_kwargs_child` / `metadata_child` | per-run (nesting setup, not cataloged) | **two places** for the child grid kwargs: Nesting section widgets (`self.nest_enable` checkbox, `self.child_w` grid widgets 1362, `self.nest_period` 1379) + catalog prefill via `self.nest_domain_dd` → `_on_nest_domain` (1647) | `rt.Grid(...)` for parent/child; child metadata → `extract_data.*` + nesting-file kwargs | (R) child presence forces `extract_data.do_extract=True` + copies `N`/`theta_s`/`theta_b`/`hc` → `*_chd` fields; **(G)** input_data.py re-derives the same `extract_data.*` fields again from the live `self.grid_child` (input_data.py ~L709-721) |
| `domain.nesting_include_pressure_fluxes` | per-run | `self.nest_pressure_fluxes` checkbox (1386) | merged into nesting-writer kwargs as `include_pressure_fluxes` (only if MARBL/BGC enabled — see `has_marbl` gate, input_data.py ~L665-675) | — |

---

## 3. Forcing (initial conditions, surface, boundary, tidal, river, CDR)

All items ultimately become kwargs to a `roms_tools` constructor via
`RomsMarblInputData._build_input_args` (input_data.py ~L574-608), whose merge order is:
**typed item fields → `options` passthrough dict → hardcoded run-time injections
(`extra`: dates, `boundaries`, `use_dask`, `model_reference_date`)** — `extra` always
wins, `options` wins over typed defaults but loses to `extra`.

| Field group | Piece(s) | Wizard UI | Executor destination | Overridden? |
|---|---|---|---|---|
| `forcing.initial_conditions` (`source`, `bgc_source`, `bgc_interpolation_method`, `allow_flex_time`, `options`) | ForcingSpec (`Forcing.yaml` `initial_conditions:`) | Dedicated typed widgets in `_ForcingEditor` (`ic_name`, `ic_layout`, `ic_path`, `ic_bgc_name`, `ic_bgc_clim`, `ic_bgc_path`, `ic_bgc_interp`, `ic_flex_time`, ~L712-796) + a free-form JSON `ic_options` Textarea for the `options` passthrough | `rt.InitialConditions(grid=self.grid, **input_args)` → `initial.initial_file` (namelist `inifile`) | — |
| `forcing.surface[]` (`source`, `type`, `correct_radiation`, `coarse_grid_mode`, `restoring_forces`, `wind_dropoff`, `options`) | ForcingSpec (`Forcing.yaml` `forcing.surface:`) | Per-item row via `_make_row` (842) with dedicated typed widgets (`type`, `name`, `climatology`, `glorys_layout`, `correct_radiation`, `wind_dropoff`, `coarse_grid_mode`, `restoring_forces`) + per-item JSON `options` Textarea; add/remove via `_add`/`_remove` (1075/1080) | `rt.SurfaceForcing(grid=self.grid, **input_args)` → `forcing.surface_forcing_path` / `surface_forcing_bgc_path`; also derives `blk_frc.interp_frc` or `bgc.interp_frc` from `frc.use_coarse_grid` | (R)+(G) **double-derived, same logic twice**: resolver computes `cppdefs.co2_tvarying`/`cppdefs.sal_restore` from the item list at config time (forge_blueprint_resolve.py ~L305-316); input_data.py re-derives the identical flags at generation time from which items actually got built (input_data.py ~L845-851) |
| `forcing.boundary[]` (`source`, `type`, `bgc_interpolation_method`, `prefill`, `prefill_kwargs`, `regrid_method`, `extrap_method`, `extrap_kwargs`, `options`) | ForcingSpec (`Forcing.yaml` `forcing.boundary:`) | Per-item row, typed widgets for `bgc_interpolation_method`, `prefill`, `regrid_method`, `extrap_method` (+ shared `type`/`name` etc.) + JSON `options` Textarea; `prefill_kwargs`/`extrap_kwargs` are **not** individually widgeted — only reachable via `options` | `rt.BoundaryForcing(grid=self.grid, **input_args)` → `forcing.boundary_forcing_path`/`boundary_forcing_bgc_path`; when `bgc_interpolation_method` is `density`/`density_mld`, a companion physics `BoundaryForcing` is built first and passed as `physics_forcing=` (input_data.py `_build_physics_boundary_companion`, ~L919-949) | if density-space BGC interpolation is requested but no physics boundary item exists, silently falls back to depth-space interpolation (warns only) |
| `forcing.tidal[]` (`source`, `ntides`, `options`) | ForcingSpec (`Forcing.yaml` `forcing.tidal:`) | Per-item row, `ntides` typed widget + JSON `options` | `rt.TidalForcing(grid=self.grid, **input_args)` → `forcing.tidal_forcing_path`; `tides.ntides` | (R) resolver sets `tides.ntides` from the item's *declared* `ntides` field, **if set** (else stays at the ModelSpec placeholder); **(G)** input_data.py *unconditionally* overwrites `tides.{ntides,bry_tides,pot_tides,ana_tides}` at generation from the *actual* generated `rt.TidalForcing.ntides`. These 4 leaf keys are excluded from `configure_build`'s overlay (`GENERATION_DERIVED_LEAF_KEYS`), so the actually-generated constituent count is what lands in the namelist, not the merely-declared one |
| `forcing.river[]` (`source`, `include_bgc`, `convert_to_climatology`, `bgc_source`, `coast_snap_buffer_km`, `domain_edge_buffer`, `options`) | ForcingSpec (`Forcing.yaml` `forcing.river:`) | Per-item row, typed widgets for `climatology`, `include_bgc`, `convert_to_climatology`, `coast_snap_buffer_km`, `domain_edge_buffer` + JSON `options`; `bgc_source` (a nested dict, not a `SourceSpec`) has **no dedicated widget** — reachable only via `options` | `rt.RiverForcing(grid=self.grid, **input_args)` → `forcing.river_path`; `river_frc.{river_source,analytical,nriv,rvol_vname,rvol_tname,rtrc_vname,rtrc_tname}` | (G) all of `river_frc`'s generation-relevant fields are set at generation time from the actual `rt.RiverForcing` dataset; these leaves are excluded from `configure_build`'s overlay (`GENERATION_DERIVED_LEAF_KEYS`), so the generated values (not the ModelSpec's disabled placeholder) land in the namelist |
| `forcing.cdr_forcing` (raw `dict`, e.g. `start_time`/`end_time`/`model_reference_date`/`releases[]` with `name,lat,lon,depth,hsc,vsc,times,release_type,tracer_fluxes`) | **per-run only** — not part of any ForcingSpec catalog piece (`Forcing.yaml` never has a `cdr_forcing` key; it's a separate `build_forge_blueprint(cdr_forcing=...)` argument) | **not exposed anywhere in the wizard** — confirmed absent from `_gather()`'s kwargs; must be hand-set in the YAML or via a direct `build_forge_blueprint(cdr_forcing=...)` call | `rt.CDRForcing(**input_args)` → `cdr_frc.{cdr_file,cdr_source,ncdr_parm,forcing_parameterized,cdr_volume}`, `cdr_output.do_cdr` | **untyped**: `dict[str, Any]`, no Pydantic validation at the `ForgeBlueprint` level — validated only implicitly by the `rt.CDRForcing` constructor at generation time. `cppdefs.cdr_forcing` is set correctly at both (R) and (G) (see §4); `cdr_frc.*`/`cdr_output.do_cdr`'s generation-derived leaves are likewise excluded from `configure_build`'s overlay, so the generated values land in the namelist |
| `forcing.resolved_datasets` | derived (snapshot of `source_registry` at resolve time) | not directly editable | **authoritative as of 2026-07-16** — threaded into `ForgeExecutor.resolved_datasets` (`forge_blueprint_engine.py`) and into `SourceData(resolved_datasets=...)`; `SourceData.dataset_key_for_source`/`streamable_for_source` now prefer this snapshot over a live `source_registry` lookup (which remains the fallback for names absent from the snapshot, e.g. GLORYS-with-explicit-layout, which is always resolved live since the snapshot is keyed by logical name only). This is what makes a blueprint resolve identically on any host/forge-version, even if `source_registry`'s tables later change | (R) recomputed by the resolver on every build; not writable by a user |

---

## 4. Compile-time settings (`cppdefs`)

`cppdefs.opt.j2` (`templates/compile-time/cppdefs.opt.j2`) is almost entirely static
`#define`/`#undef` literals — **only 7 of the fields under `model_settings.cppdefs` (+ 1
cross-section read of `cdr_frc.cdr_source`, + 1 of `upscale_output.do_upscale`) are
actually read by the template.** Everything else in the file is a hardcoded flag with
no YAML/blueprint knob.

| Field | Piece(s) | Wizard UI | Executor destination | Overridden? |
|---|---|---|---|---|
| `cppdefs.obc_{west,east,north,south}` | derived from `domain.open_boundaries` (DomainSpec) | technically reachable in the generic Advanced-settings `cppdefs` pane (see below), but pointless to hand-edit — always overwritten by (R)/(G) | `cppdefs.opt.j2` → `#define OBC_{WEST,EAST,NORTH,SOUTH}` | (R) + (G), see §2 open_boundaries row — always overwritten, never a real ModelSpec default despite `model.yaml` carrying placeholder `true` values |
| `cppdefs.marbl` | derived (as of 2026-07-16) from a new per-run `bgc_mode: Literal["marbl","none"]="marbl"` kwarg, mirroring `use_pio` exactly. ModelSpec's `model.yaml` keeps `marbl: true` as an inert, always-overwritten placeholder | dedicated `self.bgc_dd` Dropdown (`forge_blueprint_wizard.py`, options `["marbl","none"]`, default `"marbl"`), placed in the Pieces section next to `model_dd`; excluded from the generic Advanced-settings accordion (see §7 accordion note) | **NOT read by `cppdefs.opt.j2`** — `MARBL`/`MARBL_DIAGS` are unconditional `#define`s in the template regardless of this value. It IS read at `input_data.py:875` to gate whether the run-time `bgc` section gets touched at all (`has_bgc_compile`) | (R) resolver sets `cppdefs.marbl = (bgc_mode=="marbl")` and gates `code.marbl` the same way `use_pio` gates `code.pio` (raises if `bgc_mode="marbl"` but the ModelSpec has no `code.marbl`); resolver also raises if `bgc_mode="none"` but the resolved forcing still requests BGC forcing (bgc-type surface/boundary item, IC `bgc_source`, or river `include_bgc=True`) — see the Appendix. `marbl_from_model_settings()` (formerly dead code) has been deleted |
| `cppdefs.cdr_forcing` | derived (`cdr_forcing is not None`) | not directly editable | `cppdefs.opt.j2` → `#define CDR_FORCING` (template condition is actually `cppdefs.cdr_forcing OR cdr_frc.cdr_source` — a cross-section read) | (R) at config time from whether a CDR dict was supplied; **(G)** unconditionally re-set `True` at generation once CDR forcing actually builds |
| `cppdefs.co2_tvarying` | derived from `forcing.surface[]` (any `type=bgc`+`source=MBL_co2`) | not directly editable | `cppdefs.opt.j2` → `#define PCO2AIR_FORCING` | (R) at config time; **(G)** re-derived identically at generation |
| `cppdefs.sal_restore` | derived from `forcing.surface[]` (any `type=restoring` with `"sss"` in `restoring_forces`) | not directly editable | `cppdefs.opt.j2` → `#define SFLX_CORR` | (R) at config time; **(G)** re-derived identically at generation |
| `cppdefs.use_pio` | per-run (`build_forge_blueprint(use_pio=...)`) | **two places**: a dedicated `self.use_pio_chk` Checkbox (`forge_blueprint_wizard.py`:1331, wired to the `use_pio` resolver kwarg in `_gather()`:2010) AND the same field is *also* editable generically in the Advanced-settings `cppdefs` pane | `cppdefs.opt.j2` → `#define PARALLEL_IO`. `_use_pio` (executor.py:631-636) reads this flag and gates exactly two things: the `netcdf_format` passed to `RomsMarblInputData` in `generate_inputs` (`"NETCDF3_64BIT_DATA"` vs `"NETCDF4"`, ~L1485) and whether `model_params["use_pio"]=True` is added in `configure_build` (~L1937-1938). **It does NOT gate whether `code.pio` is included in the blueprint** — see §7: that's unconditional on `code_spec.pio` being set at all | (R) forces `False` unless explicitly passed `True`; if `True` and ModelSpec has no `code.pio`, resolver raises `ValueError` |
| every other `cppdefs.opt.j2` `#define`/`#undef` (`EXACT_RESTARTS`, `NONLIN_EOS`, `SALINITY`, `SOLVE3D`, `SPLIT_EOS`, `TIDES`, `UV_ADV`, `UV_COR`, `CURVGRID`, `MASKING`, `SPHERICAL`, `SPONGE`, `BULK_FRC`, `LMD_*`, `TS_DIF2`, `UV_VIS2`, `M2_FRC_BRY`, `M3_FRC_BRY`, `OBC_M2FLATHER`, `OBC_M3ORLANSKI`, `OBC_TORLANSKI`, `T_FRC_BRY`, `Z_FRC_BRY`, `NHY_FORCING`, `NOX_FORCING`, and ~25 `#undef`-only options) | none — hardcoded template literals | **nowhere** — no blueprint field exists for these | not templated at all; a code change to `cppdefs.opt.j2` is required | n/a |
| `upscale_output.do_upscale` (cross-referenced by `cppdefs.opt.j2` for `#define UPSCALING`) | OutputSpec (see §5) | see §6 | `cppdefs.opt.j2` reads `upscale_output.do_upscale` even though the *section* is a run-time/output setting — this is a compile-time flag with an output-settings home | this is the one field where the OutputSpec piece leaks into compile-time codegen |

**Wizard mechanics for `model_settings` (applies to every section in §4/§5/§6 below,
confirmed by the wizard research pass):** there is no dedicated per-section widget
layout and no raw-YAML/JSON editor for the whole dict. Instead, a single generic
component, `_SettingsEditor` (class at `forge_blueprint_wizard.py`:537, instantiated in
`_rebuild()`:2073 as `self.editor`), introspects **every** section the resolver returns
(all ~40 keys, including every row below) and auto-builds one widget per field —
type inferred from the `RunTimeSettings` sub-model annotation where one exists
(`_section_submodel`, 481) or from the Python value type otherwise (`_base_type`, 392;
this is how scalar sections like `gamma2`/`ubind` get a widget despite having no
dedicated `RunTimeSettings` sub-model). This lives under the "Advanced settings"
accordion (wired in `widget()`, ~L2429). So **every field below is wizard-reachable,
generically, not via a bespoke widget** — the table only calls out the rare exceptions
(`use_pio` above being the one field with both a dedicated widget AND the generic one).
Manual edits are captured as a sparse override layer: `_on_editor_edit` (1640) records
`self._overrides[(section, field)]`, applied via `_apply_overrides` (492) and surfaced
in `composition.overrides` (§7) via `_overrides_nested` (506).

---

## 5. Run-time physics & numerics (ModelSpec-owned)

One row per section — individual fields diverge only where noted.

| Section | Piece(s) | Wizard UI | Executor destination | Overridden? |
|---|---|---|---|---|
| `lateral_visc` (`visc2`, `rho0`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `lateral_visc_settings` namelist group; `rho0` is cross-section-read into `rho0_settings` separately (`build_namelist`) | — |
| `vertical_mixing` (`akv`, `akt_default`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `vertical_mixing_settings`; `akt_default` scalar → expanded to a per-tracer array (`akt_bak = [akt_default] * n_tracers`) in `build_namelist` | (structural, not an override — a scalar→array expansion) |
| `tracer_diff2` (`tnu2_default`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `tracer_diff2` namelist group; scalar → `tnu2 = [tnu2_default] * n_tracers` | same structural expansion |
| `bottom_drag` (`rdrg`, `rdrg2`, `zob`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `bottom_drag_settings` | — |
| `gamma2` (bare scalar) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `gamma2_settings` | — |
| `ubind` (bare scalar) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `ubind_settings` | — |
| `param` (`llm`, `mmm`, `n`, `np_xi`, `np_eta`, `nt_passive`, `ntrc_bio`; resolver also injects `nsub_x`, `nsub_e`) | **splits field-by-field**: `llm`/`mmm`/`n` ← DomainSpec grid; `np_xi`/`np_eta` ← DomainSpec partitioning; `nt_passive`/`ntrc_bio` ← ModelSpec (real defaults, not overwritten); `nsub_x`/`nsub_e` ← hardcoded `1`/`1` in the resolver, never read from ModelSpec at all | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `param_settings` namelist group (`nz`, `nt_bgc` aliases); `n_tracers` property computed as `2 + ntrc_bio + nt_passive` | (R)+(G): `llm,mmm,n,np_xi,np_eta` overwritten twice (config-time from `grid_kwargs`/`partitioning`, generation-time from the live `rt.Grid`/partitioning) — see §2; `nt_passive`/`ntrc_bio` are the only two `param` fields that are genuine, un-overridden ModelSpec defaults |
| `v_sponge` (`v_sponge`) | derived — no catalog piece | not exposed (pure-derived) | `v_sponge_settings` | (R) `= (size_x/nx)*1000/10`, computed fresh every resolve; a stored value is never read back from ModelSpec |
| `time_stepping` (`ntimes`, `dt`, `ndtfast`, `ninfo`) | `dt`/`ntimes` derived (CFL + run window); `ndtfast=60`, `ninfo=1` hardcoded in the resolver (ModelSpec's own `model.yaml` doesn't define this section) | `dt` has its own dedicated widgets (§1: `self.dt` manual FloatText + `self.dt_btn` CFL-compute button); `ntimes`/`ndtfast`/`ninfo` only reachable via the generic Advanced-settings editor | `TimeStepping` namelist group | (R) `ntimes`/`dt` always computed, never taken from ModelSpec; `ndtfast`/`ninfo` are resolver-level hardcoded literals, not overridable via any Spec (only via `run_time_overrides`) |
| `tides` (`bry_tides`, `pot_tides`, `ana_tides`) non-`ntides` fields | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `tidal_frc_settings` | **(G)** force-set to `True,True,False` at generation (input_data.py ~L1129-1131); these 3 fields plus `ntides` are excluded from `configure_build`'s overlay (`GENERATION_DERIVED_LEAF_KEYS`), so the generated values land in the namelist |
| `river_frc` (`river_source`, `analytical`, `nriv`, `rvol_vname`, `rvol_tname`, `rtrc_vname`, `rtrc_tname`) | ModelSpec (all-disabled placeholder) | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `river_frc_settings` | (G) set correctly at generation once any river item is configured; excluded from `configure_build`'s overlay (`GENERATION_DERIVED_LEAF_KEYS`), so the generated values land in the namelist |
| `blk_frc` (`interp_frc`, `check_bulk_frc_units`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `surf_frc_settings` (merged with `flux_frc`) | **(G)** `interp_frc` derived from `frc.use_coarse_grid` at generation; `check_bulk_frc_units` stays a real ModelSpec default |
| `flux_frc` (`interp_flux_frc`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `surf_frc_settings` | — |
| `bgc` (`interp_frc`, `nbgc_flx`, `xco2air_default` — physics) + (`wrt_his`, `output_period_his`, `nrpf_his`, `wrt_avg`, `output_period_avg`, `nrpf_avg`, `wrt_his_dia`, `output_period_his_dia`, `nrpf_his_dia`, `wrt_avg_dia`, `output_period_avg_dia`, `nrpf_avg_dia` — output write-control) | **split as of 2026-07-16**: physics fields are ModelSpec; the 12 write-control fields are now **OutputSpec** (`Output.yaml`'s own `bgc:` block), deep-merged over the ModelSpec's 3-field `bgc` section at resolve time (`OUTPUT_BGC_FIELDS`/`PARTIAL_OUTPUT_SECTIONS`, mirroring how `marbl_bgc` already splits) | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table); the accordion is agnostic to which piece a field came from, so this split is invisible in the UI beyond the OutputSpec-swap override-clearing behavior (`_on_output_spec` now keys off `PARTIAL_OUTPUT_SECTIONS`, not a marbl_bgc-only special case) | `bgc_settings` namelist group | **(G)** `interp_frc` derived like `blk_frc.interp_frc`, only touched at all `if has_bgc_compile` (gated on `cppdefs.marbl`); (O) the 12 write-control fields now correctly belong to, and are cleared/reseeded with, the OutputSpec selection — no longer the naming trap this used to be |
| `cdr_frc` (`cdr_source`, `cdr_file`, `ncdr_parm`, `forcing_depth_profiles`, `forcing_3d`, `forcing_parameterized`, `time_interpolation`, `relocate_to_wet_pts`, `cdr_volume`,+ vnames,`nz_chd`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `cdr_frc_settings` | (G) `cdr_source`, `cdr_file`, `ncdr_parm`, `forcing_parameterized`, `cdr_volume` set correctly at generation once CDR builds — excluded from `configure_build`'s overlay (`GENERATION_DERIVED_LEAF_KEYS`), so the generated values land in the namelist; `forcing_depth_profiles`, `forcing_3d`, `time_interpolation`, `relocate_to_wet_pts`, vnames, `nz_chd` are never touched by (G) at all — genuine ModelSpec defaults, still overlaid normally |
| `extract_data` (`do_extract`, `extract_file`, `nrpf`, `n_chd`, `theta_s_chd`, `theta_b_chd`, `hc_chd`, `extract_period`) | ModelSpec default (`do_extract:false`); overwritten wholesale when a child grid exists | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `extract_data_settings` | (R)+(G) double-derived when nesting is configured, see §2; `nrpf` is the only field never overwritten (stays ModelSpec) |
| `sponge_tune` (`ub_tune`, `spn_avg`, `sp_timscale`, `wrt_sponge`, `nrpf`, `output_period`,+ `pflx_*_vname`/`tname`) | ModelSpec — **not** part of OutputSpec despite looking like output config (`wrt_sponge`, `output_period`) | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `sponge_tune_settings` | — |
| `calc_pflx` (`calc_pflx`, `timescale`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `calc_pflx_settings` | — |
| `pipe_frc` (`pipe_source`, `p_analytical`, `npip`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `pipe_frc_settings` | — |
| `particles` (`floats`, `np`, `extra_space_fac`, `exchange_facx/y/c`, `output_period`, `nrpf`, `ppm3`, `pmin`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `particles_settings` | — |
| `lin_rho_eos` (`tcoef`, `t0`, `scoef`, `s0`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `lin_rho_eos_settings` | — |
| `sss_correction` (`dsssdt`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `sss_correction` group | — |
| `sst_correction` (`dsstdt`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `sst_correction` group | — |
| `dic_alk_correction` (`dcdt`) | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `dic_alk_correction` group | — |
| `marbl_bgc.marbl_config_file` / `marbl_timestep` | ModelSpec | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `marbl_biogeochemistry_settings` (config file is copied as a static run-time template file, `marbl_in`) | — |

---

## 6. Output settings (OutputSpec-owned)

These are exactly the sections in `forge_blueprint_resolve.OUTPUT_SECTIONS`, plus the
partial sections in `PARTIAL_OUTPUT_SECTIONS` (`marbl_bgc`'s two write-lists, and — as
of 2026-07-16 — `bgc`'s 12 output-write-control fields) — deep-merged over the
ModelSpec defaults at resolve time (`_deep_merge(settings, output_settings)`,
forge_blueprint_resolve.py ~L358), **before** `run_time_overrides`/`compile_time_overrides`
are applied (so a wizard hand-edit still wins over the OutputSpec selection).

**Invariant introduced 2026-07-16**: since `ocean_vars`/`surf_flux`/`diagnostics` are
already fully OutputSpec-owned (no ModelSpec fallback), and the `bgc`/`marbl_bgc`
write-control fields now join them (partially), **every OutputSpec must supply the
full `PARTIAL_OUTPUT_SECTIONS` field set** (all 12 `bgc.*` fields, both `marbl_bgc.*`
write-lists) or `BgcCfg`/namelist validation will fail downstream with no ModelSpec
fallback to catch the gap. The one shipped `OutputSpec` (`Output.yaml`, "standard")
carries the full set; this is a requirement to check when authoring a new OutputSpec.

| Section | Piece(s) | Wizard UI | Executor destination | Overridden? |
|---|---|---|---|---|
| `ocean_vars` (~33 `wrt_*`/`output_period_*`/`nrpf_*` restart/history/average write flags) | **OutputSpec** (`Output.yaml`) | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `basic_output_settings` namelist group | (O) deep-merged over ModelSpec (which has none of these — `model.yaml` doesn't define `ocean_vars` at all, it's 100% OutputSpec-sourced) |
| `surf_flux` (`wrt_smflx`, `wrt_stflx`, `wrt_rstflx`, `wrt_swflx`, `sflx_avg`, `output_period`, `nrpf`,+ `sst_vname`/`tname`, `sss_vname`/`tname`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `surf_flx_output_settings` | (O) |
| `diagnostics` (`diag_avg`, `output_period`, `nrpf`, `diag_uv`, `diag_trc`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `diagnostics_settings` | (O) |
| `stdout_diag` (`code_check_mode`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `stdout_diag_settings` | (O) |
| `ts_output` (`wrt_temp`, `wrt_salt`, `wrt_temp_dia`, `wrt_salt_dia`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `ts_output_settings` | (O) |
| `frc_output` (`wrt_frc`, `wrt_frc_avg`, `output_period`, `nrpf`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `frc_output_settings` | (O) |
| `cdr_output` (`do_cdr`, `do_avg`, `monthly_averages`, `output_period`, `nrpf`) | **OutputSpec** default (`do_cdr:false`) | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `cdr_output_settings`; `do_upscale` cross-read by `cppdefs.opt.j2` (see §4) | (O) then (G) sets `do_cdr=True` once CDR forcing actually builds (input_data.py ~L1319); `do_cdr` is excluded from `configure_build`'s overlay (`GENERATION_DERIVED_LEAF_KEYS`), so the generated value lands in the namelist |
| `upscale_output` (`do_upscale`, `nrpf_uscl`, `output_period_uscl`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `upscale_settings`; `do_upscale` also feeds `cppdefs.opt.j2`'s `#define UPSCALING` (§4) | (O) |
| `zslice` (`do_zslice`, `zslice_avg`, `wrt_t_zsl`, `wrt_u_zsl`, `wrt_v_zsl`, `output_period`, `nrpf`, `ndep`, `vecdep`, `nt_z`, `trc2zsc`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `zslice_settings` | (O) |
| `random_output` (`do_random`, `output_period`, `nrpf`) | **OutputSpec** | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `random_output_settings` | (O) |
| `marbl_bgc.{marbl_tracers_to_write,marbl_diagnostics_to_write}` | **OutputSpec** (`OUTPUT_MARBL_FIELDS`) — **spans two pieces**: the rest of `marbl_bgc` (`marbl_config_file`, `marbl_timestep`) is ModelSpec (§5) | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `marbl_biogeochemistry_settings` write-lists | (O) merged onto the *same* `marbl_bgc` dict that ModelSpec partially populates |
| `bgc.{wrt_his,output_period_his,nrpf_his,wrt_avg,output_period_avg,nrpf_avg,wrt_his_dia,output_period_his_dia,nrpf_his_dia,wrt_avg_dia,output_period_avg_dia,nrpf_avg_dia}` | **OutputSpec** (`OUTPUT_BGC_FIELDS`, as of 2026-07-16) — **spans two pieces**, same pattern as `marbl_bgc`: the rest of `bgc` (`nbgc_flx`, `interp_frc`, `xco2air_default`) is ModelSpec (§5) | Advanced-settings accordion — generic `_SettingsEditor` (auto one-widget-per-field; see note after §4's table) | `bgc_settings` namelist group | (O) merged onto the *same* `bgc` dict that ModelSpec partially populates, via the generalized `PARTIAL_OUTPUT_SECTIONS` mechanism (which also now drives `_on_output_spec`'s override-clearing, closing a latent gap where a `bgc.*` override would have survived an OutputSpec swap) |

---

## 7. Code / templates / composition / provenance

| Field | Piece(s) | Wizard UI | Executor destination | Overridden? |
|---|---|---|---|---|
| `code.roms` (`location`, `commit`) | ModelSpec | `self.roms_ref` Text (`forge_blueprint_wizard.py`:1338, prefilled per-model from `model.yaml` via `_model_default_roms_ref()`:1662), overrides `commit`/`branch` only — `location` itself is **not exposed** | `_cstar_code_repository()` (executor.py:985-1017) builds a `cstar_models.ROMSCompositeCodeRepository` from `self.code_spec`; consumed once at PRECONFIG (`_initialize_roms_marbl_blueprint`) | (R) if `roms_ref` passed, replaces ModelSpec's pinned commit/branch |
| `code.marbl` (`location`, `commit`) | ModelSpec (repo pin) — **presence now gated per-run** (as of 2026-07-16) by `bgc_mode`, the same mechanism as `code.pio`/`use_pio` | only indirectly gated by `self.bgc_dd` ("marbl"/"none"); the repo location/commit itself is **not editable** in the wizard — mirrors `code.pio`/`use_pio_chk` exactly | same `_cstar_code_repository()` path — `marbl=_repo(...) if code_spec.marbl else None` (needed no change: it already no-ops on `None`) | (R) resolver includes `code.marbl` iff `bgc_mode=="marbl"`, raising if the ModelSpec has no `code.marbl` block; omits it (sets `None`) iff `bgc_mode=="none"` |
| `code.pio` (`location`, `commit`) | ModelSpec | only indirectly gated by `self.use_pio_chk` (on/off); the repo location/commit itself is **not editable** in the wizard | same `_cstar_code_repository()` path. `code.pio`'s presence in the blueprint IS conditional on `use_pio` (gated in `_build_code`, not in the executor) — `_use_pio` (executor.py, reads `cppdefs.use_pio`) separately gates two unrelated things: `netcdf_format` passed to `RomsMarblInputData` in `generate_inputs` (`"NETCDF3_64BIT_DATA"` vs `"NETCDF4"`) and whether `model_params["use_pio"]=True` is added in `configure_build` | (R) resolver raises if `use_pio=True` and ModelSpec has no `code.pio` |
| `code.templates_compile_time` / `templates_run_time` (`directory`, `files`) | ModelSpec (`model.yaml` `code.templates_compile_time/_run_time`, decoupled repo = forge's own `templates/` dir) | not exposed (infra-level); a `templates_repo` kwarg exists on `build_forge_blueprint`'s signature but the wizard's `_gather()` never passes it | `_template_repo_args(stage)` (executor.py:1019-1034) feeds C-Star's `AdditionalCode` constructor; `_stage_templates(stage)` (~1036-1060) materializes the filtered files under `host.working_dir/templates/{stage}` — **no CI coverage for the cross-repo flat-staging contract** per developer-guide §6 item 2. `configure_build` later overwrites `roms_marbl_blueprint.code.compile_time`/`code.run_time` with the *rendered* locations (~1919-1932), replacing the PRECONFIG placeholders | (R) `templates_commit:` pin in ModelSpec overrides tracking branch `main` |
| `composition.model` | derived | always `PieceRef(name=model_dd.value, origin="catalog")` from `_composition()`; `modified` is computed afterward in `_rebuild()` **(fixed 2026-07-16)** | not consumed by processing — pure UI/review metadata | n/a; `modified=True` iff any deviating key in `_diff_overrides(effective, composed)` is *not* output-owned (`_is_output_key`) |
| `composition.domain` | derived | `PieceRef(name=grid_name, origin="custom")` if `domain_dd == "<custom>"`, else `origin="catalog"` (`_composition()`); `modified` computed in `_rebuild()` **(fixed 2026-07-16)** | not consumed by processing | n/a; `modified=True` iff a catalog Domain is selected *and* the current `_domain_snapshot()` (grid_w/bnd/npx/npy/grid_name/topo_*) differs from the snapshot captured at the moment of that pick (`_domain_seed`, set in `_on_domain`) — edit-then-revert clears it |
| `composition.forcing` | derived | `PieceRef(name=forcing_dd.value, origin="catalog")`; `modified` computed in `_rebuild()` by comparing `_forcing_editor.gather()` to a snapshot taken at the last catalog pick (`_forcing_seed`, set in `_on_forcing_spec`) **(unified 2026-07-16 — origin no longer flips to `"custom"` on edit)** | not consumed by processing | n/a |
| `composition.output` | derived | always `PieceRef(name=output_dd.value, origin="catalog")`; `modified` computed in `_rebuild()` **(fixed 2026-07-16)** | not consumed by processing | n/a; `modified=True` iff any deviating key in `_diff_overrides(effective, composed)` *is* output-owned (`OUTPUT_SECTIONS`/`PARTIAL_OUTPUT_SECTIONS`, via `_is_output_key`) |
| `composition.overrides` | derived — sparse `{section:{field:value}}` layer | populated from `_on_editor_edit`'s override map (see note after §4's table), applied in `_rebuild()` via `_overrides_nested` (1640/2086-2089/506) | not consumed by processing — mirrors `run_time_overrides`/`compile_time_overrides` for review purposes only | n/a |
| `provenance.*` (`generated_at`, `forge_version`, `roms_tools_version`, `override_files_applied`, `content_hash`, `notes`) | derived; `generated_at`/`forge_version`/`roms_tools_version`/`notes` are real `build_forge_blueprint` kwargs | **none appear in the wizard's `_gather()`** — the wizard never supplies them, so they resolve to `None`/defaults; `override_files_applied` is hardcoded to `[]` by the resolver | `content_hash` is verified (warn-only) by `verify_content_hash` at the start of `process_forge_blueprint`; excluded from its own hash | n/a |

**Asymmetry — fixed 2026-07-16**: previously only `composition.forcing`'s
`modified` flag was ever flipped to `True` after a catalog pick was hand-edited;
`composition.{model,domain,output}` never got `modified=True` regardless of user
edits. All four pieces now compute `modified` centrally in `_rebuild()` with
"deviate" semantics (see rows above), and the `origin` convention is unified:
every piece keeps `origin="catalog"` + `modified=True` on edit (forcing no
longer flips to `origin="custom"`).

---

## 8. Downstream `roms_marbl` blueprint — which fields land where

**Update (2026-07-16): the "stages" concept has been removed.** The executor no
longer models PRECONFIG/POSTCONFIG/BUILD/RUN as a persisted state machine —
`generate_inputs`/`configure_build` mutate an in-memory `RomsMarblBlueprint`
(a *different* blueprint from `ForgeBlueprint` — see `docs/developer-guide.md`
terminology note), and `ForgeExecutor.persist()` writes it to disk exactly
once, at the end of `configure_build()`, as a single `B_{name}.yaml` (+
`settings_B_{name}.yaml` sidecar holding `_settings_compile_time`/
`_settings_run_time`). The table below now describes build *steps*, not
stored/persisted stages — only the last row's output is ever written to disk.

| Step | Method | Fields populated |
|---|---|---|
| **Initialize** | `_initialize_roms_marbl_blueprint` | `name`, `description`, `valid_start_date`/`end_date`, `partitioning`, `code` (roms/marbl/pio repos + placeholder run_time/compile_time locations), placeholder `grid`/`initial_conditions`/`forcing`, `cdr_forcing=None`; `model_params=None`, `runtime_params=None`. In-memory only. |
| **Generate inputs** | `generate_inputs` | `grid`, `initial_conditions`, `forcing`, `cdr_forcing`, `nesting_info` populated with real file `Resource` locations; `model_params`/`runtime_params` still `None` (settings live only in the in-memory settings dicts at this point). In-memory only. |
| **Configure build** | `configure_build` | `code.compile_time`/`code.run_time` replaced with rendered file locations; `model_params={"time_step": dt, "use_pio": True (iff enabled)}`; `runtime_params={start_date,end_date,output_dir=run_output_dir}`; `working_dir=run_output_dir`. **Persists `B_{name}.yaml` + sidecar — the only write.** |
| **Run** | `run()` | no new blueprint fields — calls `prep_cstar_environment()`, hands `B_{name}.yaml`'s path to `RomsMarblRunner`; does not re-persist |

---

## 8a. Namelist sections with no `ForgeBlueprint` home at all

These are namelist groups `write_roms_namelist` fills, but which are never stored in
`ForgeBlueprint.model_settings` — `_PROCESSING_FILLED_SECTIONS`
(`forge_blueprint_resolve.py`) deliberately omits them because they're host-, artifact-,
or identity-derived, not a reviewable input. To make "every parameter" literally
complete, they get their own row-set (piece = none; wizard = nowhere; overridden = n/a
— there is no stored value to override):

| Namelist group | Where the value comes from | Executor destination |
|---|---|---|
| `grid.grid_file` | the generated grid NetCDF path (`input_data.py` `_generate_grid`) | `GridSettings` (`grdname`) |
| `initial.initial_file` | the generated IC NetCDF path (`input_data.py` `_generate_initial_conditions`) | `InitialConditions` (`inifile`) |
| `s_coord.{theta_s,theta_b,tcline}` | read back from the live `rt.Grid` object (`self.grid.theta_s/theta_b/hc`) — round-trips `domain.grid_kwargs`' own `theta_s`/`theta_b`/`hc`, see §2 | `SCoord` |
| `title.casename` | `ForgeBlueprint.casename` property (`f"{name}_{datestr}"`, itself derived from `identity`+`run`+`partitioning`) | `SimulationNameSettings.title` |
| `output_root_name.output_root_name` | `run_output_dir(scratch)/output/casename`, where `scratch` comes from the injected `HostPaths` (§8: only resolved on the run host) | `SimulationNameSettings.output_root_name` |
| `forcing.{surface,boundary,tidal,river}_forcing[_bgc]_path` | the generated forcing NetCDF paths (`input_data.py`, per forcing category, §3) | `ForcingFiles.frcfiles` |
| `reference_date_settings.reference_date` | `[year,month,day]` from `run.model_reference_date` (stored, §1) — the one field in this table that *does* trace back to a stored `ForgeBlueprint` value, just regrouped/reformatted, not re-derived | `ReferenceDateSettings` |

One hostname-conditioned branch exists: `prep_cstar_environment` special-cases
`host.system == "RCAC_anvil"` (symlinks the venv's `cstar` executable into `cwd` and
prepends it to `PATH`, working around Anvil's `cstar` resolution) — the only
machine-specific code path in `executor.py` itself.

---

## 9. Known dead / orphaned code found during this audit

- ~~`forge_blueprint_resolve.marbl_from_model_settings()` is defined but has zero call
  sites~~ — **deleted 2026-07-16** as part of adding the `bgc_mode` selector;
  `input_data.py:875`'s `has_bgc_compile` still inlines the same one-line dict-get
  directly (repurposing the dead function would have added a new cross-module import
  for no dedup benefit).
- `docs/reference-settings-run-time.md` / `reference-settings-compile-time.md` MyST
  pages `{include}` files (`templates/run-time-defaults.yaml`,
  `templates/compile-time-defaults.yaml`) that no longer exist post-consolidation —
  stale, will break the docs build if it ever re-renders these pages.

---

## Follow-ups / remaining gaps

- [x] Wizard-UI field mapping — done (see per-row citations throughout).
- [x] Executor override precedence, PIO gating, code/template consumption,
  partitioning consumption, blueprint-stage mapping — done (§7, §8).
- [x] `configure_build`'s overlay clobbering generated river/CDR/tidal values back to
  resolver-time placeholders — fixed; see the Appendix for the history and the
  `GENERATION_DERIVED_LEAF_KEYS` exclusion now baked into every relevant row above.
- [x] `marbl_from_model_settings()` — deleted (§9), 2026-07-16.
- [x] `composition.{model,domain,output}.modified` asymmetry flagged in §7 —
  **fixed 2026-07-16**. All four pieces now report `modified` with "deviate"
  semantics (true iff the current value differs from what the catalog pick
  seeded; edit-then-revert clears it), computed centrally in `_rebuild()`:
  model/output derive from `_diff_overrides(effective, composed)` partitioned by
  `OUTPUT_SECTIONS`/`PARTIAL_OUTPUT_SECTIONS` ownership (`_is_output_key`);
  domain/forcing compare a widget-snapshot taken at the last catalog pick
  (`_domain_seed`/`_forcing_seed`) against the current values. The convention is
  also unified: every piece keeps `origin="catalog"` + `modified=True` on edit
  (forcing no longer flips to `origin="custom"`).
- [x] `resolved_datasets` made authoritative at processing time (2026-07-16) —
  `SourceData`/`input_data.py` now prefer the ForgeBlueprint's frozen
  `forcing.resolved_datasets` snapshot over a live `source_registry` lookup for
  dataset-key and streamable resolution (source_registry remains the fallback for
  names outside the snapshot). See §1/§3 rows. Known pre-existing limitation,
  documented not fixed: the snapshot is keyed by logical name only, so two forcing
  items referencing GLORYS with *different* `glorys_layout`s collapse to one entry
  (this already affected `datasets` before this change too).
- [x] `topography_source`/`topography_path` moved from ForcingSpec-authoring to
  DomainSpec-authoring (2026-07-16), with a dedicated wizard dropdown — see §2.
- [x] The 12 BGC output-write-control fields moved from ModelSpec to OutputSpec
  (2026-07-16), mirroring the existing `marbl_bgc` split via a new generalized
  `PARTIAL_OUTPUT_SECTIONS` mechanism — see §6.
- [x] High-level BGC mode selector (`bgc_mode: marbl|none`, default `marbl`) added,
  mirroring `use_pio` exactly: gates `cppdefs.marbl` and `code.marbl`, and the
  resolver raises if `bgc_mode="none"` but the resolved forcing still requests BGC
  forcing (bgc-type surface/boundary item, IC `bgc_source`, or river
  `include_bgc=True`) — see §4/§7.
- [x] Advanced-settings accordion de-duplicated against dedicated widgets
  (`_ACCORDION_EXCLUDED_FIELDS`: `cppdefs.{use_pio,obc_*,marbl}`,
  `param.{llm,mmm,n,np_xi,np_eta}`, `time_stepping.dt`) — the excluded fields' values
  still flow through from the resolver-composed settings dict; hiding the widget
  cannot drop or reset them. `_diff_overrides` (the load-time override
  reconstruction path) was also guarded against the same exclusion list, closing an
  edge case where a stale saved file could otherwise leave an override on an
  excluded field with no widget to display or clear it.
- [x] Confirmed `use_pio` already correctly gates `code.pio`'s presence (no change
  needed) — used as the template for the `bgc_mode`/`code.marbl` gating above.
- [x] Byte-level `namelist.nml` golden test — done:
  `tests/test_core.py::TestGoldenNamelist::test_golden_namelist_test_tiny` drives the
  real `generate_inputs()` → `configure_build()` chain (real `write_roms_namelist`,
  only roms-tools construction classes mocked) and diffs the rendered `namelist.nml`
  against `tests/fixtures/golden_namelist_test-tiny.nml` (host-rooted paths
  normalized). It doubles as the byte-level proof of the Appendix fix: river/CDR/tides
  land in the golden with their generated values (`nriv=3`, `cdr_ncdr_parm=2`,
  `river_source=.true.`), not resolver placeholders. Still open: a real-generated-data
  integration test (actual GLORYS/ERA5/TPXO/DAI network fetch through
  `process_forge_blueprint`) — a heavier, separate test that doesn't exist yet.

---

## Appendix: the `configure_build` overlay-clobber bug (found and fixed 2026-07-16)

Kept for historical record — this is no longer a live issue; the tables above already
reflect the fixed behavior and don't call it out further.

**What it was**: whenever a domain configured real river forcing or CDR forcing, the
final `namelist.nml` had `river_frc.river_source=False`/`nriv=0` and
`cdr_frc.cdr_source=False`/`cdr_output.do_cdr=False` — even though the river/CDR NetCDF
forcing files were correctly generated and referenced in `forcing.river_path`/the CDR
resource list. ROMS would have run without ever reading the generated river/CDR forcing.
For `tides.ntides`, if the true generated tidal-constituent count (from the real TPXO
extraction) differed from whatever the resolver stored (either the ForcingSpec's
declared `ntides:` or the ModelSpec placeholder), the stale stored count would silently
win in the final namelist.

**Mechanism** (traced end-to-end in `executor.py` + `forge_blueprint_engine.py`, branch
`refactor`):

1. `ForgeExecutor._init_settings_run_time()` (executor.py:1562) seeds
   `self._settings_run_time` as a full deep-copy of `ForgeBlueprint.model_settings`
   (minus `cppdefs`) — this includes `river_frc`, `cdr_frc`, `cdr_output`, and `tides`
   **exactly as the resolver stored them**, i.e. the ModelSpec's disabled placeholders
   (`river_frc.river_source: false`, `cdr_frc.cdr_source: false`, `cdr_output.do_cdr:
   false`) for `river_frc`/`cdr_frc`/`cdr_output` — because `forge_blueprint_resolve.py`
   never touches these three sections at all (confirmed by grep: no
   `river_frc`/`cdr_frc` reference in that file), regardless of whether real rivers or
   CDR are configured in `forcing.river[]`/`forcing.cdr_forcing`.
2. `generate_inputs()` → `input_data.py` builds the real `rt.RiverForcing`/
   `rt.CDRForcing`/`rt.TidalForcing` objects and overlays the *true* values
   (`river_source=True`, the real `nriv`, `cdr_source=True`, the real
   `ncdr_parm`/`cdr_volume`, `cdr_output.do_cdr=True`, the true `tides.ntides`) via
   `_update_settings_run_time(..., allow_new=True)` — a leaf-level deep merge
   (`_deep_merge_settings_dict`, executor.py:126) into the *same* dict from step 1.
3. `configure_build(compile_time_settings, run_time_settings)` received `run_time_settings
   = split_model_settings(cfg)`'s `run_overrides` — **the entire, untouched
   `cfg.model_settings`** from step 1 — and called `_update_settings_run_time(
   run_time_settings)` **with `allow_new=False`** (the default). Because
   `river_frc`/`cdr_frc`/`cdr_output`/`tides` are top-level keys that already exist
   (from step 1), `_deep_merge_settings_dict` recursed into them and overwrote **every
   leaf key present** — which was *all* of them, since `cfg.model_settings` carries the
   full section. This silently restored the step-1 (disabled/placeholder) values,
   undoing step 2.

**Why the same overlay-wins mechanism doesn't bite `param`/`cppdefs.obc_*`**: the
resolver (R) already computes the *same* value from the *same* domain description that
`input_data.py` (G) later re-derives from the live grid object, so clobbering back to
the resolver's value is a no-op there. The bug was specific to sections where (R)
cannot know the true generation-time answer at all (`river_frc`, `cdr_frc`,
`cdr_output.do_cdr`) or only knows a *declared*, not *actual*, answer (`tides.ntides`).

**Verified empirically, in two parts, before fixing (not just read):**

1. **The load-bearing precondition** — that a *real*, river-configured domain actually
   produced a stored placeholder — was checked by running the resolver only (no network, no
   grid build: `build_forge_blueprint(dt=60.0, ...)`) on the real catalog inputs
   (`DomainSpec/ccs-12km` + `ForcingSpec/glorys-era5-unified`, whose `forcing.river` has
   a real configured `DAI` river item). Result: `cfg.forcing.river` was non-empty while
   `cfg.model_settings["river_frc"]` was `{'river_source': False, 'nriv': 0, ...}` — the
   disabled ModelSpec placeholder, verbatim, despite the configured river. `tides`
   resolved to `{'ntides': 15, ...}` (the ForcingSpec's declared value) — the value that
   would have clobbered back over whatever TPXO actually generated if the two ever
   differed.
2. **The merge mechanics** — calling the actual, imported
   `cstar_forge.forge.executor._deep_merge_settings_dict` with a synthetic `river_frc`
   dict reproduced the full sequence: seed disabled (step 1's real value) → merge in
   generation-derived enabled values (`river_source=True, nriv=3`, step 2) → merge the
   original resolver snapshot back on top (step 3) → result reverted to
   `river_source=False, nriv=0`.

**Why existing tests didn't catch this**: the `test-tiny` domain used by
`test_golden_model_settings_test_tiny` and the resolver/builder parity test has no
rivers or CDR configured — and in any case those tests check the *resolver's*
`model_settings` output or `ForgeBlueprint` construction, not the final namelist
produced by a full `process_forge_blueprint()` run. No test asserted on
`river_frc`/`cdr_frc`/`cdr_output` values in a post-`configure_build` namelist for a
domain that actually had rivers/CDR configured.

**The fix**: `forge_blueprint_engine.py` now defines `GENERATION_DERIVED_LEAF_KEYS`
(section → tuple of leaf keys) and `split_model_settings` strips those specific leaf
keys — `river_frc.{river_source,analytical,nriv,rvol_vname,rvol_tname,rtrc_vname,
rtrc_tname}`, `cdr_frc.{cdr_source,cdr_file,ncdr_parm,forcing_parameterized,
cdr_volume}`, `cdr_output.do_cdr`, `tides.{ntides,bry_tides,pot_tides,ana_tides}` — from
the overlay dict it hands to `configure_build`, so the post-generation (real) values
survive instead of being reverted to the pre-generation resolver snapshot. Sibling
fields in the same sections that `generate_inputs` never touches (e.g.
`cdr_frc.relocate_to_wet_pts`) are untouched by the fix and still flow through
normally. This was chosen over two other candidates: changing what the resolver stores
(would have touched the schema/resolver), or reordering `generate_inputs`/
`configure_build` (larger surface area for the same result).

**Regression coverage**, both verified to fail before the fix and pass after:
- `tests/test_forge_blueprint.py::TestForgeBlueprintEngine::
  test_split_model_settings_excludes_generation_derived_leaves` (unit-level, asserts
  the overlay dict excludes the right leaf keys).
- `tests/test_core.py::TestForgeExecutorBuildAndRun::
  test_configure_build_does_not_clobber_generated_river_and_tidal_settings`
  (full-chain: real `build_forge_blueprint` → real `ForgeExecutor.configure_build` with
  `split_model_settings`'s real output, `generate_inputs`'s river/tidal derivation
  simulated the way `input_data.py` does it).

- `tests/test_core.py::TestGoldenNamelist::test_golden_namelist_test_tiny` (byte-level:
  real `generate_inputs()` → `configure_build()` → real `write_roms_namelist`, diffed
  against `tests/fixtures/golden_namelist_test-tiny.nml`; mocks only the roms-tools
  construction classes, so river/CDR/tides reach the namelist with concrete
  generation-derived values rather than placeholders).

Still open: a real-generated-data integration test (actual GLORYS/ERA5/TPXO/DAI data
through `process_forge_blueprint`, no roms-tools mocking) — a heavier test that doesn't
exist yet.
