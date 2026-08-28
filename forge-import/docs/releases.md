# Release notes

## 0.6.0

### Breaking Changes

* Emitted `roms_marbl` blueprints now use schema **3.0.0**, so running them requires a C-Star that includes C-Star #643 (already on C-Star `main`): `model_params` is gone — the time step lives only in the namelist, and `use_pio` moved into `partitioning`. ([#140](https://github.com/CWorthy-ocean/cstar-forge/pull/140))
* In forge blueprints, `partitioning.n_procs_x`/`n_procs_y` are required only when auto-tiling is off. With `auto_tiling: true`, `n_cores` is used instead — if an explicit grid is still present (e.g. from a loaded blueprint), `n_cores` is derived as `n_procs_x * n_procs_y`; an `n_cores` that contradicts the grid is rejected with a clear validation error. ([#140](https://github.com/CWorthy-ocean/cstar-forge/pull/140))
* Requires C-Star `0.12.0` and ucla-roms >= `0.6.0` ([#146](https://github.com/CWorthy-ocean/cstar-forge/pull/146))

### New Features

* **Auto-tiling**: set `partitioning.auto_tiling: true` with `n_cores` (total MPI ranks) and ROMS chooses the tiling at runtime from the land mask, skipping fully-masked tiles. Requires PIO. ([#140](https://github.com/CWorthy-ocean/cstar-forge/pull/140))
* The wizard has an "auto tiling" checkbox: it disables the `n_procs_x`/`n_procs_y` boxes, shows an `n_cores` field pre-filled with `n_procs_x × n_procs_y` from the grid already entered (still editable), and forces PIO on; the choice survives save/load. ([#140](https://github.com/CWorthy-ocean/cstar-forge/pull/140))
* `namelist.nml` now includes a `&PIO_SETTINGS` group (`pio_stride`, default 1) when the model pins ucla-roms 0.6.0 or later ([#141](https://github.com/CWorthy-ocean/cstar-forge/pull/141))
* New `roms-marbl-0.6-default` ModelSpec pinning ucla-roms 0.6.0, carrying the `pio_settings` block; the `pio-dev` spec (ucla-roms `main`) also emits the group via the existing latest-schema fallback ([#141](https://github.com/CWorthy-ocean/cstar-forge/pull/141))
* `pio_settings` is editable in the wizard's advanced model-settings pane (ModelSpec-owned — edits save via "Save Model spec", not the output blueprint) ([#141](https://github.com/CWorthy-ocean/cstar-forge/pull/141))

### Bug Fixes

* Nested (child) domains with MARBL now get BGC variables in `nesting.nc`: `include_bgc` was never being enabled on `make_nesting_info` because the `cppdefs.marbl` flag was invisible to input generation. An explicit `include_bgc` in `metadata_child` still takes precedence. ([#142](https://github.com/CWorthy-ocean/cstar-forge/pull/142))
* The run-time `bgc` namelist section (`bgc.interp_frc`) is now populated during surface-forcing generation for MARBL domains; previously it was silently skipped for the same reason. ([#142](https://github.com/CWorthy-ocean/cstar-forge/pull/142))
* Input generation no longer deadlocks intermittently during NetCDF saves (progress bar frozen at a fixed percentage until the run was killed) — caused by an upstream xarray lock leak, now patched at runtime until fixed in xarray. ([#144](https://github.com/CWorthy-ocean/cstar-forge/pull/144))

### Improvements

* The wizard settings editor now skips sections that are version-gated behind a newer namelist schema than the active ucla-roms ref (e.g. `pio_settings` with a pre-0.6.0 `roms_ref` override), instead of rendering a widget whose edits would be silently discarded downstream; never-modeled sections like `cppdefs` still render as before ([#141](https://github.com/CWorthy-ocean/cstar-forge/pull/141))
* Input generation (`RomsMarblInputData`) is seeded with a deep copy of the executor's resolved compile-time settings (`settings_compile_time_base`) instead of starting empty, so generation steps can read resolved `cppdefs` flags. ([#142](https://github.com/CWorthy-ocean/cstar-forge/pull/142))
* Hardened an unguarded `cppdefs` write in the surface-forcing step (`sal_restore`) that could `KeyError` if the step ran without the grid step. ([#142](https://github.com/CWorthy-ocean/cstar-forge/pull/142))
* Input generation now works directly on the executor's settings dicts (single source of truth) instead of accumulating private copies that were merged back afterward — the class of "generation can't see resolved settings" bugs behind #142 is structurally eliminated. ([#143](https://github.com/CWorthy-ocean/cstar-forge/pull/143))
* BGC capability is threaded as an explicit `has_bgc` flag (mirroring `use_pio`) instead of being fished out of the `cppdefs` dict at each point of use. ([#143](https://github.com/CWorthy-ocean/cstar-forge/pull/143))
* The output from forge blueprints now defaults to `<root-path>/cstar/_forge_bp_runs/<run-name>` (instead of `<root-path>/cstar-forge-run/<run-name>`) ([#145](https://github.com/CWorthy-ocean/cstar-forge/pull/145))
* The output from roms blueprints created by forge now defaults to `<root-path>/cstar/_roms_bp_runs/<run-name>` (instead of `<root-path>/cstar-roms-run/<run-name>`) ([#145](https://github.com/CWorthy-ocean/cstar-forge/pull/145))
* Both defaults keep the existing HPC scratch substitution (`$SCRATCH` on Perlmutter, `$SCRATCH` falling back to `$WORK/scratch` on Anvil) ([#145](https://github.com/CWorthy-ocean/cstar-forge/pull/145))
* The `roms-marbl-0.6-default` ModelSpec now pins ucla-roms `0.6.1` ([#145](https://github.com/CWorthy-ocean/cstar-forge/pull/145))

### Miscellaneous

* The tagged GitHub release is now created automatically when the "Finalize release notes for `<tag>`" PR is merged, using the release notes just finalized in the docs as the release body — instead of the manual tag-and-publish step with GitHub's weaker auto-generated summary. ([#139](https://github.com/CWorthy-ocean/cstar-forge/pull/139))
* Merging the release-notes finalization PR no longer causes the release-notes updater to re-open a spurious "Unreleased" section. ([#139](https://github.com/CWorthy-ocean/cstar-forge/pull/139))
* Shipped example blueprints re-stamped with fresh `content_hash`es; golden fixtures updated for the new `auto_tiling` default. ([#140](https://github.com/CWorthy-ocean/cstar-forge/pull/140))
* Blueprint reference docs updated for schema 3.0.0. ([#140](https://github.com/CWorthy-ocean/cstar-forge/pull/140))
* New golden fixtures for the 0.6.0 tier (`golden_namelist_test-tiny-roms060.nml`, `golden_model_settings_test-tiny-roms060.json`) with the same `UPDATE_GOLDEN=1` regeneration flow as the 0.5.0 siblings ([#141](https://github.com/CWorthy-ocean/cstar-forge/pull/141))
* `docs/architecture-details.md` updated for the new schema tier; release-notes entry added to `docs/releases.md` ([#141](https://github.com/CWorthy-ocean/cstar-forge/pull/141))
* Shipped catalog blueprints and example YAML updated to the new default paths (`working_dir` is excluded from `content_hash`, so no restamping) ([#145](https://github.com/CWorthy-ocean/cstar-forge/pull/145))
* Docs updated to the new paths (getting-started, source-data guides, architecture details, roms-marbl blueprint reference) ([#145](https://github.com/CWorthy-ocean/cstar-forge/pull/145))


## 0.5.0

### Breaking Changes

* Requires cstar-ocean 0.11.0 or higher ([#137](https://github.com/CWorthy-ocean/cstar-forge/pull/137))

### New Features

* Users can attach existing netcdf files for grids, CDR Forcing, and rivers ([#121](https://github.com/CWorthy-ocean/cstar-forge/pull/121))
* The repo-bundled catalog is no longer the default save location and sole catalog location. A new multi-tier catalog is implemented, allowing read-only from any number of locations, automatic appending of bundled catalog items, and a default read/write catalog layer in the user's home directory. More catalog configuration and features will be coming soon, but this change at least decouples the repo location from the primary catalog save location. ([#122](https://github.com/CWorthy-ocean/cstar-forge/pull/122))
* The MARBL codebase tag can now be specified in the wizard. ([#131](https://github.com/CWorthy-ocean/cstar-forge/pull/131))
* The MARBL codebase defaults to the [C]Worthy fork (but can still be edited in the model spec) ([#131](https://github.com/CWorthy-ocean/cstar-forge/pull/131))
* Tie in to new C-Star namelist versioning features ([#137](https://github.com/CWorthy-ocean/cstar-forge/pull/137))
* Provide new default ModelSpec for ROMS 0.5.0 ([#137](https://github.com/CWorthy-ocean/cstar-forge/pull/137))
* Add precheck-safe output specs for daily, weekly, monthly restarts ([#137](https://github.com/CWorthy-ocean/cstar-forge/pull/137))
* Add up-front checks for incompatible output frequencies ([#137](https://github.com/CWorthy-ocean/cstar-forge/pull/137))

### Bug Fixes

* Fixed bug where exceptions were not printed into forge log files ([#121](https://github.com/CWorthy-ocean/cstar-forge/pull/121))
* Fixed "ValueError: Separator is not found, and chunk exceed the limit" error in wizard run box ([#121](https://github.com/CWorthy-ocean/cstar-forge/pull/121))
* The executor will no longer error out if boundary conditions are not generated (a legitimate use case for child domains) ([#126](https://github.com/CWorthy-ocean/cstar-forge/pull/126))
* Remove river forcing params that have no effect from Advanced Settings (these get dynamically determined by the river forcing setup) ([#132](https://github.com/CWorthy-ocean/cstar-forge/pull/132))
* fix _deep_merge in the resolver to deep-copy override values, so resolved blueprints never alias the shared OutputSpec section dicts. ([#134](https://github.com/CWorthy-ocean/cstar-forge/pull/134))

### Improvements

* Relocation of data output to SCRATCH space on HPCs is now backwards-compatible with blueprints that point to the old default location. ([#120](https://github.com/CWorthy-ocean/cstar-forge/pull/120))
* Machine yamls have been removed from Forge. They needlessly duplicated C-Star machine configurations and were no longer used. Machine-based directory setup remains, but will be abstracted/consolidated in upcoming work. ([#124](https://github.com/CWorthy-ocean/cstar-forge/pull/124))
* The "register kernel" procedure is now available through the CLI for conda installations ([#127](https://github.com/CWorthy-ocean/cstar-forge/pull/127))
* Downloaded source data (previous and new downloads) will receive group read permissions if possible ([#128](https://github.com/CWorthy-ocean/cstar-forge/pull/128))
* Make initial conditions optional for child grids ([#133](https://github.com/CWorthy-ocean/cstar-forge/pull/133))
* Improve UI/UX for selecting monthly/periodic average/instantaneous optionality for restarts and cdr_output ([#134](https://github.com/CWorthy-ocean/cstar-forge/pull/134))
* Validate restart-output period divides evenly by dt at blueprint creation and forge execution ([#135](https://github.com/CWorthy-ocean/cstar-forge/pull/135))

### Miscellaneous

* Get rid of "pieces" terminology throughout code, wizard, docs ([#123](https://github.com/CWorthy-ocean/cstar-forge/pull/123))
* Voila backing notebook moved to avoid confusion with user-runnable wizard notebook. ([#125](https://github.com/CWorthy-ocean/cstar-forge/pull/125))
* A conda activate/deactivate script will be in the next conda build that performs runtime protection against .local shadowing (similar to harden-env.sh, but for conda installs) ([#127](https://github.com/CWorthy-ocean/cstar-forge/pull/127))
* Pin `compilers<2` to get around temporary mpicc wrapper error from mpich ([#129](https://github.com/CWorthy-ocean/cstar-forge/pull/129))
* Revert compilers<2, add compiler-specific mpich packages to ensure macos has the exact compilers needed ([#130](https://github.com/CWorthy-ocean/cstar-forge/pull/130))

## 0.4.0

### Breaking Changes

* Dropped experimental `--stage-ic-sources` option. ([#114](https://github.com/CWorthy-ocean/cstar-forge/pull/114))

### New Features

* Installation instructions and documentation have been overhauled ([#113](https://github.com/CWorthy-ocean/cstar-forge/pull/113))

### Bug Fixes

* Fixed `do_cdr` (now `do_cdr_output`) behavior ([#116](https://github.com/CWorthy-ocean/cstar-forge/pull/116))
* Fix tides still being active even if tidal forcing is turned off ([#118](https://github.com/CWorthy-ocean/cstar-forge/pull/118))

### Improvements

* Subchunking of glorys data is now enabled by default; users can opt out with `--no-subchunk`. ([#114](https://github.com/CWorthy-ocean/cstar-forge/pull/114))
* Use CWorthy fork of PIO to get needed feature for upcoming ROMS release (0.4.0) ([#115](https://github.com/CWorthy-ocean/cstar-forge/pull/115))
* `do_cdr` renamed to `do_cdr_output` to better reflect functionality and match namelist; migration added for backwards compatibility ([#116](https://github.com/CWorthy-ocean/cstar-forge/pull/116))
* automatically add necessary MARBL diagnostics when CDR output is enabled ([#116](https://github.com/CWorthy-ocean/cstar-forge/pull/116))
* Enable support for Unified BGC dataset v2.1 ([#117](https://github.com/CWorthy-ocean/cstar-forge/pull/117))
* Add additional model specs for newer roms releases ([#118](https://github.com/CWorthy-ocean/cstar-forge/pull/118))
* Default to pio-dev model spec for now, which tracks the latest roms `main` ([#118](https://github.com/CWorthy-ocean/cstar-forge/pull/118))
* Add "simple" BGC forcing spec for demos with minimal complexity (notably, no tides, so no TPXO needed) ([#118](https://github.com/CWorthy-ocean/cstar-forge/pull/118))
* Register forge applications with cstar using entrypoints (requires C-Star 0.10.0) ([#118](https://github.com/CWorthy-ocean/cstar-forge/pull/118))

### Miscellaneous

* Update release notes finalizer to remove sections with nothing in them ([#112](https://github.com/CWorthy-ocean/cstar-forge/pull/112))
* Update release notes updater to handle unbulleted content ([#112](https://github.com/CWorthy-ocean/cstar-forge/pull/112))
* Usage of plain `cstar blueprint run` for the forge application encouraged in examples and wizard Run box ([#118](https://github.com/CWorthy-ocean/cstar-forge/pull/118))
* Properly include subchunking dependencies in default package ([#118](https://github.com/CWorthy-ocean/cstar-forge/pull/118))

## 0.3.0

### Breaking Changes

* Renamed `SpecConfig` to `ForgeBlueprint`; forge is now a real C-Star application that processes a `ForgeBlueprint` via `python -m cstar_forge.run`
* Removed the `CstarSpecBuilder`/`CstarSpecEngine` preconfig/postconfig/build/run "stages" concept in favor of a single in-memory blueprint persisted once, at build time
* Reorganized the catalog into spec directories (`ModelSpec`, `DomainSpec`, `ForcingSpec`, `OutputSpec`, `Machines`) and switched catalog files from `.yml` to `.yaml`
* Consolidated model defaults directly into each model's `model.yaml` (no more separate `settings-defaults.yaml`)
* `namelist.nml`'s `&extract_data_settings` group now writes `extract_root_name` (defaults to `"child"`), which requires ucla-roms ≥ commit `faba77a5` (merged to `main`, no tagged release yet). The `cson_roms-marbl_v0.1` ModelSpec is pinned to ucla-roms `0.2.0`, which predates this key — building against it will fail ROMS's namelist read. Use the `pio-dev` ModelSpec (pinned to ucla-roms `main`) or override `code.roms.commit` to `main` until a new tagged ModelSpec is cut

### New Features

* Added an interactive `ForgeBlueprint` wizard (Jupyter + Voilà) for building and reviewing blueprints
* Added an option to build with ParallelIO (PIO)
* Added support for chunked GLORYS staging on large domains
* Improved parent/child (nested) grid support
* `ForgeBlueprint.to_yaml`/`to_yaml_str` now stamp `provenance.generated_at`/`forge_version`/`cstar_version`/`roms_tools_version` on first save (preserved on later resaves), so a saved blueprint records which Forge/C-Star/roms-tools produced it — e.g. for checking out a matching Forge commit later. `cstar_version`/`roms_tools_version` are read from the installed package metadata (which already embeds commit info for an editable/dev checkout, via `setuptools_scm`) and reflect the environment doing the *saving*, not necessarily the one that later processes the blueprint, if that happens on a different machine.

### Bug Fixes

* Fixed several source-data and dataset-loading issues (SRTM15, GLORYS/xgcm skew, dask memory/thread limits)

### Improvements

* Hardened dev-setup scripts against a `~/.local` pip fallback on HPC systems
* Added CDR (carbon dioxide removal) forcing YAML support
* Added flexible run-window (`allow_flex_time`) support
* Add support for new "extract_root_name" nml key (only passes through the default value of `child` for now) ([#107](https://github.com/CWorthy-ocean/cstar-forge/pull/107))
* The output from forge blueprints now defaults to `<root-path>/cstar-forge-run` (instead of `<root-path>/cstar-forge-data/cstar-forge-run`) ([#110](https://github.com/CWorthy-ocean/cstar-forge/pull/110))
* The output from roms blueprints now defaults to `<root-path>/cstar-roms-run` (instead of `<root-path>/cstar-forge-data/cstar-blueprint-run`) ([#110](https://github.com/CWorthy-ocean/cstar-forge/pull/110))

### Miscellaneous

* Overhaul installation procedure, dependency structure, and instructions ([#109](https://github.com/CWorthy-ocean/cstar-forge/pull/109))

## 0.2.0

### Breaking Changes

* Drops support for [C]Worthy ucla-roms versions <0.2: removes support for running roms with roms.in and the classic suite of .opt files. Replaces that functionality with support for ROMS namelists. ([#100](https://github.com/CWorthy-ocean/cstar-forge/pull/100))
* Removed no-bgc model example ([#100](https://github.com/CWorthy-ocean/cstar-forge/pull/100))

### New Features

* Supports [C]Worthy ucla-roms version 0.2 (runtime namelists) ([#100](https://github.com/CWorthy-ocean/cstar-forge/pull/100))

### Bug Fixes

* Move catalog path to user directory ([#100](https://github.com/CWorthy-ocean/cstar-forge/pull/100))
* Fix issue where boundary files were not skipped for child grids ([#100](https://github.com/CWorthy-ocean/cstar-forge/pull/100))

### Improvements

### Miscellaneous
