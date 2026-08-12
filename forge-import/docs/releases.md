# Release notes

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
