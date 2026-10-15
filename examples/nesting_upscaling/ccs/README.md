# CCS nested + upscaled CDR example (16 km parent, 4 km child)

An eight-step workplan on the California Current: a 168x336x20 parent (16 km x 15.7 km,
rotated 33.3 degrees) with a 70x140x20 child (3.93 km x 4 km, 4:1) nested in it, one
alkalinity (ALK) release in the child near Monterey Bay, and the child's CDR tracer
tendencies upscaled back onto the parent grid and fed to a second parent run:

```
forge_parent -> parent -----------------.--> child --> upscale --> parent_up
forge_child  ----------> nest_ic -------'
forge_parent_up -------------------------------------------------^
```

It runs on ucla-roms main at commit `453189ed` (after #390, which makes the upscaled depth-profile
CDR forcing conserve its flux; `ROMS_COMMIT` in `make_forge_blueprints.py`, to be replaced with
the 0.10.2 tag once released) on top of the `roms-marbl-0.10-default` model (MARBL max-it-10,
ParallelIO 2.7.1-fork), with real forcing: GLORYS (global layout), ERA5, GLODAP + WOA_BGC +
UNIFIED BGC, MBL CO2, WOA sea-surface-salinity restoring, TPXO tides and GloFAS rivers with
RIVR2O BGC (the bundled `glorys-era5-glodap-woa` ForcingSpec with its DAI rivers replaced by
GloFAS, see "Known issues"). Output is `daily-restarts`.

**Status.** The smoke run (`smoke.vars`: 4 days of parent, the last 2 of child and parent_up)
passed end to end on tanagra on 2026-10-14 (attempts a8, a10 and a12, see "Smoke run on
tanagra"), with the files as they were then: ucla-roms 0.10.1, DAI rivers, ALK at 1e4 meq/s. The
committed files differ from those in three ways (GloFAS rivers, ucla-roms main, ALK at 1e4 mol/s)
and nothing has been run with them yet. Three longer windows are prepared for Anvil (SLURM), none of
them run: `month.vars` (31-day parent, 16-day child and parent_up), `six_month.vars` (181-day
parent, 91-day child and parent_up) and `full.vars` (365-day parent, 184-day child and
parent_up); see "Run on Anvil". The smoke runs released ALK at 1e4 meq/s (10 mol/s), 1000
times less than intended, because the first version of the files gave the rate in the wrong
unit; the committed files now release 1e4 mol/s (1.0e7 meq/s) (see "CDR placeholders").

**C-Star version.** It needs a C-Star with two fixes that came with these examples: the
legacy source-data link behind symlinked directories, and the upscaler's time ordering of
the `_uscl` files. Releases up to and including 0.16.1 have neither.

## Files

| File | What |
|---|---|
| `make_forge_blueprints.py` | Generates the three forge blueprints from the bundled catalog (rerun after a catalog or resolver change); flags `--out-dir`, `--rivers {glofas,dai,none}` (default `glofas`), `--river-climatology`, `--pressure-fluxes` undo or vary the workarounds below. `ROMS_COMMIT` pins ucla-roms. |
| `forge_parent.yaml` | Parent: Ben's grid (open N/S/W, SRTM15, dt 900 s, 4x4 tiles), `grid_kwargs_child` = the child grid, `metadata_child.period` 3600 s, no pressure fluxes in the nesting extraction (see Known issues). `cdr.mode: none`. |
| `forge_child.yaml` | Child: `grid_kwargs_parent` = the parent grid, the 70x140x20 child grid (dt 240 s, 2x4 tiles), no initial/boundary forcing (they come from `nest_ic` and `nest-from`), `cdr.mode: yaml` with one ALK release, `upscale_output.do_upscale` and `cdr_output.do_cdr_output` on. |
| `forge_parent_up.yaml` | The parent grid again (4x4), no child, `cdr.mode: upscaled`, `cdr_output.do_cdr_output` on, `cdr_frc.nz_chd: 20` (the child's levels). |
| `workplan.yaml` | The eight steps, runtime variables for every date; no launcher is named, so it runs locally on a workstation and under SLURM where there is a scheduler (every step has a `local` and a `slurm` entry in `compute_overrides`). |
| `smoke.vars` | The smoke values: parent 2010-01-15 to 01-19, child and parent_up the last 2 days, 4x4 / 2x4 tiles, hourly CDR output. |
| `month.vars` | Month run (not yet run): parent 2010-01-01 to 02-01, child and parent_up 01-16 to 02-01, ALK released 01-16 to 01-24, tiles 4x4 / 5x10 / 4x4, daily CDR output. |
| `six_month.vars` | Six-month run (not yet run): parent 2010-01-01 to 07-01, child and parent_up 04-01 to 07-01, ALK released 04-01 to 05-01, same tiles and cadence. |
| `full.vars` | Full-run values (not yet run): 365-day parent, 184-day child and parent_up, daily CDR output. |

The committed blueprints carry the smoke windows too, so `cstar blueprint check` and a
stand-alone `cstar blueprint run` see a valid, short run; the workplan's overrides replace
the windows for a full run, so the same files serve both.

## Run

From the `ccs/` directory (the workplan's `./forge_*.yaml` paths are relative to the workplan
file, so any directory works). The smoke test, 4 days of parent and 2 of child, takes about 25
minutes on tanagra:

```bash
cstar workplan check workplan.yaml --varfile smoke.vars
cstar workplan run   workplan.yaml --varfile smoke.vars --run-id ccs-smoke
cstar run status ccs-smoke
```

The run-id defaults to the workplan name (`ccs-nest-upscale`); give the smoke and the full
run different `--run-id`s, because re-using one attaches to the earlier run. The full run is
the same command with `full.vars` (proposed values, not yet run: see "Full run" below).

### Runtime variables

| Variable | Used for | `smoke.vars` | `full.vars` (`month.vars` and `six_month.vars`: see Files) |
|---|---|---|---|
| `parent_start` | `forge_parent` `run.start_date` | 2010-01-15T00:00:00 | 2010-01-01T00:00:00 |
| `parent_end` | `forge_parent` `run.end_date`, `parent` `runtime_params.end_date` | 2010-01-19T00:00:00 | 2011-01-01T00:00:00 (365 days) |
| `child_start` | `forge_child`/`forge_parent_up` `run.start_date`, the CDR `start_time` and release start, `parent_up`'s `continue-from` timestamp | 2010-01-17T00:00:00 | 2010-07-01T00:00:00 |
| `child_start_stamp` | the same instant as `YYYYMMDDHHMMSS`, for the parent restart file name `output_rst.<stamp>.nc` that `nest_ic` reads | 20100117000000 | 20100701000000 |
| `child_end` | `forge_child`/`forge_parent_up` `run.end_date`, the CDR `end_time`, `child`/`parent_up` `runtime_params.end_date` | 2010-01-19T00:00:00 | 2011-01-01T00:00:00 (184 days) |
| `cdr_end` | end of the ALK release (the release's second time) | 2010-01-19T00:00:00 (on for the whole window) | 2010-07-31T00:00:00 (`child_start` + 30 days) |
| `parent_np_x`, `parent_np_y` | `domain.partitioning` of `forge_parent` and `forge_parent_up`: the MPI tiles (and ranks) of `parent` and `parent_up`; must divide 168 and 336 | 4, 4 | 4, 4 |
| `child_np_x`, `child_np_y` | the same for `forge_child`/`child`; must divide 70 and 140 | 2, 4 | 5, 10 |
| `cdr_output_period`, `cdr_output_nrpf` | `model_settings.cdr_output` of `forge_child` and `forge_parent_up`: averaging period (s) and records per file; `nrpf x period` must divide the 86400 s restart period | 3600, 24 | 86400, 1 |

Constraints (not checkable by `workplan check`): `parent_start` < `child_start` < `child_end`
<= `parent_end`; `child_start` must be a day boundary on which the parent writes a daily
restart; `child_start_stamp` must spell `child_start`; `child_start` <= `cdr_end` <= `child_end`.
`cdr_end` need not be `child_start` + 30 days in the smoke test: with `cdr_end = 2010-01-18T00:00:00`
the smoke window exercises the release switching off.

The same variables reach the forge steps through `blueprint_overrides` (the forge window,
`cdr.cdr_forcing.start_time`/`end_time` and the release's `times`, the tiling and the CDR output
cadence) and the roms steps through `runtime_params.end_date`. Forge-step overrides do apply:
the scheduler packages them into an `apply-overrides` directive, `cstar blueprint run` merges
them into the forge blueprint (`deep_merge`; lists merge element by element, so `releases:
[{times: [...]}]` updates release 0 only) and re-validates it, and `cstar workplan check`
performs the same merge in memory. The tiling needs no regenerated blueprint: the forge-written
`namelist.nml` keeps the committed `np_xi`/`np_eta`, but C-Star takes them (and the `mpirun -n`)
from the blueprint's `partitioning` when the ROMS step runs (`cstar/roms/simulation.py`), which
the override changes (seen with 8x8 and 5x10 on tanagra).

## Smoke run on tanagra (2026-10-14)

These runs used ucla-roms 0.10.1 and DAI rivers (`convert_to_climatology: never`); the committed files now use ucla-roms main at `453189ed` and GloFAS rivers.

These runs used the first version of the release, `ALK: [1.0e4, 0.0]` read by roms-tools as
**meq/s**, so 1e4 meq/s = 10 mol/s: 1000 times less than the intended 1e4 mol/s (1.0e7 meq/s) that
`forge_child.yaml` now carries. The child's `ALK_source` maximum of 750 below is consistent with that, and every
release-related magnitude below (the child's `ALK_source`, the upscaled fluxes, parent_up's
`ALK_source`) should be about 1000 times larger with the corrected rate. The timings, the file
checks and the structure of the chain do not depend on the rate.

Attempt `ccs-smoke-a8` (16 / 8 / 16 ranks, hourly CDR output; `~/cstar/workplan_runs/ccs-smoke-a8` on tanagra) passed end to end in 21 min with every step Done; `a10` (the same, with the ROMS steps waiting for the forge steps so nothing competes) and `a12` (8x8 / 5x10 / 8x8 ranks, daily CDR output) also passed. Wall times (a10 | a12):

| Step | a10 | a12 |
|---|---|---|
| forge_parent (4 days) | 10 min 38 s | 8 min 34 s |
| forge_child (2 days) | 3 min 53 s | 3 min 45 s |
| forge_parent_up (2 days) | 8 min 30 s | 7 min 29 s |
| parent: setup + compile, then ROMS | 89 s + 96 s | 92 s + 97 s |
| nest_ic | 17 s | 17 s |
| child: setup + compile, then ROMS | 89 s + 66 s | 89 s + 42 s |
| upscale | 1 s | 1 s |
| parent_up: setup + compile, then ROMS | 87 s + 109 s | 96 s + 54 s |
| ROMS seconds per simulated day: parent / child / parent_up | 23.1 / 29.8 / 52.4 | 22.6 / 18.3 / 23.4 |

Setup + compile (git refresh of ucla-roms, MARBL and ParallelIO, three builds, input staging) is 87-119 s per ROMS step. Four times the ranks buys nothing for the parent (0.23 s per step at 16 and at 64 ranks), a modest gain for the child (8 -> 50 ranks) and none for parent_up; what shortens parent_up is the CDR output cadence (5.9 GB per simulated day hourly on the parent grid, 0.25 GB daily).

Forge scales with the window, about 30 s per window day for `forge_parent` and `forge_parent_up` (ERA5 about 10 s, GLORYS boundary about 19 s, plus 2 min of initial conditions) and 13 s for `forge_child`: a 30-day window took 23 min 31 s, 9 min 34 s and 22 min 42 s (`ccs-forge30-a1`, forge steps only).

Checks on a8, a10 and a12: `child_bry.<ts>.nc` (hourly, one file a day) and daily `output_rst.<ts>.nc` in the parent's output; `ic_from_parent_rst.<child start>.nc` in nest_ic's; `output_uscl.<ts>.nc` in the child's; `upscaled_cdr.nc` is CDF-5 (`ncdump -k`) with 49 hourly `cdr_time` values, strictly increasing, 286 profiles x 20 levels x 2 tracers, all finite, about 65 k non-zero values of at most 2.8e-6 mmol/s (not the release signal: the release is about 190 km from the nearest open boundary, the west one, and has had 2 days); the child's `ALK_source` reaches 750; parent_up's `output_cdr` has a finite `ALK_source` of both signs (about 1e-5 at most); no ERROR, BLOWUP, "Ran out of time records" or "past current time" in any ROMS log; the release site is wet on the built child grid (nearest rho point 2.1 km away, h = 72 m, below).

## Full run (not run)

`full.vars`: parent 2010-01-01 to 2011-01-01 (365 days), child and parent_up 2010-07-01 to 2011-01-01 (184 days), ALK released 2010-07-01 to 07-31, tiles 4x4 / 5x10 / 4x4 (16 / 50 / 16 ranks), daily CDR output.

Estimate, using the measured rates above (critical path, in hours):
`forge_parent` 10.6 min + 361 days x 31 s = 3.2; `parent` 1.5 min + 365 x 22.7 s = 2.3; `nest_ic` and `upscale` (about 1 min); `child` 1.5 min + 184 x 18.3 s = 0.9; `parent_up` 1.5 min + 184 x 23.4 s = 1.2; total about 7.8 h. `forge_parent_up` (8.5 min + 182 x 31 s = 1.7 h) and `forge_child` (3.9 min + 182 x 13 s = 0.7 h) run beside `parent` and finish before they are needed. Allow 1.5x for contention and a shared NFS (12 h) and 2.5x for the worst case (20 h). Two years of parent with one of child would be about 15 h typical.

Disk (daily CDR output): `parent` 0.81 GB per day (768 MB restart + 40 MB `child_bry`) = 296 GB, `child` 0.18 GB per day = 34 GB, `parent_up` 1.0 GB per day = 186 GB, forge inputs about 25 GB. Hourly CDR output would add about 1.3 TB; keep `cdr_output_period` 86400.

Launch the way the smoke runs were launched: in tmux, pinned to 64 CPUs (one hardware thread of each of 64 physical cores, so the forge steps' dask workers and the ROMS ranks cannot take more on the shared 256-CPU host), with `CSTAR_NPROCS_POST` capped (its default is a third of the CPUs):

```bash
tmux new -d -s ccs-full 'taskset -c 64-127 bash -c "source ~/miniforge3/etc/profile.d/conda.sh && conda activate cstar-env && export CSTAR_NPROCS_POST=8 && cd ~/packages/C-Star/examples/nesting_upscaling/ccs && cstar workplan run workplan.yaml --varfile full.vars --run-id ccs-full-a1 2>&1 | tee ~/cstar/ccs-full-a1.log"'
```

## Run on Anvil (SLURM)

`workplan.yaml` names no launcher, so on Anvil C-Star picks the SLURM launcher by itself and the same file and the same `--varfile`s are used as on a workstation. Every step carries its SLURM resources in `compute_overrides.slurm` (`cstar workplan check` on Anvil shows the resolved values; the roms steps need an explicit `num_cpus` because a deferred blueprint cannot be read at submit time):

| Step | CPUs | Walltime | Why |
|---|---|---|---|
| `forge_parent`, `forge_child`, `forge_parent_up` | 32 | 12 h / 6 h / 10 h | the memory of an Anvil job scales with its CPUs (1.9 GB each); forge was killed for memory at 8 CPUs on a small domain; forge steps are single-node. Raise it if a full-window forge step runs out of memory |
| `parent`, `parent_up` | 16 | 12 h | = `parent_np_x * parent_np_y` (4x4), build included |
| `child` | 50 | 12 h | = `child_np_x * child_np_y` (5x10); the same 16 / 50 / 16 serve `month.vars`, `six_month.vars` and `full.vars`; `smoke.vars` needs only 16 / 8 / 16 |
| `nest_ic`, `upscale` | 4 | 1 h | small |

If you change the tiling in a vars file, change the matching `num_cpus` in `workplan.yaml` (fewer CPUs than ranks makes `srun` fail; more wastes the allocation). Inside a job ROMS runs as a local `srun` process on the job's CPUs; it does not submit a second job.

Setup, once per shell (the job scripts run with your environment, `--export=ALL`, and contain no `module` or `conda` lines, so the shell that runs `cstar workplan run` must have the environment active):

```bash
module load conda && conda activate cstar-nest
export CSTAR_SLURM_ACCOUNT=x-ees250129 CSTAR_SLURM_QUEUE=shared
export CSTAR_PROJECT_HOME=/anvil/projects/x-ees250129   # source-data cache: <that>/cstar/source-data
export CSTAR_NPROCS_POST=8                              # default is a third of the node's cores, whatever the job asks for
cd ~/packages/C-Star/examples/nesting_upscaling/ccs
```

Run (replace the window and the run-id for the other runs; the run lands in `$SCRATCH/cstar/workplan_runs/<run-id>`):

```bash
cstar workplan check workplan.yaml --varfile month.vars
cstar workplan run   workplan.yaml --varfile month.vars --run-id ccs-month-a1
cstar run status ccs-month-a1
```

`cstar workplan run` submits all eight jobs with `afterok` SLURM dependencies (and `--kill-on-invalid-dep`, so a failed step cancels what follows) and then returns, within a minute or so; it does not have to keep running, so no `tmux` or `nohup` is needed. Re-running the same command with the same run-id attaches to the run and schedules only what is not yet scheduled. Each job also mails you (`--mail-type=ALL`). Estimates from the tanagra rates (Anvil's are not measured): `month.vars` about 1 h of critical path and about 50 GB of output, `six_month.vars` about 4 h and about 300 GB, `full.vars` about 8 h and about 540 GB, plus queue waits; a few hundred core-hours at most for each.

## Prerequisites

- C-Star installed from source (see `docs/installation.rst`, "Install from source") on a
  version with the fixes named at the top: branch `claude/nested-domain-upscale-toy-efdb2b`
  until it is merged.
- Source data under `cstar forge show-paths` -> `source_data`: GLORYS in the **global** layout
  for the window padded by a day each side (`GLORYS_GLOBAL/cmems_mod_glo_phy_my_0.083deg_P1D-m_GLOBAL_YYYYMMDD.nc`;
  with GLORYS cached, no Copernicus Marine login is needed), `SRTM15`, `TPXO/TPXO10.v2a`,
  `UNIFIED_BGC/BGCdataset_v2_1.nc`, `GLODAP`, `WOA` (woa18 salinity), `MBL_CO2`, `RIVR2O` and
  `GLOFAS/glofas_v4_rivers_daily_w_rivr2o.nc` (the GloFAS v4 discharge with the per-station
  RIVR2O concentrations, 1979-2024 daily; user-staged, forge does not download it). ERA5
  streams. The WOA23 BGC files (78 of them, `woa23_*_01.nc`) must sit directly in the `WOA/`
  directory next to the salinity; forge downloads whatever is missing there (about 3 GB for
  WOA_BGC) and fails with `PermissionError` if the cache is read-shared but write-private
  (`test -w <source_data>`). `--rivers dai` instead streams DAI and needs no GLOFAS file.
- Network access for ERA5 (Google Cloud), any missing download and the source clones (the
  `git pull` of the shared `~/.cache/cstar` clones can fail transiently: it is logged as an
  ERROR and the step goes on).
- Cores: the three ROMS steps are chained (`parent` -> `child` -> `parent_up`), so only one
  runs at a time, and the forge steps and `parent` can overlap. With `smoke.vars` the `parent`
  and `parent_up` runs use 16 ranks (4x4) and the `child` 8 (2x4), so at most 16 ROMS cores at a
  time. With `full.vars` the parents stay at 16 (4x4) and the child is 5x10 = 50 ranks, so up to
  50 ROMS cores at a time (the forge steps' dask workers come on top); the full run is pinned to
  64 CPUs (see "Full run").
- The first run compiles ROMS for each roms step (the parent's cppdefs differ from the other
  two); the `~/.cache/cstar` clones of ucla-roms, MARBL and ParallelIO are reused.

## What each step exercises

- **forge_parent**: the v10 forge blueprint on `roms-marbl-0.10-default` (ucla-roms pinned to main at `453189ed`, templates bundled with C-Star); a composite BGC IC/boundary (GLODAP ALK/DIC, WOA_BGC NO3/PO4/SiO3/O2, UNIFIED the rest) on GLORYS global; the nesting extraction for a child grid (`child_bry`, every hour; pressure fluxes off, see Known issues) built after `align_grids` of the child onto the parent.
- **forge_child**: a nested child (parent grid rebuilt from `grid_kwargs_parent` and `align_grids`-ed, no GLORYS, `bry_tides` off); `cdr.mode: yaml` with a runtime-overridden release schedule; `upscale_output` (`*_uscl` files) and CDR output.
- **forge_parent_up**: `cdr.mode: upscaled` (placeholder `cdr_forcing` dataset, `forcing_depth_profiles`, no generation step).
- **parent**: a deferred-blueprint ROMS-MARBL run with the forge-step window; daily restarts.
- **nest_ic**: an inline `nest_ic` blueprint reading the forge-written grids and the parent restart at the child start; CDF-5 conversion with `nccopy`.
- **child**: the `nest-from` (boundaries from the parent's `child_bry`) and `continue-from` (`nest_ic` output) directives together, on a deferred forge blueprint.
- **upscale**: an inline `upscaler` blueprint joining the child's `*_uscl` files into `upscaled_cdr.nc`.
- **parent_up**: `continue-from` with a `timestamp` (the parent's restart at the child start) and a `cdr_forcing.data[0].location` override that replaces the forge placeholder with the upscaler's file.

## CDR placeholders (for Ben to confirm)

| | Value | Reasoning |
|---|---|---|
| Type | ALK `tracer_perturbation` (MARBL tracer `ALK`) | alkalinity enhancement; CDR output on |
| Site | 36.8 N, -122.0 E, injection depth 2 m | Monterey Bay, about 18 km off Moss Landing; well inside the child (about 48 km east and 9 km south of its centre in the rotated frame, 140 km / 275 km from its edges) |
| Rate | 1e4 mol/s (= 1.0e7 meq/s), constant for the first 30 days of the child window, then 0 | roms-tools reads ALK `tracer_fluxes` in **meq/s**, not mol/s: step-like (`time_interpolation` false), `times: [child_start, cdr_end]`, `ALK: [1.0e7, 0.0]` (`RELEASE_ALK_FLUX` in `make_forge_blueprints.py`). 1e3 mol/s sustained for a year is 3.2e10 mol, about 1.4 Mt CO2 at 1:1 (of order 1 Mt CO2/yr), so 1e4 mol/s is a deliberately strong 10x; 30 days of it is 2.6e10 mol, about 1.1 Mt CO2 of potential (1:1 upper bound): large enough for clear plots and a non-negligible upscaled flux |
| Spread | `hsc` 4000 m, `vsc` 10 m | about one child cell horizontally; the surface mixed layer vertically |

Spread over roughly 2.5e9 m3 (the Gaussian's effective volume, about 2.6e12 kg), 1e4 mol/s adds
8.6e8 mol a day, about 3e2 umol/kg of ALK per day if nothing flushed the patch.

**Caveat for Ben.** A sustained 1e4 mol/s at one site may push the carbonate chemistry into a
nonlinear regime near the source (3e2 umol/kg per day is a large fraction of a surface ALK of
about 2.3e3 umol/kg, and the release is a single patch, not spread over a coast). The
conservative alternative is 1e3 mol/s (1e6 meq/s): set `RELEASE_ALK_FLUX = 1.0e6` in
`make_forge_blueprints.py` and rerun it (the workplan does not override the rate).

**Site check (done on the smoke run's child grid):** the nearest rho point to 36.8 N, -122.0 E
(2.1 km away) is wet with h = 72 m (SRTM15 has 519 m there: roms-tools smooths the canyon away
at 4 km); its 5x5 neighbourhood is all wet, 32-153 m, shoaling eastwards. Forge does not check
the site itself: roms-tools builds the `CDRForcing` without a grid ("Grid not provided: cannot
verify whether the specified lat/lon/depth location is within the domain or on land"), and with
`cdr_frc.relocate_to_wet_pts` true ROMS would relocate a dry release at run time.

## What to look at afterwards

Paths below are under `$CSTAR_DATA_HOME/workplan_runs/ccs-smoke/` (`cstar env show` prints `CSTAR_DATA_HOME`; `~/cstar` on a workstation).

- `cstar run status ccs-smoke`: eight steps done.
- `tasks/forge_child/work/` (the persisted `*.ovrd.yaml` blueprint) shows the overridden window, tiling, CDR output cadence and `cdr.cdr_forcing.releases[0].times`; `tasks/forge_child/input_data/` has `ccs-child_grid.nc` and the CDR forcing, but no initial-conditions or boundary file.
- `tasks/parent/output/`: `output_rst.<ts>.nc` (daily restarts, the first one day after the start) and `child_bry.<ts>.nc` (the nesting extraction, hourly, one file a day, the first stamped 01:00 of the start day).
- `tasks/nest_ic/output/ic_from_parent_rst.<child start>.nc`.
- `tasks/child/output/`: `output_uscl.<ts>.nc` (hourly, 24 records per file), `output_cdr.<ts>.nc` (`ALK_source` is the release) and daily restarts.
- `tasks/upscale/output/upscaled_cdr.nc`: CDF-5, one profile per child boundary column (286) with 20 levels (the child's `s_rho`) and two tracers, hourly, `cdr_time` strictly increasing.
- `tasks/parent_up/output/`: `output_cdr.<ts>.nc` with the upscaled ALK entering the parent along the child's boundary; compare its ALK/DIC anomaly and air-sea CO2 flux with the child's.
- `tasks/parent_up/work/` shows `cdr_forcing.data[0].location` resolved to `tasks/upscale/output/upscaled_cdr.nc`.
- In every ROMS log (`tasks/<step>/logs/<step>.out`): `started time-stepping`, no `ERROR`, `BLOWUP`, `Ran out of time records` or `past current time`. The `[ERROR] ... An error occurred while refreshing ...git` line about a shared clone is a transient network failure, not a model error.

## Known issues

- **Rivers: the bundled DAI rivers break on roms-tools 5.1.1; GloFAS is used instead (the committed default).** With the bundled `glorys-era5-glodap-woa` rivers (DAI, `include_bgc` with RIVR2O, `convert_to_climatology: if_any_missing`) DAI has gaps, so roms-tools converts the discharge to a climatology and, because RIVR2O needs calendar times, `expand_monthly_climatology_time_axis` writes it on the 15ths that lie *inside* the run window (`roms_tools/setup/utils.py`, `calendar_midmonth_dates`; a window with no 15th gets one record clamped into it). ucla-roms needs a record at or before the current time and another strictly after it (`find_new_record`, `src/roms_read_write.F90`), so no window works: a 4-day window from 2010-01-01 died with "First available forcing record is past current time for var: river_volume" (one record at the window end, smoke attempt a2) and one from 2010-01-15 with "Ran out of time records for river_volume" (one record at the start, attempt a3). Rivers without BGC are refused by forge (`_check_river_tracer_floor`: 2 tracers < 34). The smoke runs therefore used `convert_to_climatology: never` with DAI, which writes the real monthly DAI records around the window (2009-12-15, 2010-01-15, 2010-02-15 for the smoke) and lets ucla-roms start; 33 of the 102 smoke discharge values are 0 (roms-tools: "NaNs detected in 'river_volume' and set to zero"). The committed files now use **GloFAS v4** discharge (`source.name: GLOFAS`, daily, 1979-2024, no gaps in 2010) with the RIVR2O concentrations precomputed per station on the same file (roms-tools picks `discharge_accounting: total_discharge` for a GloFAS source, so `bgc_source` stays `{name: RIVR2O}` and no RIVR2O path is read); `convert_to_climatology` stays `never`, so the file holds real daily records and does not depend on the 15ths. On Anvil the shared GLOFAS file has 4579 stations inside lon -145..-115, lat 18..58 (Columbia, Fraser, Sacramento, Klamath among the largest), no NaN in 2010; how many roms-tools keeps on the parent and child grids (50 km coastal snap, at least 1 m3/s, overlapping mouths merged) is only known once forge has run. `--rivers dai` (with the default `--river-climatology never`) restores the DAI source of the smoke runs, which needs no GLOFAS file; `--river-climatology if_any_missing` restores the bundled behaviour and does not run. Whether the river choice is acceptable for the science is for Ben.
- **Pressure fluxes: ucla-roms crashes writing them with PIO (workaround in the files; seen on 0.10.1, still applies on main at `453189ed`).** `nesting_include_pressure_fluxes: true` (Ben's setting) makes the parent's nesting extraction write `up`/`vp`; `src/extract_data.F90:1143` sets `pio_gtype = pio_bnd // '2uc'` for the 1-D variable `up`, while `pio_ncwrite1` only has the `1xx` types, so the first step aborts with "no PIO decomposition for pio_gtype='s2uc' while writing 'up_south'" (attempts a5 and a6; `vp` at line 1229 correctly uses `'1vc'`); the same two lines are unchanged on main at `453189ed` (checked in the source, not run), so the workaround stays. The parent blueprint therefore has `nesting_include_pressure_fluxes: false`; the child's `sponge_tune` (`ub_tune` false) does not use them. `--pressure-fluxes on` restores Ben's setting once ucla-roms is fixed.
- **Resolution.** Ben's blueprint name says 12 km, but the grid is 2688 km / 168 = 16.0 km by 5280 km / 336 = 15.7 km; the child is 4.0 km by 3.93 km (4:1). The bundled `ccs-12km` DomainSpec is a different (224x440x60, 12 km) grid and is not used.
- **`center_lon` of the child.** Ben's file has 237.494; it is written here as -122.506 (the same point). roms-tools wraps either form to [0, 360] (`Grid._create_grid_ds`, and `align_grids` wraps both grids with `wrap_longitudes`), and the CDR release longitude is normalised on use, so neither forge nor `align_grids` would have mishandled the 0-360 form; the value only appears as the `center_lon` attribute of the grid file.
- **Forge window vs `ntimes`.** `model_settings.time_stepping.ntimes` in a forge blueprint is computed from the window when the blueprint is built and is not re-derived when the workplan overrides `run`. It only reaches the `namelist.nml` forge writes; C-Star sets `ntimes` from `runtime_params` and `dt` when the ROMS step runs (`cstar/roms/simulation.py`), so it is harmless.
- **Two forge dependencies.** `forge_child` and `forge_parent_up` wait for `forge_parent` so any cold-cache downloads (missing source data, the land-mask shapefiles) happen once; a cold cache with three simultaneous forge steps is the race fixed for the e2e test in #761. Drop the two `depends_on` lines on a warm cache.
- **Legacy source-data link (fixed alongside these examples).** With `~/cstar` and `~/cstar-forge-data` symlinks into different directories (tanagra) the adoption of `cstar-forge-data/source-data` used to create a dangling relative link and the first forge run died with `FileExistsError`; from 16e4556d it links between resolved paths (`~/cstar/source-data -> ../../../cstar-forge-data/source-data` on tanagra). Older checkouts: `ln -s /mnt/cooltub/cw/cstar-forge-data/source-data ~/cstar/source-data` first.
- **parent_up needs `cdr_frc.relocate_to_wet_pts: true` (set in the file).** The resolver's `upscaled` mode forces it to false (the original hand-made example had true), but the upscaler's 286 profiles sit at the child's boundary points, 51 of which fall on parent land cells: ucla-roms then stops with "CDR release N is on land" (attempt a7). The blueprint overrides it to true; the resolver should not force false. Still needed on ucla-roms main at `453189ed`: `src/cdr_frc.F90` still aborts with "CDR release N is on land" when `cdr_relocate_to_wet_pts` is false (checked in the source, not run).
- **`cdr_frc.nz_chd` against ucla-roms main (checked in the source, not run).** From #390 (`453189ed`) a nonzero `cdr_nz_chd` that differs from the number of `s_rho` levels in the depth-profile CDR file aborts ("does not match the N layers (s_rho)"); 0 means read it from the file. That check runs only when `cdr_forcing_depth_profiles` is true, i.e. in `parent_up`: its `nz_chd: 20` equals the child's 20 levels, which is the `s_rho` length of the upscaler's `upscaled_cdr.nc`, so it passes; the value is left at 20. `forge_child` keeps the resolver's default `nz_chd: 50`, which is read only for depth-profile forcing and so is unused there (its release is parameterized). The upscaler's file has no `cdr_trc_name`, which main still accepts for exactly two tracer columns, applied to MARBL's ALK and DIC.
- **`workplan check` with missing variables** prints "is valid" before "Resolution problems" (exit code 1); read the second line.

## Not covered

The month, six-month and full-run windows (none run yet), more than 16 ranks for the parent (no gain measured), the `ucla-roms` pressure-flux extraction (crashes, see above), the climatological-river option, time splitting, a second CDR site or
release type, DIC or volume releases, `cdr_lite` mode, upscaling through more than one
nest level, a run with the GloFAS rivers or with ucla-roms main (the Anvil runs are the first).
