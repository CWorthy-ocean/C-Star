# CCS parent on a Mac: how long does a year take on 2x2 ranks?

A two-step workplan (`forge_parent` -> `parent`) that runs Ben's California Current
parent grid (168 x 336 x 20, ~16 km, MARBL BGC, `dt` 900 s) for 10 simulated days on
4 MPI ranks (2 x 2) of a laptop, and measures what it costs: forge time including the
GLORYS download, ROMS build time, ROMS wall time per step and per simulated day.
The table at the end extrapolates that to 6 months and a year.

It is a **cost benchmark**, not the CCS science case. It has no nesting and no CDR
(`cdr.mode: none`, no `grid_kwargs_child`), and its forcing is whatever a laptop user can
get without staging files by hand, so it differs from the CCS example in `../`:

| | `../` (CCS example) | this benchmark |
|---|---|---|
| Ocean state | GLORYS, GLODAP/WOA BGC, UNIFIED BGC | GLORYS regional (downloaded by forge), UNIFIED BGC |
| Surface | ERA5 + UNIFIED BGC + MBL_co2 + WOA restoring | ERA5 (streamed) + UNIFIED BGC |
| Tides, rivers | TPXO, GloFAS (with RIVR2O) | none |
| Topography | SRTM15 | ETOPO5 (built into roms-tools; SRTM15 is a 6 GB download) |
| Ranks | 4 x 4 | 2 x 2 |

ROMS cost is dominated by grid cells x steps x tracers, so the forcing choice barely moves
the ROMS numbers below. Tides, rivers and the 4 km child of the real case add cost on top
(see the last section).

## Files

| File | What |
|---|---|
| `make_forge_blueprint.py` | Writes `forge_parent_mac.yaml` from the bundled catalog (`roms-marbl-0.10-default`, `glorys-era5-unified` trimmed, `daily-restarts`). Optional: the committed YAML is the artifact. Edit `START`/`END` there to benchmark another length. |
| `forge_parent_mac.yaml` | The parent: Ben's grid kwargs, open north/south/west, 2 x 2 partitioning, 2010-01-01 to 01-11 (960 steps), ucla-roms 0.10.1 with PIO, daily restarts, nothing else written. Passes `cstar blueprint check`. |
| `workplan.yaml` | `forge_parent` (forge) and `parent` (roms_marbl, `from_step`), local launcher. |

## Prerequisites

- A source checkout of C-Star, its environment installed and *activated*
  (`conda activate cstar-env`; the ROMS build reads `CONDA_PREFIX`).
- GNU `timeout` (the workplan sets a local `max_walltime`): `conda install coreutils` or
  `brew install coreutils`.
- Copernicus Marine credentials, once: `copernicusmarine login` (they are stored in
  `~/.copernicusmarine/`). Forge downloads one regional GLORYS file per day from 2009-12-31
  to 2010-01-12: 13 files, 2.6 GB.
- UNIFIED BGC in the source-data cache (`cstar forge show-paths`;
  `UNIFIED_BGC/BGCdataset_v2_1.nc`, 9.8 GB), or forge downloads it from Google Drive.
- About 12 GB free for the run: inputs 0.6 GB, restarts 7.2 GB, GLORYS cache 2.6 GB.

## Run

From this directory, with the environment active:

```bash
cstar workplan check workplan.yaml
cstar workplan run   workplan.yaml --run-id ccs-mac-a1
cstar run status ccs-mac-a1
```

The run directory is `~/cstar/workplan_runs/ccs-mac-a1` (`CSTAR_DATA_HOME/workplan_runs/`).
Time-stepping progress is in `tasks/parent/logs/parent.out`; ucla-roms prints its own
timing breakdown ("Region timers", `MPI_run_time`) at the end of that log.

## Measured (10-core Apple-silicon laptop, 16 GB, otherwise idle, 2010-01-01 to 01-11)

| Step | Wall time | Of which |
|---|---|---|
| `forge_parent` | 24 min 46 s | GLORYS download 11 min (13 files, ~50 s each), grid 1 s, initial conditions 25 s, surface forcing (ERA5 streamed over the ~60 x 50 degree domain) 12 min 50 s, boundary forcing 15 s |
| `parent` | 13 min 57 s | clone, build and input staging 46 s (ROMS, MARBL and PIO were already cached), ROMS 13 min 9 s |
| total | 38 min 50 s | |

ROMS, from its own `MPI_run_time`: **789 s for 960 steps = 0.82 s per step = 79 s per
simulated day = 45.6 simulated days per wall hour** (restart writes included: 20 restart
records of ~0.36 GB, 3% of the step; MARBL 23%, halo exchange 24%, `step3d_t` 39%). The
rate is flat over the run (restart-to-restart gaps 78 to 79 s per simulated day). The log's
per-step lines are written in batches, so only their count, not their timestamps, gives a
rate.

Memory, from `ps` samples: the four ROMS ranks took ~2.5 GB resident in total (~640 MB
each); the forge process was ~2 GB. Rank 0's CPU time was 1731 s over 789 s of wall time, so
each rank kept about two cores busy (probably MPI progress polling; not checked) and the
run used ~9 of the 10 cores: expect it to be slower on fewer cores.

## Extrapolation (2 x 2, this machine, linear in steps)

| Simulated time | Steps | ROMS wall | Restarts, one per day (0.72 GB) |
|---|---|---|---|
| 10 days (measured) | 960 | 13.2 min | 7.2 GB |
| 30 days | 2,880 | 39 min | 22 GB |
| **6 months** (182.5 d) | 17,520 | **4.0 h** | 131 GB |
| **1 year** (365 d) | 35,040 | **8.0 h** | 263 GB |

- Set a coarser `output_period_rst` (e.g. 10 days) for long runs: the restart pair is 0.72 GB.
- Forge scales with the window too: ~2.5 min per simulated day here (GLORYS ~50 s, ERA5 ~75 s),
  i.e. very roughly 7.5 h of forge for 6 months and 15 h for a year if the download and the
  ERA5 stream stay linear. Generate and run in chunks for anything long.
- The real CCS case also has tides, rivers and the 4 km child (70 x 140 x 20, `dt` 240 s).
  By cells x steps the child costs ~0.65 of the parent per simulated day (0.17 x 3.75), so
  the nested pair is ~1.65x the parent alone, and `parent_up` is a second parent run on top.
  Those are estimates, not measurements; the CCS example's 4 x 4 ranks scale differently.

## Known issues

- Forge's log has no per-stage timestamps (only the stage headers and dask progress bars);
  the stage times above come from the input files' modification times.
- Forge's per-day GLORYS subsets are cached under `GLORYS_REGIONAL/` by grid name
  (`ccs-mac-parent`), so a longer window reuses the first 13 files.
