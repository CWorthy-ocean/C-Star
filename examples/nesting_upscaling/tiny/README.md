# Tiny nesting + upscaling CDR example (laptop)

A two-level nested ROMS-MARBL run with one alkalinity (ALK) release in the child,
upscaled back to the parent grid, end to end through `cstar workplan run`. Eight
steps, ucla-roms main @453189ed with ParallelIO, about 24x24x10 cells per grid, 4 MPI ranks
each. It needs no account and no data you must stage yourself: the three source
files are about 40 MB of pinned test data. The wall clock goes to the three ROMS runs
(about 8 minutes in total on a 10-core laptop; see "Run").

```
forge_parent  forge_child  forge_parent_up          (forge, ~5 s each; the other two wait for forge_parent)
     |             |              |
   parent ---> nest_ic ---> child ---> upscale ---> parent_up
 (400 km)     (restart ->  (100 km,   (_uscl ->    (parent grid again,
 extracts     child IC)    ALK in)    CDR forcing)  CDR forcing on)
 child_bry
```

## Files

| File | What |
|---|---|
| `fetch_data.py` | Downloads the three pinned source files (GLORYS, ERA5, UNIFIED) through C-Star's integration-test data registry and prints the directory holding them. Needs a source checkout of C-Star (the registry is not in the wheel). |
| `make_forge_blueprints.py` | Regenerates the three forge blueprints from the bundled catalog. Optional: the committed YAML is the artifact. |
| `forge_parent.yaml` | Parent: 24x24x10 over 400 km centred at (-7.5, 60) west of Scotland, `dt` 60 s, 2012-01-01T12 to 01-03T07, restarts every 6 h; `grid_kwargs_child` + `metadata_child` switch on the hourly boundary extraction for the child. |
| `forge_child.yaml` | Child: 24x24x10 over 100 km centred at (-6.5, 60.4), `dt` 30 s, 2012-01-02T06 to 01-03T06; `grid_kwargs_parent` makes forge emit no boundary forcing and no initial-condition source; one `tracer_perturbation` ALK release (`cdr.mode: yaml`); `upscale_output` and `cdr_output` on. |
| `forge_parent_up.yaml` | The parent grid again, no child, `cdr.mode: upscaled` (CDR_FORCING compiled, forcing file supplied by the workplan), `cdr_output` on, `nz_chd: 10` to match the child's levels. |
| `workplan.yaml` | The eight steps: `forge_parent`, `forge_child`, `forge_parent_up`, `parent`, `nest_ic`, `child`, `upscale`, `parent_up`. |

All three blueprints use `roms-marbl-0.10-default` (MARBL `marbl0.45.0-max-it-10`, PIO
`2.7.1-fork`) with ucla-roms pinned to main at `453189ed` (`code.roms.commit`, set by
`make_forge_blueprints.py`) instead of the spec's 0.10.1: from #390 on, `parent_up`'s
depth-profile CDR forcing injects the flux it is given, where 0.10.1 scaled it by about
the ratio of the child's and the parent's column depths. Switch to 0.10.2 once it is
released. The blueprints use the `glorys-era5-unified` forcing
trimmed to what the test data supports: GLORYS regional initial conditions and
boundaries, ERA5 and UNIFIED surface forcing, UNIFIED initial BGC and boundary BGC.
No tides, rivers, MBL_co2 or salinity restoring.

## Prerequisites

- A source checkout of C-Star, with the repo's environment installed editable and
  *activated* (`conda activate cstar-env`): the ROMS build reads `CONDA_PREFIX` for
  MPI and netCDF, and the step scripts call the `cstar` found on `PATH`. The
  environment needs `mpirun`, gfortran, `nccopy` and git; network access for the first
  run (ROMS, MARBL and PIO are cloned and built; the data below is downloaded).
- GNU `timeout`: the workplan sets a local `max_walltime` on every step, which C-Star
  enforces with `timeout`. macOS has none; the repo's `environment-laptop.yml` installs
  `coreutils` for it. In an older environment, `conda install coreutils` (or `brew install
  coreutils`, which provides `gtimeout`).
- The first run downloads the pinned test data (about 40 MB, `fetch_data.py`), and roms-tools
  fetches ETOPO5 (about 19 MB) and the ERA5 radiation-correction file (about 86 MB).

## Run

From this directory, with the environment active:

```bash
cstar workplan check workplan.yaml --var example_data=$(python fetch_data.py)
cstar workplan run   workplan.yaml --var example_data=$(python fetch_data.py) --run-id nest-tiny
cstar run status nest-tiny
```

`fetch_data.py` prints only the directory on stdout, so it is safe inside `$(...)`; its
download progress goes to stderr. The run directory is
`CSTAR_DATA_HOME/workplan_runs/nest-tiny` (`~/cstar/workplan_runs/nest-tiny` by
default); each step's files are under `tasks/<step>/`.

Measured on a 10-core Apple-silicon laptop, source data already downloaded (wall clock,
one run, 8 min 11 s from `workplan run` to the last step done; each step waits a few
seconds for its predecessor's sentinel, so the rows add up to a little less):

| Step | Wall time | Of which |
|---|---|---|
| `forge_parent` | ~9 s | grid, forcing, nesting file |
| `forge_child`, `forge_parent_up` | ~10 s (side by side, once `forge_parent` is done) | grids, forcing, CDR forcing |
| `parent` | 2 min 33 s | ~45 s clone and build, 1 min 48 s ROMS (2580 steps of 60 s on 4 ranks) |
| `nest_ic` | 5 s | |
| `child` | 2 min 51 s | ~45 s build (not the parent's build: nesting off, CDR and UPSCALING on), 2 min 6 s ROMS (2880 steps of 30 s) |
| `upscale` | 1 s | |
| `parent_up` | 1 min 58 s | ~45 s build, 1 min 13 s ROMS (1380 steps of 60 s) |

The forge rows are from a later run, after `forge_child` and `forge_parent_up` were made to wait for `forge_parent` (see Known issues): the forge phase takes about 19 s instead of the 9 s it took with all three side by side, which does not delay the ROMS runs, because `parent` only waits for `forge_parent`.

The first run on this machine took 12 min up to the failure the upscaler fix removed
(`parent` 5 min 54 s, `child` 4 min 40 s) while another ROMS job shared the laptop:
the ROMS time-stepping is what varies with load. A first run on a machine that has never
built ucla-roms, MARBL and PIO will spend longer cloning and compiling.

If a step fails, fix the cause and run again with a new `--run-id`. (A changed
workplan under an old run-id is refused.)

## What each step exercises

- **forge_parent / forge_child / forge_parent_up (forge)**: three blueprints with placeholder source paths replaced through `blueprint_overrides` and a runtime variable (`{{example_data}}`); nesting on the parent (`grid_kwargs_child` + `metadata_child` produce `nest-tiny-parent_nesting.nc` and the `extract_data` namelist block); `grid_kwargs_parent` on the child (no boundary forcing, no initial-condition source, boundary tides forced off); a `cdr.mode: yaml` forcing with one `tracer_perturbation` release; `cdr.mode: upscaled` with a placeholder `cdr_forcing` the workplan replaces; CDF-5 inputs for PIO.
- **parent (roms_marbl)**: a deferred blueprint (`from_step`); with `extract_data` on, ucla-roms writes `child_bry.<ts>.nc` (hourly, six records per file) next to the six-hourly restarts `output_rst.<ts>.nc`.
- **nest_ic**: an inline blueprint (every field in `blueprint_overrides`) that regrids the parent's restart at the child's start onto the child grid, `ic_from_parent_rst.<ts>.nc`, converted to CDF-5. The restart is named by hand, `parent_rst: ...output_rst.20120102060000.nc`.
- **child (roms_marbl)**: two directives. `continue-from: {step: nest_ic}` takes the initial state from `ic_from_parent_rst.<ts>.nc` (and the start date from its timestamp); `nest-from: {step: parent}` supplies the open-boundary forcing from the parent's `child_bry.*.nc`. `UPSCALING` writes `output_uscl.<ts>.nc`, the child's boundary fluxes of ALK and DIC, and the `_cdr` stream writes the release's source.
- **upscale (upscaler)**: an inline blueprint pointing `uscl_file_location` at the child's `output/`; it turns the five `_uscl` files into `upscaled_cdr.nc`, a CDF-5 file of boundary flux profiles (`cdr_trcflx_profile`, 104 profiles x 10 levels x ALK/DIC).
- **parent_up (roms_marbl)**: the parent grid with `CDR_FORCING` in the build and the flux profiles as forcing: the override `cdr_forcing.data[0].location` points at `upscaled_cdr.nc`; `continue-from: {step: parent, timestamp: ...}` picks the parent's restart at the child's start (not the latest); `end_date` is one record short of the child's end (2012-01-03T05; a precaution, not tested without it).

## What to look at afterwards

- `cstar run status nest-tiny` shows eight steps done.
- `tasks/parent/output/`: `child_bry.20120101130000.nc` ... `child_bry.20120103070000.nc` (eight files) and `output_rst.20120101180000.nc` ... `output_rst.20120103060000.nc` (seven, one record each).
- `tasks/nest_ic/output/ic_from_parent_rst.20120102060000.nc`: the child's initial state.
- `tasks/child/output/`: `output_uscl.*.nc` (five files, 06:00 on 2012-01-02 to 06:00 on 01-03), `output_cdr.*.nc` and four restarts. In the first `_cdr` file `ALK_source` is non-zero around the release (maximum ~9e5).
- `tasks/upscale/output/upscaled_cdr.nc`: `ncdump -k` prints `cdf5`; `cdr_trcflx_profile[:, :, 0, :]` (ALK) is finite and non-zero, only on the east boundary (the 104 profiles are the north, east, south and west columns in turn; only the east block is non-zero). Summed over the boundary it is 0 at the first record, slightly negative for the first ~4 h (before the plume arrives), peaks near 1.8e5 mmol/s about 15 h after the start and settles near 1.5e5. `cdr_time` is strictly increasing: 25 hourly records, the first at the child's start (2012-01-02T06; days since 2000-01-01).
- `tasks/parent_up/output/`: three restarts (`output_rst.20120102120000.nc`, `...180000`, `...20120103000000`) and four `_cdr` files covering 2012-01-02T07 to 01-03T05. Every field is finite and each `_cdr` file's `ALK_source` has a positive maximum (3e3, 4.4e4, 5.0e4, 4.2e4), but individual cells are negative in every file (minimum about -6.8e3 in the first) and the first file's domain sum is negative (-2.6e4 against +4.7e5, +9.9e5 and +7.4e5 in the others): the upscaled boundary flux is two-signed, as the anomaly crosses the child's boundary in both directions. So `cdr_metrics.py` needs `--clip-negative-sources` for `parent_up` (see the overview README). Against the parent's own restart at the same time (no CDR), `ALK` of `parent_up` differs only near the child's east edge and downstream of it: 15, 24 and 36 cells differ at 12:00, 18:00 and 00:00, most of them outside the child's footprint (26 of 36 at 00:00), with the largest difference, 0.0035, 0.060 and 0.126 mmol/m3, just east of the child's east edge; `temp` and `salt` are identical.
- `tasks/*/logs/*.out`: `started time-stepping` and no ROMS `ERROR [`, `BLOWUP`, `MPI_Abort` or `Ran out of time records` in the three ROMS logs (C-Star's own `[ERROR]` lines, such as a transient failed `git` refresh of a shared clone, are not model errors).

## Known issues

- The two `depends_on: [forge_parent]` lines on `forge_child` and `forge_parent_up` are there for a cold cache: each forge step builds a grid, and the land mask of the first one downloads and unzips the Natural Earth shapefiles into a directory shared by all three, which is not safe to do twice at once (the CCS workplan does the same, #761). On a warm cache they can go and the three forge steps then run side by side.
- Forge logs `forge_blueprint integrity check FAILED ... hand-edited` for every forge step: replacing the source paths through `blueprint_overrides` changes the content the recorded hash covers. It is only a warning.
- `parent_up`'s forge blueprint has the *parent's* window (2012-01-01T12 to 01-03T06) although the run starts 18 h later: forge builds an initial-condition file for every domain without a parent grid, and roms-tools needs a GLORYS record at or within 24 h after its start, which the test data has only at 2012-01-01T12. `continue-from` moves the run start to the restart's time, so that file is unused.
- `upscaler` writes `cdr_time` in days since 2000-01-01 whatever the model reference date, so the example keeps `model_reference_date: 2000-01-01`.
- The wizard's "Upscale a nested run" recipe does not generate this workplan: it re-runs the parent's own step (same blueprint, no CDR forcing compiled, no `continue-from` at the child's start) with the `cdr_forcing` override, and has no place for a separate `<parent>_up` forge blueprint.
- With no `timeout` on `PATH` the run is refused up front; with an un-activated environment a ROMS step fails in `assert_single_toolchain_stack` (`MPIHOME ... expanded to an empty string`).

## Findings this example surfaced

Building it found the upscaler's file ordering bug (the `_uscl` files are now concatenated in time order and a non-monotonic `ocean_time` is rejected), the false forge "integrity check FAILED" warning, the initial-condition requirement behind `forge_parent_up`'s window, and the wizard recipe gap above.

## Not covered

Tides (the regional TPXO files cannot be given as a path), rivers, MBL_co2, salinity
restoring, a third nesting level (the workplan generalises: `nest_ic`/`child` and
`upscale`/`<level>_up` repeat per level), time-chunked runs, the wizard's "Upscale a
nested run" recipe (see Known issues), SLURM (change `compute_environment` and the
`compute_overrides` keys to `slurm`).
