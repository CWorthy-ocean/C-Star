# ROMS-MARBL blueprint (`B_{name}.yaml`)

Processing a forge blueprint (`cstar forge run`, or `python -m cstar_forge.run`)
emits a **ROMS-MARBL blueprint** — the YAML handoff that
[C-Star](https://c-star.readthedocs.io) builds and runs (`cstar blueprint run
B_{name}.yaml`). It is written to the blueprint's working directory alongside a
[settings sidecar](reference-roms-marbl-blueprint-settings.md).

What a current blueprint contains:

- **`run_time` code payload**: `namelist.nml` (the single generated namelist)
  plus the model's static run-time files (today just `marbl_in`).
- **`compile_time` code payload**: `cppdefs.opt` — the only rendered
  compile-time file; all other former `*.opt` outputs were absorbed into the
  namelist.
- **Input datasets**: one `Resource` entry (location + partitioned flag) per
  generated NetCDF input — grid, initial conditions, surface/boundary forcing,
  tides, rivers, CDR.
- **`model_params` / `runtime_params`**: `time_step`, `start_date`/`end_date`,
  and the blueprint `working_dir` (there is no `output_dir` — it was a
  pre-2.0.0 field superseded by `working_dir`).

To see a complete, current example, process the bundled toy domain and inspect
the result:

```bash
cstar forge run docs/forge-blueprint-example.wio-toy.yaml
# emits ~/cstar-forge-run/cson_roms-marbl_v0.1_wio-toy_10procs/B_*.yaml
```

(Blueprints under `legacy/blueprints/` predate the namelist refactor — they
list ~15 `*.opt` compile-time files and a `roms.in`, none of which are emitted
today.)
