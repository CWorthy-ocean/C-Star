# Blueprint settings sidecar (`settings_B_{name}.yaml`)

Next to every emitted [ROMS-MARBL blueprint](reference-roms-marbl-blueprint.md),
the executor persists a settings sidecar — the fully-resolved model settings,
stored separately so the blueprint itself stays uncluttered by configuration
detail.

Current structure (two top-level keys):

```yaml
compile_time:
  cppdefs: { ... }        # the ONLY compile-time section
run_time:
  title: ...
  output_root_name: ...
  reference_date_settings: { ... }
  param: { ... }           # namelist sections sit DIRECTLY under run_time —
  tides: { ... }           # there is no intermediate "roms.in:" grouping key
  marbl_bgc: { ... }
  # ... every other resolved model_settings section ...
```

The split rule is simple: `cppdefs` is the sole compile-time section; every
other section of the resolved `model_settings` is a run-time (namelist)
section. Generate a current example by processing the bundled toy domain
(`cstar forge run docs/forge-blueprint-example.wio-toy.yaml`) and inspecting
`settings_B_*.yaml` in the working directory.

(Sidecars under `legacy/blueprints/` predate the namelist refactor — they nest
run-time sections under a `roms.in:` key and put ~15 sections under
`compile_time`; neither is true of current output.)
