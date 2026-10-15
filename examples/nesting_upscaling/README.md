# Nesting + upscaling CDR examples

Two-level nested ROMS-MARBL workplans with a carbon dioxide removal (CDR) release
in the child domain, upscaled back to the parent, run end to end with
`cstar workplan run`. Each workplan starts from forge blueprints, so one command
goes from source data to the final upscaled parent run:

```
forge_parent   forge_child   forge_parent_up        forge: generate each domain's inputs
     |              |               |
   parent ----> nest_ic ----> child ----> upscale ----> parent_up
   (writes      (parent       (ALK        (child's      (parent grid rerun from the
   child_bry    restart ->    release;    boundary CDR  parent's restart at the
   boundary     child         writes      fluxes ->     child's start, with the
   data)        initial       _uscl and   parent CDR    upscaled forcing)
                state)        _cdr files) forcing)
```

| Case | Directory | Runs on | Data | Wall clock | Use it for |
|---|---|---|---|---|---|
| Tiny toy | [`tiny/`](tiny/README.md) | a laptop (4 MPI ranks per ROMS step) | about 40 MB of pinned test data; no accounts, nothing to stage | about 8 min on a 10-core Mac | a first end-to-end run; the weekly Tier 3 integration test (`cstar/tests/integration_tests/extended/test_nesting_upscaling_workplan.py`) |
| California Current | [`ccs/`](ccs/README.md) | a workstation or HPC node (up to 64 ranks per step) | real forcing (GLORYS, ERA5, GLODAP + WOA + UNIFIED BGC, TPXO, DAI rivers) staged in the source-data cache | about 8 h on the critical path, about 12 h with margin, at most 20 h, on up to 64 cores | release-scale integration testing and figures (the smoke run passes; the full run is proposed, not yet run); [`ccs/mac_benchmark/`](ccs/mac_benchmark/README.md) times the parent grid at 2x2 on a laptop |

[`cdr_metrics.py`](cdr_metrics.py) computes the roms-tools CDR metrics (uptake
efficiency and CO2 uptake, from the CO2 flux and from the DIC difference) for the
`child` and `parent_up` steps of a finished run, and plots them side by side:

```bash
python cdr_metrics.py --run-dir ~/cstar/workplan_runs/<run-id>
```

The two domains' numbers are different quantities: the child's is uptake inside
the child per unit of ALK released, parent_up's is uptake in the parent per unit
of ALK entering through the child boundary. The script does not add them. The
upscaled forcing carries the anomaly in both directions across the child
boundary, so parent_up has some negative `ALK_source` values that roms-tools
rejects. Pass `--clip-negative-sources` to clip them; the clipped amounts are
logged and saved in the output.

## What these examples rely on

- **Forge steps in a workplan.** A `roms_marbl` step takes the blueprint a forge
  step emits (`blueprint: {from_step: forge_parent}`). Other steps reach the forge
  inputs through `{{root_dir: <forge step>}}/input_data/`.
- **Three forge blueprints per case.** The parent extracts boundary data for the
  child (`grid_kwargs_child`). The child is nested (`grid_kwargs_parent`, so it
  has no boundary forcing of its own), carries the CDR release, and writes the
  upscaling output (`upscale_output.do_upscale`). `parent_up` is the parent grid
  built with `cdr.mode: upscaled`, so ROMS is compiled to read CDR forcing from a
  file. The wizard's "Upscale a nested run" recipe reruns the parent's own build
  instead, and that build ignores the upscaled file.
- **Directives.**
  - `child` uses `continue-from: {step: nest_ic}` for its initial state and
    `nest-from: {step: parent}` for its boundaries.
  - `parent_up` uses `continue-from: {step: parent, timestamp: <child start>}` and
    a `cdr_forcing` override that points at `{{output_dir: upscale}}/upscaled_cdr.nc`.
- **Runtime variables** (`cstar workplan run … --var name=value`) supply
  machine-specific paths (tiny) and the run windows (CCS), so the committed files
  stay portable.
