# Getting Started

This page takes you from nothing to a running toy simulation on a laptop or
workstation: install, build a small forge blueprint with the wizard, process
it, and hand the result to C-Star to run.

Installing on an HPC system, or setting up a development environment with an
editable checkout? Use the [Installation](installation.md) page instead, then
rejoin this guide at [Register for data access](#register-for-data-access).

## Install

The easiest way to get our entire dependency stack is to install `cstar-forge` and `cstar-ocean-standalone` into a fresh conda (or mamba) environment:

```bash
conda create -n cstar-forge-env -c conda-forge cstar-forge cstar-ocean-standalone
conda activate cstar-forge-env
```

Verify:

```bash
cstar --version
python -c "import cstar_forge; print('cstar_forge OK')"
```

```{note}
`cstar-ocean-standalone` installs its own conda-based compiler and MPI stack; if you are on a HPC or machine with its own bespoke compilers and MPI setup, use the regular `cstar-ocean` package.
```

(register-for-data-access)=
## Register for data access

Forge downloads forcing data from open datasets, two of which need a one-time
free registration — full instructions on the
[Registering with data sources](data-access.md) page. For this walkthrough:

- **GLORYS (Copernicus Marine) is required**: [sign up](data-access.md#data-access-glorys),
  then run `copernicusmarine login` once so Forge can download ocean-state data.
- **TPXO is not needed for this example** — it's required only for domains with
  tidal forcing. When you get there, see the
  [TPXO instructions](data-access.md#data-access-tpxo).

## Build a forge blueprint with the wizard

```bash
cstar forge wizard        # serves the wizard at http://localhost:8866
```

In the wizard: pick a model spec, then pick the **`wio-toy`** domain from the
catalog — a deliberately tiny (20×20×10) Western Indian Ocean domain that
processes in minutes and exists exactly for first runs like this one. Review
the resolved YAML in the Review pane, then **Save** (or **Download**)
`forge_blueprint.yaml`.

```{tip}
In a hurry? A ready-made wio-toy blueprint ships with the package — locate it
with `python -c "from cstar_forge import default_catalog;
print(default_catalog.forge_blueprint_path('wio-toy-simple'))"` — and a copy
lives in the repo as `docs/forge-blueprint-example.wio-toy.yaml`. You can skip
the wizard entirely and process either directly.
```

```{note}
**Where your work is saved.** Blueprints and workplans you save from the
wizard, and any specs you register to the catalog, land in your own
writable catalog layer at `~/cstar-forge-data/catalog` (`blueprints/`,
`workplans/`, and spec directories) — never inside the installed package.
The bundled examples (like `wio-toy` above) stay visible alongside your own
in the wizard's dropdowns, read-only, marked with a `(bundled)` badge. Set
`CSTAR_FORGE_CATALOG` to point the writable layer elsewhere. Upgrading from
a pre-0.5 install and have blueprints stranded under an old
`site-packages/cstar_forge/catalog/blueprints/`? Just copy those YAML files
into `~/cstar-forge-data/catalog/blueprints/` — they're self-contained.
```

## Process the blueprint

```bash
cstar blueprint run path/to/forge_blueprint.yaml
```

This fetches the source data (GLORYS, ERA5 — expect the first run to spend
most of its time downloading), generates all ROMS input
files, renders the model settings, and emits a **ROMS-MARBL blueprint** under
the blueprint's `working_dir` (for wio-toy:
`~/cstar-forge-run/cson_roms-marbl_v0.1_wio-toy_10procs/`). The final line of
output tells you exactly what to do next:

```text
Blueprint: ~/cstar-forge-run/.../roms_marbl_blueprint.yaml
Run it with:  cstar blueprint run <path>
```

```{tip}
Power-user options (stage selection,
`--clobber`, dask tuning, verbosity) are available via the dedicated
`cstar forge run path/to/forge_blueprint.yaml` entry point (see
`cstar forge run --help`).
```

## Run the simulation

```bash
cstar blueprint run <path-to-roms_marbl_blueprint.yaml>
```

Both steps use the same `cstar blueprint run` command; each blueprint's
`application` field tells C-Star which application processes it. The forge
blueprint from the previous step (`application: forge`) is handled by the
`forge` application that the installed `cstar-forge` package registers via
its entry point. This generated **ROMS-MARBL blueprint**
(`application: roms_marbl`), with all of the inputs needed to execute the
simulation, is handled by the `roms_marbl` application built into C-Star
itself.

C-Star fetches and compiles the model code (using the toolchain installed
above) and executes the simulation; outputs land under the same working
directory. See the [C-Star documentation](https://c-star.readthedocs.io) for more details on run management,
workplans, and analysis.

## Next steps

- Browse the bundled **domain catalog** in the wizard or customize a domain's grid parameters.
- Moving to a cluster? The [Installation](installation.md) page covers HPC
  installs (with the toolchain coming from environment modules instead of
  conda) and reproducible locked environments.
