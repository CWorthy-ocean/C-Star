# C-STAR Forge

A utility for generating new regional oceanographic modeling domains and creating reproducible [C-Star](https://c-star.readthedocs.io) workflows through blueprint descriptors.

[![Run Tests](https://github.com/CWorthy-ocean/cstar-forge/actions/workflows/tests.yaml/badge.svg)](https://github.com/CWorthy-ocean/cstar-forge/actions/workflows/tests.yaml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/CWorthy-ocean/cstar-forge/graph/badge.svg)](https://codecov.io/gh/CWorthy-ocean/cstar-forge)

## How it works

C-STAR Forge takes you from "I want a regional ROMS-MARBL domain here" to a running
simulation in three steps:

1. **Build a forge blueprint** — an interactive **wizard** (in Jupyter, or as a
   standalone Voilà web app) helps you assemble a `ForgeBlueprint` from catalog
   **specs** (model, domain, forcing, output). The blueprint is a single YAML file
   that fully describes what to generate.
2. **Process the blueprint** — the **forge application** consumes the forge blueprint
   and produces everything needed to run: grids, initial conditions, surface/boundary
   forcing, rivers, tides, rendered model settings, and a **ROMS-MARBL blueprint**
   describing the resulting setup:

   ```bash
   python -m cstar_forge.run path/to/forge_blueprint.yaml
   ```

3. **Run the simulation** — [C-Star](https://c-star.readthedocs.io) consumes the
   ROMS-MARBL blueprint to build and execute the actual ROMS-MARBL simulation.

To learn more, check out the [documentation](https://cworthy-ocean.github.io/cstar-forge/overview/).

## Installation

Until packaged releases are available, install from source with the setup script
(creates the `cstar-forge-v0` conda environment, installs the package in editable
mode, and registers a Jupyter kernel):

```bash
git clone https://github.com/CWorthy-ocean/cstar-forge.git
cd cstar-forge
./dev-setup.sh
conda activate cstar-forge-v0   # or: micromamba activate cstar-forge-v0
```

See [docs/installation.md](docs/installation.md) for details, options, and
verification steps.

## Building a ForgeBlueprint

Two interactive front-ends assemble a `ForgeBlueprint` (the authoritative input to
processing). Both are thin shells over
`cstar_forge.forge_blueprint_resolve.build_forge_blueprint`; all resolution/validation
lives in the resolver. See `docs/developer-guide.md` for the architecture.

### In a Jupyter notebook

- **`cstar_forge/forge-blueprint-wizard.ipynb`** — run the wizard inline, then inspect `wiz.config`.
  Good for exploring / scripting.

```python
from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizardApp
app = ForgeBlueprintWizardApp()
app.display()
# ... build & review ...  then:  cfg = app.inner.config
```

`ForgeBlueprintWizardApp` adds a catalog-location bar above the wizard: it auto-loads
the bundled in-repo catalog (`catalog/`) by default, or you can enter a different
local path, `"local"`, a GitHub URL, or an http URL and click **Reload catalog** to
rebuild the wizard against it.

### As a standalone web app (Voilà)

- **`cstar_forge/forge-blueprint-wizard-app.ipynb`** — a code-free "form" view, served by
  [Voilà](https://voila.readthedocs.io/) (pure Python, `pip install voila`, no admin).

```bash
./run-wizard-app.sh        # opens http://localhost:8866
```

On an HPC **login node** (no browser there), bind locally and SSH-forward from your
laptop:

```bash
# on the login node:
./run-wizard-app.sh --no-browser
# on your laptop:
ssh -N -L 8866:localhost:8866 <user>@<login-node>
# then open http://localhost:8866 locally
```

Use the **Download** link to save `forge_blueprint.yaml` to your machine (works in the
browser without server file access), or **Save to disk** to write it on the host
running Voilà. Take the file to the machine of your choice to run the processing
step.

> Portability note: the builder needs only `pydantic` + `pyyaml` (and `ipywidgets`/
> `voila` for the UI) — no ROMS / C-Star / roms_tools. The one exception is the
> optional **Compute dt (CFL)** button, which builds a grid via `roms_tools`; leave
> it and enter `dt` directly to stay fully lightweight.

## Processing a ForgeBlueprint

On the machine where the input files should be generated (laptop or HPC):

```bash
python -m cstar_forge.run path/to/forge_blueprint.yaml
```

Run `python -m cstar_forge.run --help` for options (partial runs, host inspection,
verbosity, dask controls). Outputs — input NetCDF files, rendered model settings, and
the ROMS-MARBL blueprint — are written under the blueprint's working directory, and
the emitted ROMS-MARBL blueprint is then handed to C-Star to run the simulation.

```{image} docs/assets/csforge.png
:alt: C-STAR Forge Logo
:class: csforge-logo
:align: center
```

```{warning}
This project is still in an early phase of development.

You are welcome to try out using the package, but be aware that development is ongoing and we cannot yet guarantee backwards compatibility. 
```
