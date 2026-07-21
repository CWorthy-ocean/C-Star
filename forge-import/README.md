# C-STAR Forge

A utility for generating new regional oceanographic modeling domains and creating reproducible [C-Star](https://c-star.readthedocs.io) workflows through blueprint descriptors.

[![Run Tests](https://github.com/CWorthy-ocean/cstar-forge/actions/workflows/tests.yaml/badge.svg)](https://github.com/CWorthy-ocean/cstar-forge/actions/workflows/tests.yaml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/CWorthy-ocean/cstar-forge/graph/badge.svg)](https://codecov.io/gh/CWorthy-ocean/cstar-forge)

## Using C-STAR Forge
To learn how to use C-STAR Forge, check out the [documentation](https://cworthy-ocean.github.io/cstar-forge/overview/).

## Building a ForgeBlueprint

Interactive front-ends for assembling a `ForgeBlueprint` (the authoritative input to
processing — see `docs/forge-blueprint-inventory.md`). Both are thin shells over
`cstar_forge.forge_blueprint_resolve.build_forge_blueprint`; all resolution/validation lives
in the resolver.

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

## The C-Star Ocean Network
```{image} workflows/visualization/assets/csforge.png
:alt: C-STAR Forge Logo
:class: csforge-logo
:align: center
```

```{warning}
This project is still in an early phase of development.

You are welcome to try out using the package, but be aware that development is ongoing and we cannot yet guarantee backwards compatibility. 
```
