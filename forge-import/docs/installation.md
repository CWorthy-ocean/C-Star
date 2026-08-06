# Get Started with C-STAR Forge

## Prerequisites

- Python 3.8 or higher
- Git
- Conda, Mamba, or Micromamba (the setup script will automatically install Micromamba if needed)

## Setup

### 1. Fork the Repository

Create a fork of the repository at [https://github.com/CWorthy-ocean/cstar-forge](https://github.com/CWorthy-ocean/cstar-forge).

### 2. Clone Your Fork

In your terminal, clone your forked repository:

```bash
git clone https://github.com/<YOUR_GITHUB_USERNAME>/cstar-forge.git
cd cstar-forge
```
Consider creating a branch to keep your work organized
```bash
git checkout -b <branch-name>
```


:::{note}
**HTTPS vs SSH Access**

The command above uses HTTPS. Alternatively, you can use SSH:

```bash
git clone git@github.com:<YOUR_GITHUB_USERNAME>/cstar-forge.git
cd cstar-forge
```

For more information on HTTPS and SSH access, see the [GitHub documentation on cloning repositories](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).
:::


### 3. Run the Setup Script

Run the setup script:

```bash
./dev-setup.sh
```

**Options:**
- `--clean`: Remove and rebuild the environment if it already exists
- `--batch` or `-f`: Run without user prompts (useful for CI/automation)

**Examples:**
```bash
# Normal setup (will prompt for confirmation)
./dev-setup.sh

# Clean rebuild (removes existing environment first)
./dev-setup.sh --clean

# Automated setup without prompts
./dev-setup.sh --force
```

:::{note}
The `dev-setup.sh` script automates the setup process:
- [Installing Micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html) (if needed)
- [Creating the conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)
- [Installing the `cstar-forge` package in editable mode](https://pip.pypa.io/en/stable/topics/local-project-installs/#editable-installs)
- [Installing `C-Star`](https://c-star.readthedocs.io)
- [Setting up the Jupyter kernel](https://ipython.readthedocs.io/en/stable/install/index.html)
:::

### 4. Verify Installation

To verify that everything is installed correctly:

**a) Activate the environment and test the installation:**

```bash
# Activate the environment (using micromamba or conda)
eval "$(./bin/micromamba shell hook --shell bash)"  # or use conda if micromamba not available
micromamba activate cstar-forge-v0  # or: conda activate cstar-forge-v0

# Test that cstar_forge can be imported
cd workflows
python -c "import cstar_forge; print('✓ cstar_forge works')"
```

**b) Check that the Jupyter kernel is installed:**

```bash
jupyter kernelspec list | grep cstar-forge-v0
```

You should see `cstar-forge-v0` in the list. If not, the kernel installation may have failed.

**c) Check that the package can be imported in Python:**

```python
import cstar_forge

print(f"System detected: {cstar_forge.config.system}")
```

**d) Inspect the configured paths:**

The `show-paths` command displays the detected system and all configured data paths:

```bash
python -m cstar_forge.config show-paths
```

This will show output like:
```
System tag : MacOS
Hostname   : your-hostname

Paths:
  here         -> /path/to/cstar-forge/cstar_forge
  source_data  -> /path/to/source-data
  input_data   -> /path/to/input-data
  scratch      -> /path/to/scratch-directory
  ...
```

## Register for data access

C-STAR Forge facilitates access to a collection of open datasets required to force regional oceanographic models. 
These data are documented in ROMS Tools [here](https://roms-tools.readthedocs.io/en/latest/datasets.html).

Access to most of the data is facilitated automatically. 
- [Sign up for access](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service) to the Copernicus Marine Service 
- [Sign up for access](https://www.tpxo.net/global) to TPXO data


## Launch the blueprint wizard

Once your environment is set up, launch the wizard to build your first forge blueprint.

**Option A — Voilà web app (code-free form):**

```bash
./run-wizard-app.sh        # opens http://localhost:8866
```

On an HPC login node (no browser there), run `./run-wizard-app.sh --no-browser` and
SSH-forward the port from your laptop:
`ssh -N -L 8866:localhost:8866 <user>@<login-node>`, then open
`http://localhost:8866` locally.

**Option B — Jupyter notebook:**

```bash
jupyter lab cstar_forge/forge-blueprint-wizard.ipynb
```

Make sure the kernel is set to `cstar-forge-v0` (change it in the Kernel menu if
needed). Run the cells to display the wizard inline; the wizard object stays
available for inspection and scripting.

In either front-end: pick a model spec, pick (or customize) a domain, review the
resolved YAML in the Review pane, and **Save** (or **Download**) the resulting
`forge_blueprint.yaml`.

## Process the blueprint

Take the saved `forge_blueprint.yaml` to the machine where the input files should be
generated (it can be the same machine) and run the forge application:

```bash
python -m cstar_forge.run path/to/forge_blueprint.yaml
```

This downloads and prepares source data, generates all ROMS input files, renders
model settings, and emits a **ROMS-MARBL blueprint** into the blueprint's working
directory. That ROMS-MARBL blueprint is the handoff to
[C-Star](https://c-star.readthedocs.io), which builds and runs the actual ROMS-MARBL
simulation. See `python -m cstar_forge.run --help` for options.



:::{tip}
SSH keys provide a more secure and convenient way to authenticate with GitHub, eliminating the need to enter your credentials for each push or pull operation. To set up SSH keys for GitHub, see:
- [Generating a new SSH key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
- [Adding a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)
:::


