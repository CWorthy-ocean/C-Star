# Get Started with C-SON Forge

## Prerequisites

- Python 3.8 or higher
- Git
- Conda, Mamba, or Micromamba (the setup script will automatically install Micromamba if needed)

## Setup

### 1. Fork the Repository

Create a fork of the repository at [https://github.com/CWorthy-ocean/cson-forge](https://github.com/CWorthy-ocean/cson-forge).

### 2. Clone Your Fork

In your terminal, clone your forked repository:

```bash
git clone https://github.com/<YOUR_GITHUB_USERNAME>/cson-forge.git
cd cson-forge
```


### 3. Run the Setup Script

The `dev-setup.sh` script automates the entire setup process, including:
- [Installing Micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html) (if needed)
- [Creating the conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)
- [Installing the `cson-forge` package in editable mode](https://pip.pypa.io/en/stable/topics/local-project-installs/#editable-installs)
- [Installing `C-Star`](https://c-star.readthedocs.io)
- [Setting up the Jupyter kernel](https://ipykernel.readthedocs.io/en/stable/user-guide.html#kernel-installation)

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

### 4. Verify Installation

To verify that everything is installed correctly:

**a) Check that the Jupyter kernel is installed:**

```bash
jupyter kernelspec list | grep cson-forge-v0
```

You should see `cson-forge-v0` in the list. If not, the kernel installation may have failed.

**b) Activate the environment and test the installation:**

```bash
# Activate the environment (using micromamba or conda)
eval "$(./bin/micromamba shell hook --shell bash)"  # or use conda if micromamba not available
micromamba activate cson-forge-v0  # or: conda activate cson-forge-v0

# Test that cson_forge can be imported
cd workflows
python -c "import cson_forge; print('✓ cson_forge works')"
```

**c) Check that the package can be imported in Python:**

```python
import cson_forge
print(f"System detected: {cson_forge.config.system}")
```

**d) Inspect the configured paths:**

The `show-paths` command displays the detected system and all configured data paths:

```bash
python -m cson_forge.config show-paths
```

This will show output like:
```
System tag : MacOS
Hostname   : your-hostname

Paths:
  here         -> /path/to/cson-forge/cson_forge
  model_configs -> /path/to/cson-forge/cson_forge/model-configs
  source_data  -> /path/to/source-data
  input_data   -> /path/to/input-data
  run_dir      -> /path/to/run-directory
  ...
```

For JSON output:
```bash
python -m cson_forge.config show-paths --json
```

## Register for data access

CSON Forge facilitates access to a collection of open datasets required to force regional oceanographic models. 
These data are documented in ROMS Tools [here](https://roms-tools.readthedocs.io/en/latest/datasets.html).

Access to most of the data is facilitated automatically. 
- [Sign up for access](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service) to the Copernicus Marine Service 
- [Sign up for access](https://www.tpxo.net/global) to TPXO data


### Notes
:::{note}
**HTTPS vs SSH Access**

The command above uses HTTPS. Alternatively, you can use SSH:

```bash
git clone git@github.com:<YOUR_GITHUB_USERNAME>/cson-forge.git
cd cson-forge
```

For more information on HTTPS and SSH access, see the [GitHub documentation on cloning repositories](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).
:::

:::{tip}
SSH keys provide a more secure and convenient way to authenticate with GitHub, eliminating the need to enter your credentials for each push or pull operation. To set up SSH keys for GitHub, see:
- [Generating a new SSH key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
- [Adding a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)
:::


