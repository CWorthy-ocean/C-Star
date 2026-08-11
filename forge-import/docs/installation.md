# Advanced Installation (HPCs or developers)

For a laptop or workstation, the one-command conda install in
[Getting Started](getting-started.md) is all you need. This page covers the
two heavier scenarios — **HPC systems** and **developer setups** — plus
byte-reproducible installs from the lockfile.

## Prerequisites

- Conda, Mamba, or Micromamba (developer setups: `dev-setup.sh` bootstraps
  micromamba automatically if none is present)
- Python ≥3.12 is required; the managed environments pin an appropriate version
- Git (developer setups only)

## Installation on HPC

### Install the package

On a supported HPC system the compiler/MPI/netCDF toolchain comes from the
site's environment modules, so install **just** `cstar-forge` — deliberately
*without* `cstar-ocean-standalone`:

```bash
conda create -n cstar-forge-env -c conda-forge cstar-forge
conda activate cstar-forge-env
```

```{warning}
Do not install `cstar-ocean-standalone` (or the conda `compilers` packages)
into an environment you will activate on HPC. The conda compiler packages
export `CC`/`CXX`/`FC` on every activation and put their own `mpif90` ahead of
the module toolchain on `PATH` — silently hijacking ROMS builds that should
use the cluster's compilers.
```

For byte-identical installs across a team or a paper's lifetime, use the
[lockfile replay](#reproducible-installs-lockfile) below instead of a fresh
solve.

### Keep the environment isolated from `~/.local`

On clusters, pip's `--user` fallback and Python's user-site directory
(`~/.local/lib/pythonX.Y`) are a chronic source of silently broken
environments: a module-provided Python of the same minor version shares that
directory and shadows the env's packages. The repo ships the hardening we use:

- `scripts/harden-env.sh` — sets `PIP_USER=0`/`PYTHONNOUSERSITE=1` and can
  persist the latter into the env via an `activate.d` hook, so every future
  activation stays off user-site.

If you hit `ModuleNotFoundError` for packages you know are installed, or pip
installs that vanish, this is the first thing to check.

### Jupyter kernel for the cluster's JupyterHub

The wizard served via `cstar forge wizard` needs no kernel registration. But if
you want to open notebooks in the **cluster's central Jupyter installation**
(e.g. an HPC OnDemand portal), that external Jupyter must be told about your
env's kernel — with an activation wrapper, so shell magics and
`activate.d`-dependent packages work inside notebooks:

```bash
bash scripts/register-kernel.sh   # from a checkout, with the env active
```

(Equivalent minimal form, without the activation wrapper:
`python -m ipykernel install --user --name cstar-forge-env`.)

### Using the wizard from a login node

Login nodes have no browser; bind locally and SSH-forward from your laptop:

```bash
# on the login node:
cstar forge wizard --no-browser
# on your laptop:
ssh -N -L 8866:localhost:8866 <user>@<login-node>
# then open http://localhost:8866 locally
```

If your HPC provides a Jupyter interface, it may be more convenient to clone the repo (see next section) and use the
[forge-blueprint-wizard.ipynb](../cstar_forge/forge-blueprint-wizard.ipynb) notebook instead of the Voila web app.

Alternatively, C-Star Forge is designed so that you can build your Forge blueprints on one machine and process the data on another; feel free to run the wizard from your laptop, upload your blueprint to your HPC, and process it there from the command line.

## Installation for developers

Developer setups install `cstar_forge` in **editable** mode from a clone, with
sibling packages (C-Star, roms-tools) swappable to arbitrary git refs.

### 1. Fork and clone

Fork [CWorthy-ocean/cstar-forge](https://github.com/CWorthy-ocean/cstar-forge),
then:

```bash
git clone https://github.com/<YOUR_GITHUB_USERNAME>/cstar-forge.git
cd cstar-forge
git checkout -b <branch-name>
```

:::{note}
The command above uses HTTPS; `git clone git@github.com:<YOUR_GITHUB_USERNAME>/cstar-forge.git`
uses SSH. To set up SSH keys, see the
[GitHub SSH documentation](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent).
:::

### 2. Create the environment

Pick whichever of the following matches your workflow. All three produce the
same `cstar-forge-env` conda environment with `cstar_forge` installed in
editable mode.

#### (a) Easy mode: `dev-setup.sh`

A thin orchestrator that finds or bootstraps a package manager (micromamba →
mamba → conda → downloads micromamba locally), creates/reuses the
`environment.yml` env, pip-installs C-Star and roms-tools from GitHub
(`--no-deps`), installs `cstar_forge` editable, and registers the Jupyter
kernel:

```bash
./dev-setup.sh
```

**Options:**
- `--clean`: remove and rebuild the environment
- `--batch` / `-f` / `--force`: no interactive prompts (CI/automation)
- `--with-compilers`: also install compilers/mpich/netcdf-fortran (laptops
  only — see the HPC warning above)
- `--roms-tools-ref REF` / `--c-star-ref REF`: pin the sibling installs to a
  git branch, tag, or commit instead of `main`

#### (b) Manual conda

```bash
conda env create -f environment.yml
conda activate cstar-forge-env
./set-repo-versions.sh --roms-tools-ref main --c-star-ref main  # pip --no-deps from GitHub
pip install -e . --no-deps
bash scripts/register-kernel.sh   # optional: Jupyter kernel + activation wrapper
```

`environment.yml` is a *generated* file (from `pyproject.toml` via pixi — edit
the latter, never the former) and doesn't carry `roms-tools`/`cstar-ocean`, so
the `set-repo-versions.sh` step is required, not optional. Re-run
`set-repo-versions.sh` any time you need different sibling refs.

#### (c) Pixi

[Pixi](https://pixi.sh) reads dependencies straight from `pyproject.toml`
(`[tool.pixi.*]`) and its lockfile (`pixi.lock`), so it needs neither
`dev-setup.sh` nor `environment.yml`:

```bash
pixi install -e dev                 # dev environment (editable + dev/app extras)
pixi run -e dev pytest tests/ -v
pixi install -e dev-laptop          # dev + local build toolchain (compilers, MPI,
                                    # netCDF) for compiling ROMS on a laptop
```

### 3. Verify

```bash
conda activate cstar-forge-env   # or: micromamba activate cstar-forge-env
python -c "import cstar_forge; print('✓ cstar_forge works')"
jupyter kernelspec list | grep cstar-forge-env   # if you registered the kernel
python -m cstar_forge.config show-paths          # detected system + data paths
```

`show-paths` output looks like:

```text
System tag : MacOS
Hostname   : your-hostname

Paths:
  here         -> /path/to/cstar-forge/cstar_forge
  source_data  -> /path/to/source-data
  input_data   -> /path/to/input-data
  scratch      -> /path/to/scratch-directory
  ...
```

(reproducible-installs-lockfile)=
## Reproducible installs (lockfile)

`pixi.lock` is the single source of truth for the fully-solved dependency
closure (conda-channel layer + pypi layer, per platform). Three ways to replay
it, in order of preference:

**a) pixi, on any machine — including HPC (recommended)**

pixi ships as a single static binary that installs into `$HOME`, no admin
access required:

```bash
pixi install -e dev
```

This reproduces the exact locked environment (conda + pypi layers together, in
the correct order) with full fidelity.

**b) Plain conda, via the exported lockfile artifacts**

*Non-developers: use the `user` environment's artifact.* The `user` pixi
environment sources everything — cstar-forge itself included — from
conda-forge, so its explicit spec is a complete environment with **no pip step
at all**:

```bash
python scripts/export-lock-artifacts.py --env user --outdir lock-artifacts
conda create -n cstar-forge-env --file lock-artifacts/conda-explicit-user-linux-64.txt
```

For dev environments (editable checkout + pypi layer), the artifacts come in
two layers per environment/platform — a conda explicit-spec file and a
`requirements-<env>-<platform>.txt` for the pinned pypi layer. Both are
published as GitHub release assets
(`.github/workflows/lock-artifacts.yaml`) and can be regenerated locally with
`python scripts/export-lock-artifacts.py --env dev --outdir <dir>`. Replay:

```bash
conda create -n cstar-forge-env --file conda-explicit-dev-linux-64.txt
conda install -n cstar-forge-env pip   # pixi's pypi installer is uv-based, so pip
                                       # is NOT in the explicit spec above
conda activate cstar-forge-env
python -m pip install --no-deps -r requirements-dev-linux-64.txt
python -m pip install -e . --no-deps   # the editable checkout itself
```

`--no-deps` is required, not optional: the requirements file is already the
complete resolved closure, and pip must not re-resolve it. `python -m pip`
(rather than bare `pip`) ensures the newly-created env's own pip is used. This
path works with any conda version.

**c) `conda env create --file pixi.lock` via the conda-lockfiles plugin (future — not yet usable)**

The [conda-lockfiles](https://github.com/conda/conda-lockfiles) plugin aims to
let plain conda consume `pixi.lock` directly. Not usable yet: its newest
release only reads lock-file schema ≤6 while pixi ≥0.76 writes v7, and it
requires conda ~26+. Revisit once the plugin catches up.
