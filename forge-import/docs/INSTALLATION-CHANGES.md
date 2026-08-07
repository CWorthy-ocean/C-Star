# Installation changes — what existing users need to know

The dependency overhaul (August 2026) changed how C-STAR Forge is installed and
managed. If you had a working setup before, here's what's different.

## Action needed

- **The conda environment is renamed**: `cstar-forge-v0` → `cstar-forge-env`.
  Rebuild with `./dev-setup.sh --clean` (your old env keeps working, but docs,
  scripts, and the Jupyter kernel now use the new name — select the
  `cstar-forge-env` kernel in notebooks).
- **Python 3.12+ is now required** (was 3.11). A `--clean` rebuild handles this.

## What changed under the hood

- **`pyproject.toml` is now the single source of truth** for dependencies.
  `environment.yml` is a *generated* file — don't edit it by hand; edit
  `pyproject.toml` and regenerate (CI enforces this).
- **`dev-setup.sh` is much smaller** but behaves the same (same flags, plus
  `--with-compilers`). The HPC hardening it used to inline now lives in
  `scripts/harden-env.sh` and `scripts/register-kernel.sh`.
- **Dependencies now come from real releases**: roms-tools ≥ 4.0.1 (conda-forge;
  dask is a core dependency now — the `[dask]` extra is gone) and
  cstar-ocean ≥ 0.8 (PyPI and conda-forge).

## New options (optional)

- **pixi for development**: `pixi install -e dev` reproduces the exact locked
  environment from `pixi.lock` — a single-binary install that also works on HPC
  without admin rights.
- **Plain-conda lockfile replay**: each release publishes explicit-spec +
  pinned-requirements artifacts for machines that only have conda. See
  `docs/installation.md` ("Reproducible installs").

## Coming soon

- `pip install cstar-forge` and `conda install -c conda-forge cstar-forge`
  (first release + feedstock are in progress).
