# AGENTS.md

Instructions for AI coding agents working in this repository. The developer
guide is `docs/contributing.rst`.

## Project

C-Star (`cstar-ocean`) runs reproducible regional ocean simulations, driven
by the `cstar` Typer CLI. A **blueprint** (YAML loaded into a Pydantic model)
declares a run; a **workplan** composes blueprints into a DAG that C-Star
schedules. C-Star stages inputs, compiles ucla-roms, MARBL, and ParallelIO,
writes the namelist, and runs locally or through SLURM. The `forge`
application builds a new domain's inputs with roms-tools.

## Setup

```bash
conda env create -f environment-laptop.yml  # environment-hpc.yml on HPC
conda activate cstar-env
pip install -e ".[dev]"                     # ".[dev,docs]" for the docs
```

- Install anything else with `mamba install -c conda-forge <pkg>`. Use pip
  only for the editable install or a package not on conda-forge, after
  `pip install --dry-run`: pip can replace conda's numpy, netCDF4, or mpi4py
  with wheels that break compiled packages. Never install into `base` or a
  shared HPC environment, and ask before changing one you did not create.
- On macOS the conda `rsync` is required; the system one is too old for the
  ucla-roms Makefiles.
- Run `python -m cstar.cli.cli` and pytest from this checkout's root: the
  current directory comes first on `sys.path`, so an editable install of
  another checkout does not shadow this one.

## Tests: never run bare `pytest`

Bare `pytest` at the repository root collects the integration suite, which
clones and compiles ucla-roms and downloads data. Select tests by path:

```bash
python -m pytest cstar/tests/unit_tests   # default for every change, offline
# Tier 1: forge with real roms-tools, no ROMS build, about 1 minute
python -m pytest cstar/tests/integration_tests/test_fixtures.py \
    cstar/tests/integration_tests/forge
# Tier 2: compiles ROMS, MARBL, and ParallelIO and runs mpirun, about 10 min
python -m pytest cstar/tests/integration_tests/e2e \
    cstar/tests/integration_tests/ucla_roms
```

- Never run `cstar/tests/integration_tests` as a whole: its `extended/`
  directory is a heavier suite that only CI runs. Tell the user before
  starting Tier 2. Select tests by path, not by marker (the registered
  markers, `network` and `slow`, are unused), and pytest-xdist is not a
  dependency (no `-n`).
- Integration tiers need `nccopy`, `mpirun`, and the compilers on `PATH`.
  Without the environment's `bin/` on `PATH`, the Forge tests in Tier 1 skip
  ("nccopy is required") and pytest still exits 0. Read the skip count.
- Golden fixtures encode what the model is given. Regenerate one only for an
  intended change (`UPDATE_GOLDEN=1`, `-k <test>`) and review the diff.

## Lint and types

Run `pre-commit run --all-files` from `cstar-env` before committing. The file
hooks check only `cstar/`: whitespace fixers, `check-yaml`, a 200 kB file
limit, `ruff-check --fix`, `ruff-format`, mypy, and `absolufy-imports`.
`check-manifest` compares every tracked file with the sdist; add a dev-only
top-level file to `[tool.check-manifest] ignore` in `pyproject.toml`.

- Absolute imports only (`from cstar.x import y`). Keep new code fully
  annotated; mypy gates commits.
- Ruff selects `I`, `F`, `E`, `W`, `D`, `UP`, `TCH`, and `RUF`. Type-only
  imports go under `if TYPE_CHECKING:`; Pydantic models are exempt through
  `runtime-evaluated-base-classes`. Keep `target-version = "py311"`.

## Layout

Under `cstar/`: `base/` (shared primitives), `roms/` (`ROMSSimulation`,
`namelist.py`), `marbl/`, `pio/`, `orchestration/` (blueprint and workplan
models, DAG runner, launchers, tracking), `applications/` (registry,
`ucla_roms/`, `forge/`, and others), `catalog/`, `wizard/`, `system/`
(per-machine contexts), and `cli/`. `cstar/externals/` is filled at run time.

## Conventions

Read `docs/developers/conventions.rst` before adding a class, model field,
flag, constant, CLI option, or mechanism. In short: reuse the existing
extension points and constant families, keep one source for each default, fail
before compute is spent, and never carry run-scoped state in an environment
variable. Deprecations: "Deprecations and compatibility" in
`docs/contributing.rst`.

## Gotchas

- **Blueprint `schema_version` is semver.** A major bump needs a
  `SchemaAdapter` or `SchemaBreak` and a new
  `docs/schemas/bp/<app>/<app>_schema.<ver>.json`; never regenerate a
  published one. See "Versioning the Blueprint Schema" in
  `docs/developers/custom_applications.rst`.
- **A namelist key from a new ucla-roms release needs a new namelist tier**,
  in `cstar/roms/namelist.py` and Forge's `namelist_model.py`, with per-tier
  goldens; older releases stop on unknown keys. See "Versioned namelist
  schemas" in `docs/developers/forge_internals.rst`.
- Build verification fails on mixed conda and module toolchains;
  `CSTAR_DISABLE_BUILD_VERIFICATION=1` skips it. roms-tools YAML is
  multi-document; read it with `yaml.safe_load_all`.

## Related repositories

- [ucla-roms](https://github.com/CWorthy-ocean/ucla-roms): `src/namelist.nml`
  is the template C-Star's namelist models follow. Its `cppdefs.opt` keys,
  which Forge renders from `cppdefs.opt.j2`, select model features and the
  libraries the build links.
- [roms-tools](https://github.com/CWorthy-ocean/roms-tools) writes the input
  NetCDF files and YAML; C-Star pins `roms_tools>=5.3.0,<6`. With an editable
  sibling install, check what is installed (`pip show`) before trusting it.
- Forge emits `ucla_roms` blueprints, so a `ucla_roms` schema change updates
  Forge in the same pull request.

## Data safety

Never delete, move, or overwrite model output, source data, scratch space,
anything under `CSTAR_DATA_HOME` or `CSTAR_PROJECT_HOME`, or another user's
files, even to start fresh. Do not run `--clobber`, `cstar admin clean`,
`cstar admin migrate-catalog`, `cstar admin migrate-outputs`, or
`cstar blueprint migrate --inplace` without the user's explicit OK; give them
the exact command instead.

## Pull requests

- `ci/update_release_notes.py` copies the Breaking Changes, New Features, Bug
  Fixes, Improvements, and Miscellaneous sections of each merged description
  into `docs/releases/`. Write those bullets for users, one line each; put
  reviewer detail under `## Notes for Reviewers`. Never edit release notes on
  a branch.
- The repository squash-merges, so a pull request stacked on another carries
  the base's commits after it merges. Stack only when needed, say so in the
  description, and rebase onto `origin/main` once the base merges.

## Writing docs

Write user pages for an ocean modeler who wants a scientific result. Lead
with what the reader does, and show a command or real output rather than
describing it; run every example first. Document what exists today and leave
history to the release notes. Put the happy path first and leave internal
mechanics and every error case to developer pages and `--help`. Say each thing
once and link to it; Python snippets go in `docs/reference/python.rst`, each
paired with its CLI command. Plain sentences, no marketing tone.

## Posting on GitHub

This project follows the [C]Worthy AI policy:
https://github.com/CWorthy-ocean/.github/blob/main/AI_POLICY.md

- You may draft pull request descriptions, bug reports, and summaries of test
  results for the user.
- Do not post replies, review comments, answers, or discussion comments on the
  user's behalf, and do not join a discussion on your own. If asked to, tell
  the user that the project asks for replies in their own words, and give them
  notes to write from. If they share your output, they quote it and say where
  it came from.
- Fill in `.github/PULL_REQUEST_TEMPLATE.md`, including the `AI disclosure:`
  line: `none`, or `<tool>; <what it did>`.
- Do not add `Co-authored-by`, `Assisted-by`, `Generated-by`, or similar AI
  trailers to commits.
- The user answers for what you open under their account. Tell them plainly
  what you did not run or could not verify.
