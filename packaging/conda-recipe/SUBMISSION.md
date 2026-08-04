# Submitting the conda-forge recipe

`recipe.yaml` is written in the rattler-build v1 schema, which
[conda-forge/staged-recipes](https://github.com/conda-forge/staged-recipes) accepts
directly (alongside the older `meta.yaml`/conda-build format).

Before submitting: fill in real GitHub handles in `extra.recipe-maintainers`
(currently a placeholder), and re-verify the pinned `sha256` against the
released sdist if `version` has moved on from what's checked in here.

## Steps

1. Fork and clone `conda-forge/staged-recipes`.
2. Create a branch, e.g. `git checkout -b add-cstar-ocean`.
3. Copy this recipe into the fork:
   ```
   mkdir -p recipes/cstar-ocean
   cp recipe.yaml recipes/cstar-ocean/recipe.yaml
   ```
   The `cstar-ocean-standalone` output rides along in the same `recipe.yaml` —
   staged-recipes builds both outputs from one PR; no separate recipe directory
   is needed for it.
4. Commit and push, then open a PR against `conda-forge/staged-recipes`
   (base branch `main`), following the repo's PR template (fill in the
   checklist: license included, tests defined, etc.).
5. CI (`azure-pipelines`) will build both outputs (`cstar-ocean`,
   `cstar-ocean-standalone`) on linux-64, osx-64, osx-arm64, win-64 (the
   standalone output is skipped on win via `build.skip`).
6. A conda-forge bot will comment asking for maintainer confirmation
   (`@conda-forge-admin, please add me`) and ping `@conda-forge/staged-recipes`
   reviewers automatically after ~30 min. Respond to any linter or review
   comments, then wait for two reviewer approvals (or one + bot merge) to land.
7. After merge, conda-forge creates the `cstar-ocean-feedstock` repo and the
   packages appear on `conda-forge` within the following CI run
   (`conda install -c conda-forge cstar-ocean` / `cstar-ocean-standalone`).
8. Future version bumps happen automatically via the `regro-cf-autotick-bot`
   opening PRs against the new feedstock — no need to touch staged-recipes again.
