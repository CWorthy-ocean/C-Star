.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- Remote netCDF sources that were previously misclassified as text are now retrieved as binary and sha256-verified against file_hash when one is given. A stale file_hash on such a source can now fail with a hash mismatch where it previously passed silently. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- A blueprint that omits ``working_dir`` now runs under ``CSTAR_DATA_HOME/blueprint_runs/<application>/<name>`` instead of the directory the command was run from; write ``working_dir: .`` to keep the old behaviour. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- Forge no longer moves a ``~/cstar/_forge_bp_runs/<name>`` working directory onto scratch: forge blueprints saved by earlier versions have that default removed when loaded and take the new default, and any other ``working_dir`` is used as written. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- On Bouchet, ``CSTAR_DATA_HOME`` defaults to ``<scratch_pi_*>/<user>/cstar`` instead of ``~/cstar`` when neither ``SCRATCH`` nor ``CSTAR_DATA_HOME`` is set, so new workplan and blueprint runs land on scratch. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- ``cstar workplan run --dry-run`` is removed; use ``cstar workplan check`` (deep by default) to validate a workplan without scheduling it. ``--dry-run`` keeps its "change nothing" meaning on ``cstar blueprint migrate``, ``cstar admin clean`` and ``cstar admin migrate-outputs``. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- ``cstar workplan run`` now fails with a clear error when ``CSTAR_CLI_DRY_RUN`` is exported in the environment, since the plan-only mode it used to request no longer exists. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- If ``PROJECT`` is set on a laptop or an unrecognised cluster, the source-data cache now moves under it; it previously stayed under your home directory. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- On a machine that exports only ``SCRATCH_DIR`` or ``LOCAL_SCRATCH``, the source-data cache now goes to scratch instead of your home directory. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- Forge's per-system layout registry (``SYSTEM_LAYOUT_REGISTRY``, ``register_system``) and ``detect_system`` are removed; custom layouts registered against it no longer apply. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- New workplan runs are written to ``CSTAR_DATA_HOME/workplan_runs/<run-id>`` instead of ``CSTAR_DATA_HOME/<run-id>``; scripts or notebooks that build the old path will not find new runs. (`#739 <https://github.com/CWorthy-ocean/C-Star/pull/739>`_)
- ``Workplan.compute_environment`` is validated: unknown keys are rejected at load. The two keys the old bundled templates used (``num_nodes``, ``num_cpus_per_process``) were never read and are dropped with a warning instead. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- The blueprint page's "Workplan (experimental)" export is removed; the Workplan page replaces it. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- The catalog's ``roms_marbl_blueprint_path`` now returns the blueprint file for a flat ``blueprints/B_<name>.yaml`` entry (the per-machine directory layout still returns the directory). (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)

New features
~~~~~~~~~~~~


- ``build_forge_blueprint`` emits a ``UserWarning`` under ``use_pio=True`` listing every river ``custom_file`` or ``cdr_forcing_file`` that is not classic-format netCDF, explaining the conversion Forge will perform and how to pre-convert by hand. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- When staging a user-provided file under ``use_pio``, Forge converts netCDF-4 files to CDF-5 with ``nccopy -k cdf5``, keeps the original untouched, and logs a warning that the conversion happened. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- A workplan step may set ``blueprint: inline``; its blueprint is then built from the application's model plus the step's ``blueprint_overrides``, so no blueprint file is needed for fully-overridden steps. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- Inline steps are validated at ``cstar workplan check`` and at ``cstar workplan run`` preflight, reporting every missing required field in one message; the ``blueprint`` field itself stays required, so a forgotten line is still a schema error. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- The merged blueprint is written to the step's work directory (``tasks/<step>/work/blueprint.yaml``) when the workplan is scheduled, so the run artifacts record exactly what ran and reload/resume need no special handling. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- ``working_dir`` may be omitted from any blueprint; C-Star places the run under ``CSTAR_DATA_HOME/blueprint_runs`` by application and blueprint name, so blueprints stay portable between machines. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- ``cstar blueprint run --pre-run`` stages, builds and prepares a run without launching the model; a later ``cstar blueprint run --resume`` attaches to the prepared directory and launches it. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- ``cstar workplan run --pre-run`` does the same for every step, running locally even on scheduler systems; re-running the workplan without the flag (same run-id) attaches to the prepared directories and launches. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- Steps a pre-run cannot prepare are skipped and reported with the reason: an application that does not support pre-run, a blueprint produced by another step, a directive that consumes another step's output, or anything downstream of a skipped step. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- Applications declare support with ``pre_runnable = True`` on their ``ApplicationDefinition``; today only ``roms_marbl`` does. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- New ``CSTAR_PROJECT_HOME`` setting names your group's shared project directory; it defaults to ``$PROJECT`` and holds the shared source-data cache. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- ``runs:`` declares other workplan runs by alias (a run-id or a ``{{var}}`` runtime variable), and ``<step>@<alias>`` references one of their steps in ``depends_on``, deferred blueprints, directive ``step`` keys and ``{{output_dir: ...}}``-style placeholders. (`#737 <https://github.com/CWorthy-ocean/C-Star/pull/737>`_)

  - Every external reference is checked when the workplan is scheduled: the run must exist on this machine, the step must exist in it and must have completed, and the exact run record used is pinned into the transformed workplan for provenance.
  - A workplan may depend on a step of another run that is still in progress when both runs use the SLURM launcher; the new step is submitted with a SLURM dependency on that job. The local launcher refuses in-progress external steps.
  - ``cstar workplan check``, ``cstar workplan run`` and ``cstar workplan status`` report external dependencies: unresolvable or unusable ones are listed up front, and ``status`` shows the external token in the Dependencies column.

- ``continue-from`` accepts an optional ``timestamp`` (with ``step`` or ``path``) to continue from the restart dated exactly that time rather than the latest one. (`#740 <https://github.com/CWorthy-ocean/C-Star/pull/740>`_)

  - ``timestamp`` takes an ISO 8601 date-time, a date alone (meaning midnight), or the 14-digit stamp from restart file names (e.g. ``20120201000000``).
  - If the source has no restart at that timestamp, the step fails with an error listing the restart timestamps that are available.

- ``cstar wizard`` opens a two-page app, Blueprint and Workplan; ``cstar forge wizard`` still works and launches the same app. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- The Workplan page composes a workplan from catalog, path, uploaded, deferred (``from_step``) or inline blueprints, with per-step overrides, ``continue-from``/``nest-from`` directives (including the restart ``timestamp``), compute overrides, and references to steps of other runs (``step@alias``). (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)

  - Recipes generate time-chunked runs (calendar months, fixed days or equal parts; each chunk writes only ``end_date``), spin-up dt ramps, forge-then-run pairs and upscaling chains, from an existing step or directly from a blueprint.
  - Loading an older workplan rewrites deprecated forms (``rst_path``, ``bry_path``, ``joined_output``, same-run absolute paths, dummy-file blueprints) into the current ones and lists every change; the loaded file is never modified without an explicit overwrite confirmation.
  - Deep check, pre-run readiness, and streamed Check and Run (with a Pre-run-first option on SLURM targets) run from the page.

- A live preview pane shows the YAML (editable, with Apply and Discard) and a dependency graph; a Preview control places it right, bottom or top and a "Keep preview visible" pin keeps it on screen while the cards scroll. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- ``compute_environment`` can choose the launcher (``launcher: local | slurm``), name the system it was written for, and set workplan-wide SLURM defaults that every step inherits unless it overrides them. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)

  - Requesting ``slurm`` on a machine without a scheduler fails at schedule time; a ``system`` that does not match the current machine logs a warning.

- Applications can describe the blueprint they emit for a deferred downstream step (``ApplicationDefinition.emitted_blueprint``); forge does, so a deferred ROMS-MARBL step is prefilled with its file name and CPU count. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- The catalog reads a flat ``blueprints/B_<name>.yaml`` as a ROMS-MARBL blueprint beside the older per-machine directories. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)



Bug Fixes
~~~~~~~~~


- Classic-format netCDF inputs (CDF-1/2/5, e.g. forge use_pio output) were often misclassified as text, and validate() then rejected them with a "text file (e.g. a roms-tools YAML)" TypeError. Before that check existed, the same misclassification also made local files get copied with shutil.copy2 instead of symlinked, and made remote files get read whole into memory and skip sha256 verification. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- A 0-byte local input file now raises a clear "is empty" ValueError from validate() instead of being reported as a text/YAML file. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- Local binary inputs given by a relative path are now symlinked by their resolved path, so the staged symlink no longer dangles. This already affected netCDF4/HDF5 inputs, and the classification fix above would otherwise have extended it to classic-format files that used to be copied as text. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- A failed or interrupted ``nccopy`` conversion no longer leaves a partial output file behind that the no-clobber reuse logic would pick up on the next run. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- A user-provided file that was already classic-format is now copied as-is under ``use_pio`` rather than being needlessly re-encoded, so ``nccopy`` is only required when a conversion is actually needed. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- A failed or interrupted CDF-5 conversion of a Forge-generated input (or a nest_ic / upscaler output) no longer leaves a truncated file at the final path that no-clobber reuse logic would treat as finished on the next run. (`#731 <https://github.com/CWorthy-ocean/C-Star/pull/731>`_)
- Forge resolved ``ntimes`` to 0 for any run window shorter than a day because the step count was derived from whole days. (`#730 <https://github.com/CWorthy-ocean/C-Star/pull/730>`_)
- Local-launcher steps with a walltime were wrapped as ``timeout 600s -k 2s``, which GNU ``timeout`` rejects, so every time-bounded local step failed before starting. (`#730 <https://github.com/CWorthy-ocean/C-Star/pull/730>`_)
- A ``local`` compute override now produces a working ``timeout`` command; previously any step with ``compute_overrides.local`` failed immediately, so runs that relied on that failure path (none known) will now actually run with the configured walltime. (`#730 <https://github.com/CWorthy-ocean/C-Star/pull/730>`_)
- A blueprint, step or run name with no letters or digits is rejected instead of silently producing an empty directory name that collapsed onto its parent directory. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- Sentinel files are written atomically; the local proxy script could previously catch an in-place rewrite mid-way and leave a step with an empty sentinel, so the step showed no status and the run never reached a terminal state. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- Step sentinels now record which launcher created them, so a local pre-run after a failed SLURM run under the same run-id no longer crashes while reading the SLURM sentinel and instead clears and re-prepares the failed steps. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- ``cstar admin clean`` no longer deletes the user catalog at ``~/cstar/catalog`` when it clears the C-Star data directory. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- Tab completion of step names found no steps for a run whose tracking record had been cleaned up. (`#739 <https://github.com/CWorthy-ocean/C-Star/pull/739>`_)
- Forge runs with ``bgc_mode: none`` crashed in ROMS with garbage tracer names because the namelist still requested 32 BGC tracers; Forge now sets the BGC tracer count to 0 when MARBL is off. (`#741 <https://github.com/CWorthy-ocean/C-Star/pull/741>`_)

Improvements
~~~~~~~~~~~~


- A user-provided river or CDR file that already sits at its Forge output path and is netCDF-4 now raises under ``use_pio`` instead of being silently accepted and failing later in ROMS. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- Added ``NetCDFFormat`` and ``netcdf_format()`` to ``cstar.base.utils`` as the single owner of magic-number format detection; ``check_nc_pio_compatible`` now uses it instead of an inline header check. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- ``convert_to_cdf5`` is now the single owner of the ``nccopy -k cdf5`` call, with a ``remove_source`` option (default on) so user-provided originals can be converted without being deleted. (`#731 <https://github.com/CWorthy-ocean/C-Star/pull/731>`_)
- ``convert_to_cdf5`` refuses a conversion whose source and destination are the same file instead of letting ``nccopy`` truncate its own input. (`#731 <https://github.com/CWorthy-ocean/C-Star/pull/731>`_)
- The ROMS-MARBL blueprint Forge emits no longer carries a working directory, so a workplan step or the C-Star default places it; the forge blueprint schema is now version 9. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- A system can declare its own scratch convention through ``SystemContext.scratch_root``, consulted for ``CSTAR_DATA_HOME`` after the ``CSTAR_SCRATCH_DIRS`` variables. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- A launcher that reads a sentinel written by another launcher uses its persisted status instead of querying a pid or job id that belongs to the other system, and refuses to adopt an attempt it cannot track while that attempt is still in progress. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- The first line of a step log now distinguishes a launch from a completed pre-run from a resume after a failure. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- The source-data cache now lives at ``<root>/cstar/source-data`` everywhere, and an existing cache at the old location is linked in, so nothing is downloaded again. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- ``cstar admin clean`` spares the source-data cache when it sits inside the C-Star data directory. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- Removed the orchestrator's unreachable monitor mode, leaving a single scheduling path for workplan runs. (`#738 <https://github.com/CWorthy-ocean/C-Star/pull/738>`_)
- The orchestrator now refuses to launch a step a second time within a run, rather than silently resubmitting its job. (`#738 <https://github.com/CWorthy-ocean/C-Star/pull/738>`_)
- Runs started before this change keep their existing directory when re-run, resumed, gathered, or cleaned, so in-flight work continues where it is. (`#739 <https://github.com/CWorthy-ocean/C-Star/pull/739>`_)
- A malformed ``timestamp`` (partial date, timezone, fractional seconds) is reported by ``cstar workplan check`` / submission, before any step runs. (`#740 <https://github.com/CWorthy-ocean/C-Star/pull/740>`_)
- A Forge blueprint that requests BGC tracers with MARBL turned off is now rejected with a clear message instead of failing inside ROMS. (`#741 <https://github.com/CWorthy-ocean/C-Star/pull/741>`_)

  - Stored blueprints fail during ``cstar forge run`` validation, before any source data is staged or inputs are generated.
  - The wizard's validity banner reports the same error.

- ``cstar workplan check``'s deep resolution is a library function (``cstar.orchestration.check.deep_check``) the CLI wraps. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- The wizard shell is wider, and ``position: sticky`` now works for the status bar and the preview pane in voila (the ipywidgets and voila output containers no longer clip it). (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- The Download button separates its caption from the file name on both pages. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)


Miscellaneous
~~~~~~~~~~~~~

- Documented inline blueprints in the workplan guide; the nesting tutorial now uses ``blueprint: inline`` for its ``nest_ic`` and ``upscaler`` steps and the tutorial's ``nest_ic_bp.yaml`` dummy blueprint is removed. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- The workplan JSON schema advertises the ``inline`` token. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- Integration tests are now tiered by directory: ``forge/`` (real roms-tools, no ROMS build; ubuntu and macOS on every PR), ``e2e/`` (forge → roms_marbl workplan with ParallelIO, a NETCDF4 negative control, and hello_world workplans covering DAG shapes and dependency-failure propagation; ubuntu on every PR), ``roms_marbl/`` (the prebuilt case on the PIO-off, partitioned path), and a weekly ``integration_extended`` matrix tracking ucla-roms ``main``. (`#730 <https://github.com/CWorthy-ocean/C-Star/pull/730>`_)
- Upstream source data is pinned by commit SHA and sha256 in a pooch registry and cached in CI alongside roms-tools' own pooch cache. (`#730 <https://github.com/CWorthy-ocean/C-Star/pull/730>`_)
- ``docs/contributing.rst`` documents the tiers, how to add a case, the golden protocol, the data pin, and the CI cache behaviour. (`#730 <https://github.com/CWorthy-ocean/C-Star/pull/730>`_)
- CI: unit and integration tests run once per PR update instead of twice (push + pull_request), roughly halving CI time per push. (`#735 <https://github.com/CWorthy-ocean/C-Star/pull/735>`_)
- CI: test workflows can be triggered manually via ``workflow_dispatch`` for branches without a PR. (`#735 <https://github.com/CWorthy-ocean/C-Star/pull/735>`_)
- CI: a new push to a PR cancels that PR's in-progress test runs; runs on ``main`` are never cancelled. (`#735 <https://github.com/CWorthy-ocean/C-Star/pull/735>`_)
- The HPC page's "Where data goes" section states one working-directory rule for every application; the forge blueprint, terminology, tutorial and developer pages follow it, and the committed blueprint JSON schemas are regenerated. (`#732 <https://github.com/CWorthy-ocean/C-Star/pull/732>`_)
- Documentation for pre-running a blueprint and a workplan (``blueprints.rst``, ``workplans.rst``, ``hpc.rst``, ``terminology.rst``) and for declaring ``pre_runnable`` in a custom application. (`#733 <https://github.com/CWorthy-ocean/C-Star/pull/733>`_)
- The HPC and data-access docs describe the single cache rule and ``CSTAR_PROJECT_HOME`` in place of the per-system table. (`#736 <https://github.com/CWorthy-ocean/C-Star/pull/736>`_)
- New "Referencing other runs" section in the workplan docs and notes on the directives page; the published workplan JSON schema is regenerated for the new ``runs`` field. (`#737 <https://github.com/CWorthy-ocean/C-Star/pull/737>`_)
- Step names and run aliases may no longer contain ``@``, which now separates a step from its run alias; a workplan with such a step name is rejected when loaded. (`#737 <https://github.com/CWorthy-ocean/C-Star/pull/737>`_)
- Corrected the workplans guide, which said re-running a run ID keeps monitoring until the run ends; it now schedules any remaining steps and returns. (`#738 <https://github.com/CWorthy-ocean/C-Star/pull/738>`_)
- Corrected the workplans guide's status example, which passed the run ID as ``--run-id`` although ``cstar workplan status`` takes it as an argument. (`#738 <https://github.com/CWorthy-ocean/C-Star/pull/738>`_)
- The HPC guide, terminology page, and workplan tutorial show the new run layout; the HPC guide's step-directory path now includes ``tasks/``. (`#739 <https://github.com/CWorthy-ocean/C-Star/pull/739>`_)
- The Forge specs docs now describe how ``param.ntrc_bio`` follows ``bgc_mode``. (`#741 <https://github.com/CWorthy-ocean/C-Star/pull/741>`_)
- Pattern generators live in ``cstar/orchestration/patterns.py`` with golden fixtures; an e2e test runs a generated hello_world chain through ``cstar workplan run``. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
- Wizard documentation gains a Workplan page section; workplan documentation describes ``compute_environment`` and drops the incorrect ``local: num_cpus`` claim. (`#743 <https://github.com/CWorthy-ocean/C-Star/pull/743>`_)
