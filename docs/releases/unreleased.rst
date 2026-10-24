.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- The ROMS-MARBL output file models and stream names (``RestartFile``, ``BoundaryFile``, ``CarbonateSensitivityFile``, ``OUTPUT_STREAMS``, ``OUTPUT_ROOT_NAME`` and related names) must now be imported from ``cstar.applications.roms_marbl.outputs``. (`#788 <https://github.com/CWorthy-ocean/C-Star/pull/788>`_)

  - Their old homes, ``roms_marbl.transforms``, ``roms_marbl.app`` and ``roms_marbl.file_system``, no longer re-export them.

- New attempts no longer write ``<stem>_original.bak``, ``<stem>.vars`` or their rotated ``.1``, ``.2`` copies in the run directory; read the attempt's ``attempts/<time>/`` copies instead. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- The wizard's default ModelSpec, and the ucla-roms release the integration tests pin, move to ``roms-marbl-0.12-default`` / ucla-roms 0.12.0. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)
- New namelist tier for ucla-roms >= 0.12.0, which always writes ``cdr_carbonate_sensitivity_time_interpolation``; earlier releases stop on that key, so they keep the 0.11 tier, and Forge rejects turning it off there with an error naming 0.12.0. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)
- ucla-roms refs that aren't release tags (``main``, other branches, and commit hashes C-Star cannot resolve to a release) select the 0.12 tier; a ucla-roms build without #396 stops on the new key. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)

  - Pin a release tag (e.g. ``0.11.0``) to keep the 0.11 tier; a commit hash that resolves to a release keeps that release's tier in both Forge and roms_marbl.

- Nine environment settings are removed; an exported one has no effect, and ``cstar`` prints a warning naming the replacement. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)

  - ``CSTAR_CLOBBER_WORKING_DIR`` and ``CSTAR_FRESH_CODEBASES``: pass ``--clobber`` to ``cstar blueprint run`` or ``cstar blueprint migrate`` (``--clobber <step|all>`` for ``cstar workplan run``).
  - ``CSTAR_CLI_DRY_RUN``: pass ``--dry-run`` to ``cstar blueprint migrate`` or ``cstar admin clean``, or use ``cstar workplan check``; those two commands and ``cstar workplan run`` refuse to run while it is exported.
  - ``CSTAR_CLI_VERBOSE``: pass ``--verbose`` to ``cstar blueprint run`` or ``cstar blueprint migrate``.
  - ``CSTAR_IN_ACTIVE_ALLOCATION``, ``CSTAR_ORCH_DELAYS``, ``CSTAR_ORCH_MAX_CONC``, ``CSTAR_ORCH_LOCAL_DELAY``, ``CSTAR_SLURM_POST_SUBMIT_DELAY``: no replacement; C-Star detects an active SLURM allocation from ``SLURM_JOBID``, and the polling intervals, I/O concurrency limit and post-submit delay are fixed.

- ``CSTAR_RUNID`` is internal: ``cstar env show`` lists it only with ``--group internal``, and neither ``--display export`` nor the configuration docs include it. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)
- Python API: ``cstar.base.env.max_concurrency()`` is now the constant ``cstar.base.utils.MAX_CONCURRENCY``, ``MigrationRequest.dry_run()`` is a ``dry_run`` field (``clobber()`` is removed), ``cstar.cli.common.set_flag`` is removed, and ``RunnerRequest``'s ``resume``, ``pre_run``, ``clobber`` and ``step`` are keyword-only. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)
- Blueprints and workplans are always read as YAML, which still loads ordinary JSON but not tab-indented JSON. (`#812 <https://github.com/CWorthy-ocean/C-Star/pull/812>`_)
- Removed ``PersistenceMode``, ``render_model`` and ``read_json_to_raw`` from ``cstar.orchestration.serialization``, and the ``mode`` argument from ``serialize``, ``deserialize`` and the other read helpers. (`#812 <https://github.com/CWorthy-ocean/C-Star/pull/812>`_)
- ``cstar blueprint migrate --in-place`` on a ``.json`` blueprint now writes YAML into that file. (`#812 <https://github.com/CWorthy-ocean/C-Star/pull/812>`_)
- ``cstar admin clean`` now requires exactly one of ``--run-id <id>`` or ``--all``; the old bare ``cstar admin clean --yes`` full wipe is now ``cstar admin clean --all --yes``. (`#814 <https://github.com/CWorthy-ocean/C-Star/pull/814>`_)

  - Without either option, or with both, the command stops with a usage error and removes nothing.

- ``CSTAR_FF_DEBUG_BUILD_MODE`` is renamed ``CSTAR_DEBUG_BUILD_MODE``, and developer mode no longer turns it on; set it to ``1`` explicitly for debug ROMS builds. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)
- ``CSTAR_FF_SLURM_DISABLE_MT`` is renamed ``CSTAR_SLURM_DISABLE_MT``, and developer mode no longer turns it on; Elja still sets it through its system settings. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)
- ``CSTAR_ORCH_TRX_FREQ`` is renamed ``CSTAR_FF_ORCH_TRX_TIMESPLIT_FREQ``, a time-splitting feature flag read only while ``CSTAR_FF_ORCH_TRX_TIMESPLIT`` is on. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)

  - Like every feature flag it is set in the shell, not the configuration file; a configuration file that still names ``CSTAR_ORCH_TRX_FREQ`` stops cstar commands until that line is removed.

- The developer-only commands ``cstar workplan compose``, ``cstar workplan generate``, ``cstar workplan plan`` and ``cstar template create`` are removed, with their four ``CSTAR_FF_CLI_*`` flags. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)
- Every old name above prints a warning naming its replacement when it is still exported. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)
- The ROMS application is named ``ucla_roms``; ``roms_marbl`` is still accepted everywhere (blueprints, workplan steps, CLI arguments, catalog lookups) and never needs editing. (`#818 <https://github.com/CWorthy-ocean/C-Star/pull/818>`_)

  - Files C-Star writes (transformed workplans, blueprints emitted by forge, chunk and resume blueprints, migrated copies) now say ``ucla_roms``, which earlier releases refuse.
  - In Python, ``Application.ROMS_MARBL`` is now ``Application.UCLA_ROMS``.

- Standalone runs and catalogs use the new name: the default run directory is ``blueprint_runs/ucla_roms/<name>``, and catalogs keep ROMS blueprints under ``blueprints/ucla_roms/``. (`#818 <https://github.com/CWorthy-ocean/C-Star/pull/818>`_)

  - An existing ``blueprint_runs/roms_marbl/<name>`` is still found and reused, so resuming or continuing an earlier run works.
  - A catalog's ``blueprints/roms_marbl/`` directory is read with a warning for one more release; ``cstar admin migrate-catalog`` moves it.

- Run manifest schema 1.5.0: step definition hashes recorded by an older manifest are dropped when it is read, so a re-entered run skips the edited-step check once; the next run records them again. (`#818 <https://github.com/CWorthy-ocean/C-Star/pull/818>`_)
- On Anvil and Bouchet, calling ``ROMSSimulation.run()`` from Python without a ``queue_name`` is now refused, because those systems require a queue; it used to submit to the primary queue. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)



New features
~~~~~~~~~~~~


- Re-entering a run refuses a step that reached ``Done`` under a different definition, naming the steps and the exact ``--clobber <step>`` to pass, or ``--run-id <other-id>`` to start a new run. (`#783 <https://github.com/CWorthy-ocean/C-Star/pull/783>`_)

  - A step's definition covers its blueprint file's content, ``blueprint_overrides``, ``directives``, dependencies and application; runtime ``--var`` values, ``compute_overrides`` and ``workflow_overrides`` are not part of it.
  - A step whose producer changed through ``from_step`` is named too; a step new to the workplan, a step that failed, or one recorded by an earlier release without a definition hash, is not checked.
  - A run manifest that cannot be read also refuses the re-entry; one written by a newer release keeps the "upgrade cstar-ocean" guidance.

- Step records list each step's products: kind, path relative to the step, size in bytes and metadata (ROMS stream, timestamp and output format; the roms-tools sidecar of each forge input; the role of each configuration file). (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)

  - roms_marbl records joined outputs (``restart``, ``history``, ``average``, other streams as ``output``), the generated namelist, the staged ``cppdefs.opt`` and ``marbl_in`` (``config``), the compiled executable (``build``) and the log.
  - A ROMS stream another step consumes is a kind of its own, so the consumer can find it by kind: ``gas_exchange`` (``_cdrgas``, read by ``carbonate-sensitivity-from``), ``upscaling`` (``_uscl``, read by the upscaler), ``cdr_output`` (``_cdr``); the boundary extraction a nested run reads is ``boundary_forcing`` with role ``child``.
  - forge records the published blueprint, the generated inputs by kind (grid, initial conditions, boundary, surface, tidal, river and CDR forcing), its three rendered configuration files and the log; nest_ic, upscaler and plotter record their one output each.

- ``cstar run check`` verifies the recorded products against the run directory: a missing or resized product fails the check. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)

  - ``--verify`` also compares content where a product records a sha256 (nothing records one yet, and the report says so).
  - A log is reported only when it shrank: a step's own log is still written after its record is taken.
  - A done step without a step record and a latest attempt that has not finished are reported for information only.
  - For a run-id the manifest is refreshed first, as ``cstar run show`` does; ``--no-refresh`` reads it as last recorded.
  - A directory holding ``cstar-run.yaml`` but no ``tasks/`` (a standalone ``cstar blueprint run``) is accepted as a target.

- Custom applications declare their products by overriding ``artifacts()`` on their ``ApplicationDefinition`` with glob patterns per ``ArtifactKind``; a runner may add metadata to a product while it runs. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- New ``cdr_metrics`` application writes the CDR efficiency and CO₂ uptake time series (``output/cdr_metrics.nc``) and a plot of them (``output/cdr_metrics.png``) from ROMS-MARBL ``_cdr`` output. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)

  - ``sources`` lists any number of directories or files, such as the segments of a ``continue-from`` chain; their records are stitched together and sorted by time.
  - A gap or overlap between consecutive averaging windows (a missing or repeated segment) fails the step and names the records.
  - Paths are given as ``location:`` entries, so relative paths resolve against the blueprint file and workplan placeholders such as ``{{output_dir: <step>}}`` work.

- ``domains`` maps a domain name to its grid and sources, ordered child to parent; one domain gives single-domain metrics. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Nested (upscaled) experiments: mark each parent domain with ``source_role: transfer``, and the domains are combined with roms-tools' ``UpscaledCDR``. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)

  - The CO₂ uptake of all domains is summed; the efficiency is normalised by the release domain's cumulative source only, so the transferred alkalinity is not counted twice.
  - ``cdr_metrics.nc`` then also holds each domain's uptake series and the ``closure_residual`` and ``<parent>_DIC_difference_inside_<child>`` diagnostics, and ``output/cdr_domains.png`` maps the domains.
  - Parents must be MARBL runs writing ``_cdr`` output; a CDR-lite parent (which writes ``_cdrtrc``) cannot be analysed.

- ``_cdr`` output lacking a variable the metrics need is reported with the ROMS setting that writes it; a release domain whose sources have the wrong sign fails with a report of where and how much. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- The metrics file records its provenance: domains and roles, grid files, number of ``_cdr`` files and the roms-tools version. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Each attempt that reads the workplan file carries its inputs into ``attempts/<YYYYMMDDTHHMMSS>/`` in the run directory: ``workplan.yaml`` (or ``workplan.json``), ``workplan.vars``, ``workplan_transformed.yaml`` and ``blueprints/<step>/<file>``. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)

  - The run manifest records each attempt's directory and a sha256 digest of each carried file, and points each step at its carried blueprint (``carried_blueprint``).
  - Each step record names the blueprint file the step ran (``blueprint_file``), such as its directive-applied override file.
  - A re-entry with ``--run-id`` alone or ``--resume`` replays the recorded plan and carries nothing.

- ``cstar run check`` compares carried inputs with their recorded digests: a changed file fails the check, and a missing file or deleted older attempt is listed for information. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- ``cstar run show`` lists the inputs directory of each attempt. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- ``cdr_metrics`` steps list their metrics series and figures in their step record, so ``cstar run show`` lists them and ``cstar run check`` reports either file if it goes missing. (`#793 <https://github.com/CWorthy-ocean/C-Star/pull/793>`_)

  - The series is an ``analysis`` product with role ``cdr_metrics``; the figure has role ``metrics``, and the map of nested domains has role ``domains``.
  - ``analysis`` is a new product kind for a dataset a step computes from other steps' output; the role says which analysis.

- Each step record says what ran the step (``execution``): the scheduler (``slurm`` or ``local``), the host and the process id. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)

  - Inside a SLURM job it also records the job id and the granted nodes, tasks, CPUs on the step's node, partition and account, read from the step's own environment.
  - A standalone ``cstar blueprint run`` inside an ``salloc`` records that allocation.

- The run manifest records each step's latest submission (``launcher``, ``job_id``, ``submitted_at``), written when the step is submitted. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)

  - ``launcher`` says whether ``job_id`` is a SLURM job id or the process id of a local step, also for a job that was killed before its step could write a record.
  - A step an attempt clobbers or resumes loses its earlier submission.

- ``cstar run show`` has a Jobs table with each step's job, how long it waited from submission to start, and its wall time. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)

  - The wait includes time a step was held for the steps it depends on; ``sacct`` can separate the two by job id.

- A run can be named by ``run://<uuid>`` or by its directory as well as by its run-id, in ``runs:`` aliases and in ``cstar run show``, ``check``, ``log``, ``path`` and ``cd``. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)

  - A run-id not tracked on this machine is looked up in the run directories of that name in your data home and the project runs area; two different runs under one run-id are an error that lists both.
  - A tracked run-id is always the tracked run; when the project runs area holds another run with that run-id, a warning names it and how to choose it.
  - When a tracked run's directory is gone, as after a scratch purge, a copy of the same run in the project runs area is used.

- A workplan can depend on a run that is not tracked on this machine, such as a copy or a promoted run, using the steps its step records say completed. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)

  - Scheduling pins each referenced run's run-id, uuid and start (and directory, when named by one) in the transformed workplan, and the run manifest records the uuid.

- ``cstar run ls --shared`` lists the project runs area and ``cstar run ls --all`` adds every run found in the run directories, with a ``where`` column. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)

  - The workplan builder's Refresh runs lists the same runs, named by ``run://<uuid>``.

- ``cdr_lite.cdr_carbonate_sensitivity_time_interpolation`` (ucla-roms >= 0.12.0) controls whether ddic_dco2/ddic_dalk read from forcing files are interpolated in time (default, as before) or held until the next record. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)

  - Setting it to false is rejected where ROMS would ignore it: outside ``bgc_mode: cdr_lite``, or with online carbonate sensitivities.
  - The wizard shows it under the CDR settings in ``bgc_mode: cdr_lite`` only.

- The ucla-roms 0.12 namelist schema accepts one entry per child domain (up to 8) for the extraction settings, so a 0.12 namelist or namelist override with several children validates. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)

  - With extraction on, every child needs its own vertical-grid settings (``N_chd``, ``theta_s_chd``, ``theta_b_chd``, ``hc_chd``), as ROMS requires.
  - A single value is still written as a single value, so one-child namelists are unchanged.

- New ``roms-marbl-0.12-default`` ModelSpec pinned to ucla-roms 0.12.0. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)
- ``cstar run attach <dir>`` registers a run directory that arrived from elsewhere in this machine's tracking, writing one tracking record and nothing into the run. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)

  - the run is checked first; a transfer with missing or truncated products is refused, other findings are listed and the run is attached with exit code 1
  - a run-id already tracked for the same run (same uuid) in another directory is re-attached; one tracked for another run is refused, and ``--as <run-id>`` attaches under a free run-id
  - ``--verify`` compares each product's recorded digest, ``--dry-run`` shows what would be recorded

- Attached runs are owned or read-only: read-only by default in the project runs area or when another user owns the directory, owned otherwise; ``--own`` and ``--read-only`` choose. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)

  - a read-only run refuses ``cstar run gather``, ``cstar run check --repair``, ``cstar run check --record-digests`` and ``cstar admin clean`` of its directory (the record is forgotten, the directory left)
  - ``cstar admin clean --run-id`` never removes a directory under the project runs area, whatever the mode
  - an attached run is never relaunched: ``cstar workplan run --run-id`` and ``--resume`` refuse it; start a new run that references it

- Each product records a content digest (``digest``, ``<scheme>:<hex>``): ``nc-sample-v2`` for netCDF files (structure plus a bounded sample of the data, independent of on-disk encoding), ``sha256`` of the bytes otherwise. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)

  - ``cstar run check --record-digests`` computes and records digests for every product that has none, writing the step records and the manifest; it needs an owned tracked run or an explicit directory
  - ``cstar run check --verify`` compares the recorded digest in its own scheme; a record that predates digests with a bare ``sha256`` is still compared, and a scheme this build does not compute is reported as skipped

- ``run://<uuid>`` finds an attached run wherever its directory is, through the tracking record, before any index scan. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- ``cstar run ls --all`` labels attached runs ``attached`` or ``attached, read-only`` in the ``where`` column. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- ``cstar run promote <run> [--to DIR] [--mode copy|link|move|materialize] [--include builds|temp-output|all]... [--dry-run] [--verify]`` copies a run into the project runs area as ``<CSTAR_PROJECT_HOME>/cstar/runs/<run-id>``, or ``DIR/<run-id>`` with ``--to``. (`#800 <https://github.com/CWorthy-ocean/C-Star/pull/800>`_)

  - By default the copy holds the manifest, transformed workplan, attempts, step records, logs, each step's ``work/`` YAML files and every product the step records list; ``--include builds`` adds a ROMS step's compile-time code, runtime code and cloned codebases, ``--include temp-output`` the per-rank output pieces, ``--include all`` the whole directory.
  - ``--mode link`` registers the run with the project as a symlink without copying; ``--mode materialize`` replaces that link with a copy; ``--mode move`` copies the whole directory, removes the source once the copy passed the check, and re-points a tracked run's record (the run is then attached, read-only when it is in the project runs area).
  - Nothing is ever overwritten: a destination holding another run, or this run already, a source missing a recorded product, and a destination file system without room for the plan are refused before any byte is copied; ``--dry-run`` prints the plan with per-group file counts and sizes.
  - ``--verify`` computes the digest of each source product that has none, records it in the copy's step records and manifest (the source is never written) and compares the copy; exit 1 when the copy has problems of its own, with the absolute paths that the source's own files already carry listed but not counted.

- ``cstar forge show-paths`` shows the project directory and the project runs area alongside the source-data cache and catalog. (`#802 <https://github.com/CWorthy-ocean/C-Star/pull/802>`_)

  - The runs area is shown even before it exists, with a note that ``cstar run promote`` creates it.
  - When neither ``CSTAR_PROJECT_HOME`` nor ``$PROJECT`` is set, the output says so.
  - ``--json`` adds ``project_home`` and ``project_runs`` to ``paths``, after the existing keys; each is ``null`` without a project directory.

- Two tutorial workplans run a nested ROMS-MARBL simulation with a CDR release in the child, upscaled back to the parent, and end by computing and plotting the combined CDR metrics. (`#776 <https://github.com/CWorthy-ocean/C-Star/pull/776>`_)

  - The tiny tutorial runs on a laptop in about 10 minutes from about 44 MB of pinned test data, with one command and no accounts.
  - The California Current tutorial (16 km parent, 4 km child) uses real forcing and runs under SLURM or on a workstation; ``full.vars`` holds a year-long window, and the page shows how to run a month.

- ``cstar env show --group internal`` lists the variables C-Star sets for its own processes. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)
- C-Star reads ``config.env`` under its config home (``~/.config/cstar``, or ``CSTAR_CONFIG_HOME`` / ``XDG_CONFIG_HOME/cstar``) on every command and when used from Python. (`#806 <https://github.com/CWorthy-ocean/C-Star/pull/806>`_)

  - The file is dotenv: one ``NAME=value`` per line, ``#`` comments, ``~`` and ``$VAR`` expanded in values.
  - Precedence, highest first: a command-line option, an exported variable, the configuration file, the system's own default, the built-in default.
  - A key the file may not set (an unknown name, a feature flag, ``CSTAR_CONFIG_HOME``, a removed setting, or a name with no value) stops the command with one error naming the file and every offending key.

- ``cstar env set NAME VALUE`` writes or updates one line of the file, creating it on first use and keeping comments; ``cstar env unset NAME`` comments the line out. (`#806 <https://github.com/CWorthy-ocean/C-Star/pull/806>`_)
- ``cstar env show`` prints the configuration file's path and marks each value ``[env]``, ``[config]`` or ``[default]``; ``--display export`` is unchanged. (`#806 <https://github.com/CWorthy-ocean/C-Star/pull/806>`_)
- ``cstar init`` walks through the setup that follows an install and writes ``~/.config/cstar/config.env`` from the answers. (`#809 <https://github.com/CWorthy-ocean/C-Star/pull/809>`_)

  - On a SLURM system it asks for the account, queue and default walltime, listing your ``sacctmgr`` associations and ``sinfo`` partitions when they can be queried, and marks the settings the system requires.
  - It shows the data home, project home and catalog with where each value came from (shell, file, scratch variable, system convention, ``$PROJECT``, XDG or the built-in default), recommends naming a scratch directory on a cluster where none was found, and lets you keep or change each.
  - Pressing Enter keeps a value, ``-`` leaves a setting unset; a kept default is left as a commented line, a value exported in the shell is moved into the file with a note that the ``export`` line can go, and settings already in the file that the command does not ask about are preserved.
  - It then offers to install the ``cstar`` shell function and to register a Jupyter kernel, prints the Copernicus Marine and TPXO registration pointers, and ends with a summary; a skipped or failed optional step is reported and the exit code stays 0 once the file is written.
  - ``cstar init --yes`` accepts every default without prompting, for containers and scripts.

- ``cstar admin clean`` is available without developer mode: ``--run-id <id>`` removes one run's tracking record and run directory, and ``--all`` empties C-Star's data, state, cache and config directories. (`#814 <https://github.com/CWorthy-ocean/C-Star/pull/814>`_)

  - The shared source-data cache and your catalog are kept by ``--all``.

- ``cstar workplan submit`` launches a workplan: it schedules every step and exits, under SLURM or locally. (`#816 <https://github.com/CWorthy-ocean/C-Star/pull/816>`_)

  - ``cstar workplan run`` still works with every option and re-entry form, unlisted in ``--help`` and without a notice.

- ``cstar workplan --help`` lists only ``check`` and ``submit``; ``cstar run`` lists the run-level commands. (`#816 <https://github.com/CWorthy-ocean/C-Star/pull/816>`_)

  - ``cstar workplan status``, ``ls``, ``list``, ``gather``, ``log``, ``path`` and ``cd`` (and their ``cstar wp ...`` forms) still run the same commands as ``cstar run ...``, unlisted and without a notice.
  - ``cstar workplan check <workplan file>`` still validates a workplan file and stays listed; ``cstar run check <run>`` checks a run.

- The upscaler reads each record's averaging window from ``avg_begin_time``/``avg_end_time`` in ``_uscl`` files from ucla-roms after 0.12.0, including a shorter final window. (`#815 <https://github.com/CWorthy-ocean/C-Star/pull/815>`_)










Bug Fixes
~~~~~~~~~


- Two steps staging the same codebase at once could fail or log errors (a failed ``git pull``, a removed cache, "Directory not empty", or a ``cp -av`` exiting 1) because the shared clone cache had no lock. (`#787 <https://github.com/CWorthy-ocean/C-Star/pull/787>`_)

  - A step waits up to 30 minutes for another step to finish with the cache, logging that it is waiting, then clones the repository into its own directory instead.
  - On a file system without file-locking support, staging logs a warning and uses the cache without the lock, as before.

- ``cstar workplan run --resume`` no longer fails when it cannot update the run manifest, for example on a file system without ``flock``; it warns and resumes. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)
- Unit test CI no longer replaces the conda builds of numpy, netCDF4, h5py, numba and other compiled packages with PyPI wheels, so it tests the stack ``environment-laptop.yml`` installs. (`#799 <https://github.com/CWorthy-ocean/C-Star/pull/799>`_)
- ``cstar run check <directory>`` loaded the transformed workplan without rebasing it onto the directory, so a copied run checked by path listed its own step directories as orphans and ``--repair`` never rebuilt its ``gathered_output``. (`#800 <https://github.com/CWorthy-ocean/C-Star/pull/800>`_)
- ``cstar run ls --format json`` and ``--format csv`` produced invalid output when piped or redirected, because long lines were wrapped at 80 columns. (`#801 <https://github.com/CWorthy-ocean/C-Star/pull/801>`_)
- A run whose disk usage was never measured was listed with a size of ``-1MB``. (`#801 <https://github.com/CWorthy-ocean/C-Star/pull/801>`_)

  - JSON now gives ``null`` and CSV an empty field for that size.
  - The table says "not measured", followed by the ``cstar run status <id> --size`` hint for a run tracked on this machine.

- ``cstar blueprint run`` no longer crashes with a traceback when a deferred blueprint or a directive reads a producer step record written by a newer cstar-ocean; it prints the upgrade guidance and exits 1. (`#803 <https://github.com/CWorthy-ocean/C-Star/pull/803>`_)
- ``cstar blueprint run`` no longer prints a traceback when a directive (``continue-from``, ``nest-from``, …) fails with a C-Star error; it prints "Unable to apply directives: …" and exits 1. (`#803 <https://github.com/CWorthy-ocean/C-Star/pull/803>`_)
- Listing and status commands no longer crash on a run manifest or step record from a newer cstar-ocean; they log it and show the run without that information. (`#803 <https://github.com/CWorthy-ocean/C-Star/pull/803>`_)
- The upscaler could combine the child's ``_uscl`` files out of time order, which scrambled the upscaled CDR forcing and made the parent rerun abort. (`#776 <https://github.com/CWorthy-ocean/C-Star/pull/776>`_)

  - It now orders them by the timestamp in their names, and repeated or out-of-order records raise an error that names the record and files.

- C-Star now repairs the broken source-data cache link that 0.16.0 and 0.16.1 created when the C-Star root or the legacy forge data folder sits behind a symlink, which made every forge run fail. (`#776 <https://github.com/CWorthy-ocean/C-Star/pull/776>`_)
- Copying a codebase out of the repository cache no longer fails intermittently because git was still writing into the cache after refreshing it. (`#808 <https://github.com/CWorthy-ocean/C-Star/pull/808>`_)
- A workplan written as ``.json`` could not be run cleanly: its first run exited with an error after scheduling, and ``--run-id`` re-entry and ``--resume`` failed with "expected ident". (`#810 <https://github.com/CWorthy-ocean/C-Star/pull/810>`_)

  - The run's plan was YAML saved under a ``.json`` name, so every reader parsed it as JSON.

- ``cstar run check``, ``cstar run attach`` and ``cstar run promote`` could not find the plan of a run started from a ``.json`` or ``.yml`` workplan. (`#810 <https://github.com/CWorthy-ocean/C-Star/pull/810>`_)
- A blank ``--run-id`` exited with code 1 and no message; it now reports the problem. (`#814 <https://github.com/CWorthy-ocean/C-Star/pull/814>`_)
- ``cstar admin migrate-catalog`` no longer fails after moving blueprints out of a legacy directory that is a symbolic link; the link is reported as left in place. (`#818 <https://github.com/CWorthy-ocean/C-Star/pull/818>`_)
- ``cstar run promote --mode move`` of a run tracked on this machine (and re-attaching a copy of such a run) no longer leaves the run's sentinel files behind under ``run_state/<run-id>/``; those of finished steps are removed and the directory with them when empty, while a sentinel whose step has not finished is kept with a warning. (`#817 <https://github.com/CWorthy-ocean/C-Star/pull/817>`_)
- A ``runs:`` alias pinned to a run directory that has since moved or been removed no longer fails on the stale path: it warns, naming the pinned directory and the run's uuid, finds the run by that uuid and re-pins the alias by uuid; a pinned directory that holds another run is still an error. (`#817 <https://github.com/CWorthy-ocean/C-Star/pull/817>`_)
- On a SLURM system such as Anvil, a workplan step run with the local launcher no longer fails at startup with "Environment settings for the system could not be loaded" when ``CSTAR_SLURM_ACCOUNT`` or ``CSTAR_SLURM_QUEUE`` is unset. (`#820 <https://github.com/CWorthy-ocean/C-Star/pull/820>`_)
- ``parent_up`` applied each hour of upscaled CDR flux one hour late and never applied the child's final hour of export. (`#815 <https://github.com/CWorthy-ocean/C-Star/pull/815>`_)
- With ``CSTAR_SLURM_QUEUE`` unset, a ROMS step run with the local launcher on Perlmutter, Expanse, Derecho or Elja failed with "not found in list of queues"; it now submits to the system's primary queue. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)

  - The primary queues are ``regular`` (Perlmutter), ``compute`` (Expanse), ``main`` (Derecho) and ``any_cpu`` (Elja).
  - With the walltime unset, the job requests the queue's maximum walltime.

- With ``CSTAR_SLURM_QUEUE`` unset, a workplan run with the SLURM launcher on Elja or Perlmutter failed the same way; it now submits to the primary queue. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)
- A PBS job script with no account carried an empty ``-A``; it now leaves ``-A`` out, so PBS charges the project set in ``PBS_ACCOUNT``. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)


Improvements
~~~~~~~~~~~~


- ``ArtifactKind.namelist`` is now ``config``: every file the model read as configuration, told apart by ``metadata.role`` (``cppdefs``, ``namelist``, ``marbl_in``); ``build`` means a compiled executable. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- One module, ``roms_marbl.outputs``, now holds how a ROMS-MARBL step names its output files and the models that parse those names. (`#788 <https://github.com/CWorthy-ocean/C-Star/pull/788>`_)
- ``--resume <workplan>`` and re-runs under a derived run ID compare with the latest attempt's carried workplan; older attempt directories may be deleted by hand. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)

  - When that copy is gone too, ``--resume <workplan>`` warns and resumes the recorded plan, and a derived re-run is refused until ``--run-id`` is given.
  - An unreadable run manifest is reported as such instead of being treated as nothing recorded.

- An attempt refused while it prepares leaves the run as the previous attempt left it: the plan in the run directory is replaced only once the inputs are carried. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- A step reads the files of the step it depends on from that step's record by product kind, falling back to searching its ``output`` directory by name when the record lists none of that kind. (`#790 <https://github.com/CWorthy-ocean/C-Star/pull/790>`_)

  - ``continue-from`` reads the producer's restarts, or, from a step that recorded none, initial-conditions files named like restarts (what ``nest_ic`` writes).
  - ``nest-from`` reads the child boundary extraction, ``carbonate-sensitivity-from`` the gas-exchange sensitivities, the upscaler the ``_uscl`` files, and a deferred blueprint the emitted blueprint.
  - A recorded file that a step reads but that is missing on disk now fails the step with a pointer to ``cstar run check``, instead of the step silently using the files that remain.

- ``cdr_metrics`` reads the ``_cdr`` files a producing step recorded, instead of searching its ``output`` directory, falling back to the search for older runs and plain directories. (`#793 <https://github.com/CWorthy-ocean/C-Star/pull/793>`_)

  - A recorded ``_cdr`` file missing from disk now fails the step, naming the domain and pointing at ``cstar run check``, instead of the step silently using the files that remain.

- ``continue-from``, ``nest-from`` and ``carbonate-sensitivity-from`` read a step's recorded files only from its ``output`` directory, the same directory they search when the record lists none. (`#795 <https://github.com/CWorthy-ocean/C-Star/pull/795>`_)
- The upscaler resolves a linked ``uscl_file_location`` first, so it reads the record of the step the link points to and refuses a recorded ``_uscl`` file missing from disk. (`#795 <https://github.com/CWorthy-ocean/C-Star/pull/795>`_)
- One shared helper now reads a step's recorded files of a kind in one directory, replacing three copies of the same lookup. (`#795 <https://github.com/CWorthy-ocean/C-Star/pull/795>`_)
- Keeping a run's records never stops or stalls the run: a command that waits more than a minute for the manifest lock skips that update with a warning. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)
- YAML files are read with libyaml when PyYAML has it, about five times faster on a large run manifest. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)
- ``cstar run status``, ``gather`` and ``check --repair`` explain that they act on a run tracked on this machine and point at ``cstar run show`` or the run's directory for any other. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)
- An attached run's ``status`` is what its manifest and step records say; ``show`` and ``check`` read its directory as it is, with no sentinel refresh. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- A ``runs:`` alias can depend on the finished steps of an attached run, as for a run known only by its manifest. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- ``cstar run gather`` builds ``gathered_output`` under the run's recorded directory, so an attached run outside the data home is gathered in place. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- The netCDF digest (``hash_netcdf_contents``) moved from the forge application to ``cstar.base.digest``, unchanged, so orchestration can use it without reaching into an application module. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- Building a subchunked GLORYS reference no longer patches asyncio for the rest of the forge step, which on Python 3.14 broke asyncio timeouts, fsspec I/O and event-loop shutdown. (`#804 <https://github.com/CWorthy-ocean/C-Star/pull/804>`_)
- The local launcher checks a waiting step's dependencies every 2 s instead of every 10 s, so local workplan steps start sooner. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)
- ``cstar workplan run --resume`` no longer re-exports removed settings recorded in an older run's environment. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)
- ``--yes`` is now a shared CLI option constant, used by ``cstar admin clean`` and ``cstar init``. (`#809 <https://github.com/CWorthy-ocean/C-Star/pull/809>`_)
- ``cstar run promote`` copies files in 64 MiB blocks: on Anvil's GPFS a run now copies at 1,750 MiB/s, 4.6× faster than before and 4× faster than ``rsync -a``. (`#811 <https://github.com/CWorthy-ocean/C-Star/pull/811>`_)
- Each run attempt now always saves its workplan as ``workplan.yaml``, whatever the authored file's suffix. (`#812 <https://github.com/CWorthy-ocean/C-Star/pull/812>`_)
- ``cstar admin clean`` exits 0 when a run ID has nothing to remove, instead of 99. (`#814 <https://github.com/CWorthy-ocean/C-Star/pull/814>`_)
- ``cstar admin clean --dry-run`` lists what would be removed without asking about each item. (`#814 <https://github.com/CWorthy-ocean/C-Star/pull/814>`_)
- ``cstar init`` shows the run-output, project and catalog locations and asks one question, "Use C-Star's default locations?" (or "Use these locations?" when any is customised), before prompting for each. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)

  - Answering no walks through each location as before; ``--yes`` keeps them all.
  - On a cluster where no scratch directory was found, the question defaults to no, so pressing Enter leads to the prompts.

- ``cstar init`` introduces the shell function as "'cstar run cd' shell support": the ``cstar run cd`` command requires a shell function to operate. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)
- Hints and error messages that suggest launching or re-entering a run now name ``cstar workplan submit``. (`#816 <https://github.com/CWorthy-ocean/C-Star/pull/816>`_)
- The workplan builder in the wizard labels its launch button **Submit** and streams ``cstar workplan submit``. (`#816 <https://github.com/CWorthy-ocean/C-Star/pull/816>`_)
- The tracking record no longer stores the workplan name, start time, authored workplan path or transformed workplan path, nor the ``name``, ``attached`` and ``access`` metadata keys; it holds the run directory, sentinels, environment, user variables, measured step sizes, the cached uuid, typed ``attached`` and ``access`` fields, and ``recorded_at`` (when the record was written on this machine). (`#817 <https://github.com/CWorthy-ocean/C-Star/pull/817>`_)

  - The name, start time and workplan paths are read from the run's manifest (``cstar-run.yaml``), so ``ls``, ``status``, ``show`` and ``--resume`` see the run as it is, wherever its directory now is.
  - A record written by 0.17.0 or earlier still loads: its attached and access marks become the typed fields, and what it held that this release derives is kept under ``legacy`` and used only for a run that has no manifest to ask (one scheduled before the manifest existed, or whose directory is gone).
  - A record carrying an ``access`` value this release does not know is read as read-only with a warning, so the run stays listed and nothing writes into its directory.

- ``cstar run ls`` without ``--all`` or ``--shared`` reads only the tracking records in your state home and each tracked run's manifest for its name and start time; it scans no run directories. (`#817 <https://github.com/CWorthy-ocean/C-Star/pull/817>`_)
- Submitting a SLURM job on a system that requires an account and queue, without them, now fails before anything is submitted, naming every missing setting and pointing to ``cstar init``. (`#820 <https://github.com/CWorthy-ocean/C-Star/pull/820>`_)
- The upscaler refuses a set of ``_uscl`` files with a missing file, a gap or overlap between windows, or a mix of ucla-roms versions, and names the offending file. (`#815 <https://github.com/CWorthy-ocean/C-Star/pull/815>`_)
- ``upscaled_cdr.nc`` records its time convention (``cdr_time:convention``) and where its windows came from (``uscl_windows``). (`#815 <https://github.com/CWorthy-ocean/C-Star/pull/815>`_)
- On Expanse, which requires a project on every job, a missing ``CSTAR_SLURM_ACCOUNT`` now stops a SLURM-launcher run at start and refuses a ROMS job before submission, naming the setting and ``cstar init``. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)
- ``cstar init`` on Expanse marks the SLURM account as required on this system. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)



Miscellaneous
~~~~~~~~~~~~~

- The "Re-running a workplan" docs state the rule and its two escape hatches. (`#783 <https://github.com/CWorthy-ocean/C-Star/pull/783>`_)
- Docs: a "Declaring Products" section with the kinds table in the custom-applications guide, the products and product checks in the workplans guide, the Product term; the published run schemas' ``kind`` examples list ``config``. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- The hello_world e2e asserts empty products and no product findings; the Tier 2 forge-to-ROMS e2e asserts both records list their products, ``show`` totals are non-zero and ``check`` finds no product problem. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- Workplans guide: new "Computing CDR metrics from segmented runs" section, including a note that flux-based metrics currently differ across a restart while DIC-based metrics match. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Published blueprint schema ``cdr_metrics`` 1.1.0. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Tier 2 e2e: the MARBL forge step gains an ALK release; two new ROMS steps form a ``continue-from`` chain; two ``cdr_metrics`` steps compute metrics from the whole run and from the segments. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Published the run manifest and step record schemas v1.1.0, and documented carried inputs in the workplan guide and terminology. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- The directives guide gains a "Finding a step's files" section, and the deferred-blueprint note describes how the blueprint is found without ``filename``. (`#790 <https://github.com/CWorthy-ocean/C-Star/pull/790>`_)
- The wizard notebook (``cstar forge copy-notebook``) now links to the C-Star wizard documentation instead of the archived cstar-forge docs site. (`#792 <https://github.com/CWorthy-ocean/C-Star/pull/792>`_)
- The workplans guide describes how ``cdr_metrics`` finds its sources and records its products, and the custom-applications guide lists the ``analysis`` kind. (`#793 <https://github.com/CWorthy-ocean/C-Star/pull/793>`_)
- New tutorial "Automating your simple model with workplans": the end-to-end run as a two-step workplan with a deferred blueprint, with real ``cstar run status``, ``show`` and ``ls`` output, plus ``log``, ``path`` and ``cd``. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- New tutorial "Spin-up, continuation and a nested grid": a six-step workplan using ``blueprint_overrides`` (a shorter spin-up time step), ``continue-from``, an inline ``nest_ic`` step and a two-source ``nest-from``, with a roms-tools plot of the nested grids and a screenshot of the wizard's nesting controls. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- Ready-made forge blueprints for the wio-toy outer grid and a 2000 km child grid nested in it, with tides and rivers off so no TPXO download is needed. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- The end-to-end tutorial is retitled "Using Blueprints to Set Up and Run an Ocean Model" and points to the workplan page as its next step. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- Published the run manifest and step record schemas v1.2.0, and documented execution facts in the workplan guide and terminology. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)
- Published the workplan schema v1.1.0 and the run manifest schema v1.3.0, and documented run handles, extending a finished run, the project runs area and ``ls --shared``/``--all``. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)
- ``pio-dev`` (tracks ucla-roms ``main``) carries the new key; the packaging check lists the new ModelSpec. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)
- The integration test catalog, the weekly extended workflow's release cell and the Tier 1 namelist goldens move to ucla-roms 0.12.0. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)
- Forge developer docs, the contributing guide and the ModelSpec docs describe the 0.12 tier and the new default spec; new unit goldens for the 0.12 spec differ from the 0.11 ones by exactly the new key. (`#797 <https://github.com/CWorthy-ocean/C-Star/pull/797>`_)
- Run manifest schema 1.4.0 and step record schema 1.3.0 published (both add ``digest``); older documents load unchanged. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- Docs: "Attaching a run that arrived from elsewhere", the ``--verify`` / ``--record-digests`` paragraphs of "Checking a Run Directory", the ``where`` labels, and glossary entries for Attach and the product digest. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- A hello_world e2e (``test_run_attach.py``, seconds) covers attach, verify, owned and read-only verbs, re-entry refusal, ``--as``, and a workplan depending on an attached run by its run-id. (`#798 <https://github.com/CWorthy-ocean/C-Star/pull/798>`_)
- Unit test CI runs about twice as fast by measuring coverage with Python's ``sys.monitoring`` instead of the default tracer. (`#799 <https://github.com/CWorthy-ocean/C-Star/pull/799>`_)
- Unit test CI logs stay readable: test output is captured again, and each run lists its 25 slowest tests. (`#799 <https://github.com/CWorthy-ocean/C-Star/pull/799>`_)
- Documentation: "Promoting a run" in the workplans page, the project runs area paragraph in the HPC page (with the umask/group-permission note, since C-Star changes no permissions) and a "Promote" terminology entry. (`#800 <https://github.com/CWorthy-ocean/C-Star/pull/800>`_)
- Tier 2 e2e module ``test_run_promote.py`` (hello_world, seconds): copy, dry run, ``--to``, link then materialize, a workplan depending on the promoted run by uuid, move with the tracking update and relaunch refusal, ``--verify``, ``gathered_output`` rebuild. (`#800 <https://github.com/CWorthy-ocean/C-Star/pull/800>`_)
- The workplan tutorial's sample ``cstar run ls`` table shows the new "not measured" wording. (`#801 <https://github.com/CWorthy-ocean/C-Star/pull/801>`_)
- The HPC guide says what ``cstar forge show-paths`` prints, including the project runs area. (`#802 <https://github.com/CWorthy-ocean/C-Star/pull/802>`_)
- ``nest-asyncio`` is no longer a dependency of ``cstar-ocean`` or of the conda environment files. (`#804 <https://github.com/CWorthy-ocean/C-Star/pull/804>`_)
- New tutorial pages on nesting and upscaling: a walk-through of the tiny case and a high-level page for the California Current case. They replace the unlinked placeholder workplan sketch in ``docs/tutorials``. (`#776 <https://github.com/CWorthy-ocean/C-Star/pull/776>`_)
- The weekly extended integration tests run the tiny tutorial end to end. They check its CDR metrics and that the parent rerun injects exactly the upscaled flux. (`#776 <https://github.com/CWorthy-ocean/C-Star/pull/776>`_)
- Documentation: the configuration page's variable table no longer lists the removed settings or ``CSTAR_RUNID``, and the workplan tutorial no longer mentions ``CSTAR_CLOBBER_WORKING_DIR``. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)
- Containers: the runner and NERSC images no longer set the unused ``CSTAR_INTERACTIVE``. (`#805 <https://github.com/CWorthy-ocean/C-Star/pull/805>`_)
- The e2e step-log checks now show the lines after each error, so a failed command's stderr appears in the assertion message. (`#808 <https://github.com/CWorthy-ocean/C-Star/pull/808>`_)
- The configuration page documents the file, its precedence and the new commands; the HPC and installation pages point to it beside the existing ``export`` example. (`#806 <https://github.com/CWorthy-ocean/C-Star/pull/806>`_)
- The configuration page gains "Where a setting can be set, and which wins": the full precedence chain, the SLURM layers a workplan and its steps add, and what ``--resume`` re-exports. (`#806 <https://github.com/CWorthy-ocean/C-Star/pull/806>`_)
- The installation page's "After installing" section now starts with ``cstar init`` and keeps the kernel, configuration and shell-function steps as its sub-steps; the HPC page's scheduler section leads with ``cstar init`` and keeps the ``config.env`` example; the configuration page points at ``cstar init`` as the way to create the file. (`#809 <https://github.com/CWorthy-ocean/C-Star/pull/809>`_)
- Removed the ``workplan.json`` mention from the workplans guide. (`#812 <https://github.com/CWorthy-ocean/C-Star/pull/812>`_)
- New "Cleaning up" section in the workplans guide. (`#814 <https://github.com/CWorthy-ocean/C-Star/pull/814>`_)
- The installation guide describes the new ``cstar init`` locations question. (`#813 <https://github.com/CWorthy-ocean/C-Star/pull/813>`_)
- Docs and tutorials use ``cstar workplan submit`` and ``cstar run <command>`` throughout, and note that the earlier spellings still work. (`#816 <https://github.com/CWorthy-ocean/C-Star/pull/816>`_)
- Blueprint schema 3.3.0 (a minor bump: older files load and run unchanged) is published as ``ucla_roms_schema.3.3.0.json``, alongside run manifest schema 1.5.0. (`#818 <https://github.com/CWorthy-ocean/C-Star/pull/818>`_)
- Docs: the blueprint page moved to ``blueprints/ucla_roms``; examples, tutorials and templates use ``application: ucla_roms``, with a note wherever the old name is still accepted. (`#818 <https://github.com/CWorthy-ocean/C-Star/pull/818>`_)
- Wizard labels, hints and log messages say UCLA-ROMS, keeping "with MARBL" where the output needs MARBL. (`#818 <https://github.com/CWorthy-ocean/C-Star/pull/818>`_)
- The run docs state what ``cstar run ls`` reads without flags. (`#817 <https://github.com/CWorthy-ocean/C-Star/pull/817>`_)
- The workplans page gains a "Known limitations" section: what you see and what to do for each known edge of re-entering, promoting and recording runs. (`#819 <https://github.com/CWorthy-ocean/C-Star/pull/819>`_)
- The workplans page states the linked-promotion policy: a ``promote --mode link`` run may stay linked indefinitely, shows as ``project, linked``, and its owner materializes it before a scratch purge. (`#819 <https://github.com/CWorthy-ocean/C-Star/pull/819>`_)
- The workplans page notes that on a scheduler system every step, even one run locally, needs ``CSTAR_SLURM_ACCOUNT`` and ``CSTAR_SLURM_QUEUE``, which ``cstar init`` sets. (`#819 <https://github.com/CWorthy-ocean/C-Star/pull/819>`_)
- The HPC guide now says the SLURM settings are needed only to submit jobs. (`#820 <https://github.com/CWorthy-ocean/C-Star/pull/820>`_)
- The nesting and upscaling tutorial drops its version-sensitive reference-numbers table and the known issue this fixes, and says what the weekly test's closure bound does and does not check. (`#815 <https://github.com/CWorthy-ocean/C-Star/pull/815>`_)
- The CCS developer page notes that its resource numbers predate the fix and that ``parent_up`` now receives about 1.5 % more alkalinity over a month. (`#815 <https://github.com/CWorthy-ocean/C-Star/pull/815>`_)
- The HPC guide lists, for each system, which SLURM settings are required and which queue a job uses when ``CSTAR_SLURM_QUEUE`` is unset. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)
- The workplans guide's Execution note no longer says every step fails at start-up without the SLURM account and queue; it names the settings each system requires and when they are checked. (`#821 <https://github.com/CWorthy-ocean/C-Star/pull/821>`_)
