.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- The local launcher now behaves like the SLURM launcher on ``cstar workplan run --run-id <id>``: completed steps are reused instead of re-executed, in-flight steps are adopted, and failed steps are re-run from scratch. Previously a local reload re-submitted every step. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- A local step whose process died without finalizing its status (crash, ``kill -9``, reboot) is now reported as failed and re-run on reload; previously it was treated as still running. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- Re-running or resuming a step no longer truncates its previous log: ``logs/<step>.out`` is rotated to ``<step>.out.1``, ``.2``, … and the new log opens with a line naming the attempt kind and the prior log. A repeated ``ROMSSimulation.run()`` likewise rotates the previous namelist, job script and ROMS stdout instead of overwriting them. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)

New features
~~~~~~~~~~~~


- ``cstar workplan run --run-id <id> --resume`` resumes every failed step whose application is resumable; failed steps of other applications are re-run from scratch with a warning, and completed steps are reused. ``--resume`` cannot be combined with ``--clobber`` or a fresh workplan path. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- ``cstar blueprint run <blueprint> --resume`` resumes an interrupted standalone run from the blueprint's working directory; the flag is rejected up front for applications that do not declare themselves resumable. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- ``ApplicationDefinition.resumable`` (default ``False``) and ``RunnerRequest.resume`` let an application declare and honour resume support; ``roms_marbl`` is resumable. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- ROMS-MARBL resume: the runner writes a derived ``<blueprint>.resume.yaml`` in the step's ``work/`` directory whose initial conditions and ``start_date`` point at the newest complete, readable restart (partition set in ``temp_output`` for non-ParallelIO runs, whole file in ``output`` for ParallelIO), leaves the original blueprint untouched, and attaches the existing working directory instead of staging, cloning, compiling or partitioning. With no usable restart the run resumes from the original initial conditions; a restart at or after ``end_date`` is rejected as an already-finished run. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- ``ROMSSimulation.attach()`` and matching ``attach()`` methods on ``ExternalCodeBase``, ``AdditionalCode``, ``InputDataset`` and ``ROMSInputDataset`` (plus ``attach_partitions``) adopt what a previous attempt left on disk, reporting every missing component in a single error. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)

Bug Fixes
~~~~~~~~~


- The orchestrator measured the whole run output directory with ``du`` on every step launch and status change, and did so concurrently for every changed step on reload; on large runs this added a full directory walk to each status update. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Two steps of the same run finishing in the same poll tick could overwrite each other's run-record update, losing a sentinel or a recorded size; run-record updates from the status hook and from ``--size`` now hold the run lock across the whole read-modify-write. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Step-name tab completion for ``--step`` returned nothing for a run with no tracking record instead of falling back to the run's ``tasks/`` directory as documented (regression in #681). (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Resolving a deferred blueprint against a missing, invalid or truncated transformed workplan reported a generic "No live workplan found" message, hiding the real cause; a YAML syntax error could escape as an unhandled traceback from ``cstar blueprint run``. The underlying error is now included and chained. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- ``cstar workplan ls`` deserialized every run's transformed workplan even when the run's name was already cached in its record. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- A workplan step no longer fails at startup when its directive file has gone missing — for example because its working directory was cleaned while the job waited in a scheduler queue. The file is restored from the workplan recorded for the run. (`#694 <https://github.com/CWorthy-ocean/C-Star/pull/694>`_)
- A ``--directives`` value pointing at a remote URI (``https://...``) no longer always fails with "Directive file not found"; the downloaded copy is validated rather than the original URI. (`#694 <https://github.com/CWorthy-ocean/C-Star/pull/694>`_)
- A blank or whitespace-only run-id no longer produces a misleading "Directive file is malformed" error for a file that was never read. (`#694 <https://github.com/CWorthy-ocean/C-Star/pull/694>`_)
- A failure to write the restored directive file now surfaces as a clean parameter error instead of an unhandled traceback. (`#694 <https://github.com/CWorthy-ocean/C-Star/pull/694>`_)
- A reloaded local step whose recorded status was ``Failed`` or ``Cancelled`` was reported as completed and reused; it is now re-run. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- Cancelling a reloaded local step whose process was gone raised ``AttributeError`` instead of being skipped. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- Environment variables set while configuring a codebase (``ROMS_ROOT``, ``MARBL_ROOT``, ``PIO_ROOT``) were not visible to ``environment_variables``, so ``is_configured`` could not see a codebase configured in the same process. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- ``ROMSSimulation.run()`` raised ``FileExistsError`` on the ``roms`` symlink when called a second time in the same working directory. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)

Improvements
~~~~~~~~~~~~


- ``cstar workplan ls --size`` and ``cstar workplan status <run-id> --size`` fall back to the run's ``tasks/`` directory to find step directories when the transformed workplan file is no longer available, so cleaned-up runs can still be measured. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- The ``status`` table caption reports the total step output and when it was last measured, and tells you the exact ``--size`` command to run when it has not been measured. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Disk usage is exposed through a single ``WorkplanRun.size_mb`` accessor instead of three separate parses of the metadata string. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- ``cstar workplan run --run-id`` reload branching uses an explicit flag rather than a check on optional paths. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Stale run ids with unreadable workplans are reported once at debug level in ``ls`` rather than one warning per run. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Minor cleanups from the #681 review: ``KEY_RUN_NAME`` constant, ``ItemView`` model config, ``max_concurrency`` catches ``ValueError`` only, restored ``get_backup_path`` docstring. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- When a missing directive file cannot be restored, the error explains why: no run-id is set, the workplan recorded for the run could not be loaded, no step in it writes to that path, or the file could not be written. (`#694 <https://github.com/CWorthy-ocean/C-Star/pull/694>`_)
- The prior-attempt reuse decision is shared by the SLURM and local launchers in one helper instead of living only in the SLURM launcher. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- Codebase environment export is separated from compilation (``_export_env``), so a codebase can be adopted without rebuilding it. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- ``build()``'s ``cppdefs.opt`` consistency checks for ``PARALLEL_IO`` and ``MPI_MASKING`` are shared with ``attach()``, so a resumed run fails as loudly as a fresh build on a mismatched configuration. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- ``RestartFile.candidates()`` is the single restart-file enumeration used by ``find()`` and the resume logic. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
- Directive source keys ``path``/``step`` are shared module constants used by both ``continue-from`` and ``nest-from``. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)

Miscellaneous
~~~~~~~~~~~~~

- Documentation for ``--resume`` in the blueprint and workplan guides and for ``resumable`` in the custom-applications guide. (`#695 <https://github.com/CWorthy-ocean/C-Star/pull/695>`_)
