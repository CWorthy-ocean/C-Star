.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~

- N/A

New features
~~~~~~~~~~~~

- N/A

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

Improvements
~~~~~~~~~~~~


- ``cstar workplan ls --size`` and ``cstar workplan status <run-id> --size`` fall back to the run's ``tasks/`` directory to find step directories when the transformed workplan file is no longer available, so cleaned-up runs can still be measured. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- The ``status`` table caption reports the total step output and when it was last measured, and tells you the exact ``--size`` command to run when it has not been measured. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Disk usage is exposed through a single ``WorkplanRun.size_mb`` accessor instead of three separate parses of the metadata string. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- ``cstar workplan run --run-id`` reload branching uses an explicit flag rather than a check on optional paths. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Stale run ids with unreadable workplans are reported once at debug level in ``ls`` rather than one warning per run. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- Minor cleanups from the #681 review: ``KEY_RUN_NAME`` constant, ``ItemView`` model config, ``max_concurrency`` catches ``ValueError`` only, restored ``get_backup_path`` docstring. (`#693 <https://github.com/CWorthy-ocean/C-Star/pull/693>`_)
- When a missing directive file cannot be restored, the error explains why: no run-id is set, the workplan recorded for the run could not be loaded, no step in it writes to that path, or the file could not be written. (`#694 <https://github.com/CWorthy-ocean/C-Star/pull/694>`_)

Miscellaneous
~~~~~~~~~~~~~

- N/A
