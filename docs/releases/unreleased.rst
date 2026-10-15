.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- A workplan ``name`` must be slug-stable (``north-atlantic-2012``, not ``North Atlantic 2012``); a non-slug name is refused with the slug to use, which is the run-id that name already produced, so existing runs stay reachable. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

  - A slug is ASCII: lowercase letters, digits, ``_`` and single ``-``; ``slugify`` now reduces accented letters to their base letter (``Étude Ü`` gives ``etude-u``) instead of keeping them, which changes the derived id only for names that contained non-ASCII characters.
  - The bundled workplan templates and the docs examples are renamed accordingly, and the wizard's name field says so.
  - The workplan schema version stays 1.0.0; this is the only narrowing of what a valid workplan file may contain.
  - The transformed workplan saved in the run directory keeps the workplan ``name`` instead of being renamed to ``<name> (transformed)``; files written by earlier releases still load under the run-id they produced, and ``cstar workplan ls`` shows the name without the old suffix.

- Re-running a different workplan file under a run-id derived from the same name is refused unless its content matches the run's recorded original; pass ``--run-id`` to choose another id or to re-enter deliberately. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

  - Editing the recorded file in place (adding steps as a run grows) never trips the guard, and one ``--run-id <id>`` re-entry re-records the new file and original, so later runs need no flag.

- ``cstar workplan status <unknown-id>`` exits with status 2 instead of printing a message and exiting 0. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan run --resume`` no longer re-exports the ``CSTAR_DATA_HOME`` and ``CSTAR_STATE_HOME`` captured when the run started; a reloaded run resolves against the current ones, and one warning names any other captured ``CSTAR_*`` setting that differs from the shell. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)

New features
~~~~~~~~~~~~


- ``cstar workplan run --resume <id>`` now explains that ``--resume`` takes no value and shows the ``--run-id <id> --resume`` form instead of "workplan not found". (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan run <run>/<stem>_transformed.yaml`` re-enters that run exactly as ``--run-id <id>`` does, instead of failing validation; the run-id comes from the run directory holding the file, so a run started under an explicit ``--run-id`` is found too. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

  - A copy of the file anywhere else is refused with the ``--run-id`` way in, and ``--pre-run`` with a transformed workplan is refused with a message asking for the authored one.

- Orphaned step directories and sentinels left by renamed or removed steps are reported on re-entry, listed with their paths and left in place. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- A run directory loads from wherever it sits: the transformed workplan records its run root (``root``), and every reader rebases the paths under that root onto the directory the file is read from; files written by earlier releases carry no root and load exactly as before. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)

  - Moving one run between data homes on the same machine works end to end: move ``workplan_runs/<id>`` from the old ``CSTAR_DATA_HOME`` to the new one and point ``CSTAR_DATA_HOME`` at it; ``status``, ``path``, ``gather``, ``continue-from`` and ``--resume`` all resolve there.
  - A run record follows a run only when its recorded directory is gone and the current data home holds ``workplan_runs/<id>``; a run copied to an arbitrary path, or to another machine, has valid internal paths but is not reachable by run-id until ``attach`` (Phase 2).


Bug Fixes
~~~~~~~~~


- Re-running a workplan overwrote the previous attempt's ``_original.bak``, ``_transformed.yaml`` and ``.vars`` in place; they are now rotated per attempt (``.1``, ``.2``, ...). (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- A ``.vars`` file from an earlier attempt survived a later attempt that passed no variables. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan check`` accepted an authored workplan with an invalid name by treating it as an already transformed one. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Attaching large user-provided netCDF files in the wizard, or verifying them at forge staging, no longer reads each file whole into memory (a login-node hang or kernel kill with tens of ``_cdrgas`` files). (`#769 <https://github.com/CWorthy-ocean/C-Star/pull/769>`_)
- Resuming a run from a shell that no longer exports the original ``CSTAR_DATA_HOME`` recorded an output directory under the wrong data home; the record now takes the root from the transformed workplan it was given. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- A transient HTTP error (e.g. GitHub rate limiting) while inspecting a remote input dataset no longer gets it rejected as "a text file (e.g. a roms-tools YAML)"; C-Star now reports the HTTP error. (`#770 <https://github.com/CWorthy-ocean/C-Star/pull/770>`_)
- Fetching a remote workplan or blueprint no longer times out after 1 second; timeouts and connection failures are reported as "Unable to retrieve remote file" like other retrieval failures. (`#770 <https://github.com/CWorthy-ocean/C-Star/pull/770>`_)

Improvements
~~~~~~~~~~~~


- The ``--run-id`` help text says the id defaults to the workplan name. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Looking up whether a mistyped path is a run-id no longer creates lock files or reads run records. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- A user-provided file's ``content_hash`` is now ``nc-sample-v1:<sha256>``: the netCDF structure, variables up to 1 MiB in full, and up to 64 evenly spaced rows (always the first and last record, at most 2 MiB) of each larger variable, independent of format, chunking and compression. (`#769 <https://github.com/CWorthy-ocean/C-Star/pull/769>`_)

  - A blueprint recorded with the earlier full-content digest, or with a scheme this C-Star does not know, is accepted with one warning that the file was not verified; re-saving the blueprint refreshes it.
  - The wizard's attached-file status and the staging mismatch warning show the digest's hex rather than its scheme tag.

- ``LiveWorkplan.load``, ``try_load`` and ``load_all`` replace ad-hoc deserialization at every reader of a transformed workplan; ``rebase_paths``, ``read_all`` and ``read_document`` are shared helpers. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- Following a moved run tolerates directories that cannot be inspected and runs off the event loop when listing runs. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)


Miscellaneous
~~~~~~~~~~~~~

- Docs: a "Re-running a workplan" section with the reuse rules (done steps reused unless clobbered, failed steps re-run unless resumed, in-progress steps adopted when the launcher matches, the collision rule, rotation, orphans), and the terminology entry for run ID. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Unit tests serve the bundled workplan templates for their GitHub URLs instead of fetching them from ``main``. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Forge blueprint reference and the ``UserProvidedFile`` docstring describe the new digest. (`#769 <https://github.com/CWorthy-ocean/C-Star/pull/769>`_)
- Docs: the workplans page describes the recorded root, which moves work by run-id today, and which wait for ``attach``. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- New end-to-end test ``test_workplan_relocation.py`` (hello_world steps, no ROMS build) that copies a whole data home, repoints ``CSTAR_DATA_HOME`` and exercises ``status``, ``path``, ``gather``, ``continue-from`` and ``--resume`` from the copy. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- Unit tests no longer reach the network except for pooch-cached dataset downloads, and the unit CI workflow caches those downloads. (`#770 <https://github.com/CWorthy-ocean/C-Star/pull/770>`_)
