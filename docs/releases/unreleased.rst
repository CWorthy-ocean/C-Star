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

New features
~~~~~~~~~~~~


- ``cstar workplan run --resume <id>`` now explains that ``--resume`` takes no value and shows the ``--run-id <id> --resume`` form instead of "workplan not found". (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan run <run>/<stem>_transformed.yaml`` re-enters that run exactly as ``--run-id <id>`` does, instead of failing validation; the run-id comes from the run directory holding the file, so a run started under an explicit ``--run-id`` is found too. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

  - A copy of the file anywhere else is refused with the ``--run-id`` way in, and ``--pre-run`` with a transformed workplan is refused with a message asking for the authored one.

- Orphaned step directories and sentinels left by renamed or removed steps are reported on re-entry, listed with their paths and left in place. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

Bug Fixes
~~~~~~~~~


- Re-running a workplan overwrote the previous attempt's ``_original.bak``, ``_transformed.yaml`` and ``.vars`` in place; they are now rotated per attempt (``.1``, ``.2``, ...). (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- A ``.vars`` file from an earlier attempt survived a later attempt that passed no variables. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan check`` accepted an authored workplan with an invalid name by treating it as an already transformed one. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

Improvements
~~~~~~~~~~~~


- The ``--run-id`` help text says the id defaults to the workplan name. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Looking up whether a mistyped path is a run-id no longer creates lock files or reads run records. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

Miscellaneous
~~~~~~~~~~~~~

- Docs: a "Re-running a workplan" section with the reuse rules (done steps reused unless clobbered, failed steps re-run unless resumed, in-progress steps adopted when the launcher matches, the collision rule, rotation, orphans), and the terminology entry for run ID. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Unit tests serve the bundled workplan templates for their GitHub URLs instead of fetching them from ``main``. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
