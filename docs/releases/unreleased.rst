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


- New ``cstar workplan gather <run-id>`` command builds a run-level ``joined_output/`` directory of relative symlinks pointing at every file in the per-step ``joined_output`` directories. It is safe to re-run at any point — including while a workplan is still in progress — and each invocation replaces the symlink set with one reflecting the current state on disk. If two steps produce a file with the same name, the command aborts with an error listing every conflicting filename (and modifies nothing). (`#657 <https://github.com/CWorthy-ocean/C-Star/pull/657>`_)

Bug Fixes
~~~~~~~~~


- Ensure SLURM queue name is specified when starting a simulation (`#659 <https://github.com/CWorthy-ocean/C-Star/pull/659>`_)

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- Added a "Gathering Workplan Outputs" section to the workplans documentation. (`#657 <https://github.com/CWorthy-ocean/C-Star/pull/657>`_)
