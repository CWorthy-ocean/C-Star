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


- Re-entering a run refuses a step that reached ``Done`` under a different definition, naming the steps and the exact ``--clobber <step>`` to pass, or ``--run-id <other-id>`` to start a new run. (`#783 <https://github.com/CWorthy-ocean/C-Star/pull/783>`_)

  - A step's definition covers its blueprint file's content, ``blueprint_overrides``, ``directives``, dependencies and application; runtime ``--var`` values, ``compute_overrides`` and ``workflow_overrides`` are not part of it.
  - A step whose producer changed through ``from_step`` is named too; a step new to the workplan, a step that failed, or one recorded by an earlier release without a definition hash, is not checked.
  - A run manifest that cannot be read also refuses the re-entry; one written by a newer release keeps the "upgrade cstar-ocean" guidance.

Bug Fixes
~~~~~~~~~

- N/A

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- The "Re-running a workplan" docs state the rule and its two escape hatches. (`#783 <https://github.com/CWorthy-ocean/C-Star/pull/783>`_)
