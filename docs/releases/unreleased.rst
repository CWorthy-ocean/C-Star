.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- ``BlueprintMigration`` protocol deprecates aggregate plan-and-migrate method (`#688 <https://github.com/CWorthy-ocean/C-Star/pull/688>`_)

New features
~~~~~~~~~~~~


- Add ability to perform an in-place migration with backup of original blueprint (`#688 <https://github.com/CWorthy-ocean/C-Star/pull/688>`_)

Bug Fixes
~~~~~~~~~


- Fix unexpected migration parameter callback execution order (`#688 <https://github.com/CWorthy-ocean/C-Star/pull/688>`_)

Improvements
~~~~~~~~~~~~


- Add debug logging during migration operations (`#688 <https://github.com/CWorthy-ocean/C-Star/pull/688>`_)
- Relocate ``rich.console.Console()`` instance to correct shared module (avoid circular deps) (`#688 <https://github.com/CWorthy-ocean/C-Star/pull/688>`_)
- Relocate rich output utilities to correct shared module for re-use (`#688 <https://github.com/CWorthy-ocean/C-Star/pull/688>`_)
- Partially complete docstrings in affected code have been completed throughout (`#688 <https://github.com/CWorthy-ocean/C-Star/pull/688>`_)

Miscellaneous
~~~~~~~~~~~~~

- N/A
