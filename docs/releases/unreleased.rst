.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- CSTAR_CLOBBER_WORKING_DIR is no longer used to clobber all workplan steps. Use ``cstar workplan run workplan.yaml --clobber all`` instead. (`#635 <https://github.com/CWorthy-ocean/C-Star/pull/635>`_)

New features
~~~~~~~~~~~~


- Add the option to clobber individual steps with ``--clobber <step name>``. Clobbering all steps is achieved via ``--clobber all``. (`#635 <https://github.com/CWorthy-ocean/C-Star/pull/635>`_)

Bug Fixes
~~~~~~~~~

- N/A

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- N/A
