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


- ``cstar workplan run`` no longer rejects a step whose blueprint path contains a ``{{ }}`` placeholder (a runtime variable or ``{{output_dir: step@alias}}``); the path is filled when the workplan is prepared, as documented and as ``cstar workplan check`` already allowed. (`#752 <https://github.com/CWorthy-ocean/C-Star/pull/752>`_)

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- N/A
