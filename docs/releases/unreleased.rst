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
- A relative ``blueprint:`` path in a workplan step no longer fails at launch with "Blueprint file not found"; it is resolved against the workplan file's directory, whether ``cstar workplan run`` is invoked from that directory or elsewhere. (`#753 <https://github.com/CWorthy-ocean/C-Star/pull/753>`_)

  - ``cstar workplan check`` resolves relative paths the same way, so a workplan that passes ``check`` is one ``run`` accepts.
  - The transformed workplan written into the run directory records the resolved absolute path.
  - ``{{placeholder}}`` paths are filled first and then anchored; ``inline``, ``from_step``, remote URIs and absolute paths are left untouched.


Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- The workplan docs and the ``Step.blueprint_path`` docstring state that relative paths are resolved against the workplan file's location. (`#753 <https://github.com/CWorthy-ocean/C-Star/pull/753>`_)
