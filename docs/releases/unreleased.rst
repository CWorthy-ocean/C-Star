.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- The return type of `Launcher.update_status` has been modified (`#579 <https://github.com/CWorthy-ocean/C-Star/pull/579>`_)
- Registration of _step-to-command_ mapping functions is removed. (`#585 <https://github.com/CWorthy-ocean/C-Star/pull/585>`_)

New features
~~~~~~~~~~~~

- N/A

Bug Fixes
~~~~~~~~~


- Fix failure to cache updated status during status checks (`#579 <https://github.com/CWorthy-ocean/C-Star/pull/579>`_)

Improvements
~~~~~~~~~~~~


- Avoid unnecessary writes of run-state files with no changes (`#579 <https://github.com/CWorthy-ocean/C-Star/pull/579>`_)
- Ensure status changes are tracked for non-terminal statuses (`#579 <https://github.com/CWorthy-ocean/C-Star/pull/579>`_)
- mitigate private usage warnings (`#581 <https://github.com/CWorthy-ocean/C-Star/pull/581>`_)
- mitigate unknown type warning (`#581 <https://github.com/CWorthy-ocean/C-Star/pull/581>`_)
- convert `TemplateFillTransform` to pure method without side-effects on input (`#581 <https://github.com/CWorthy-ocean/C-Star/pull/581>`_)
- remove unnecessary null check (`#581 <https://github.com/CWorthy-ocean/C-Star/pull/581>`_)
- mitigate graph type partially known warning (`#581 <https://github.com/CWorthy-ocean/C-Star/pull/581>`_)
- add a class-level `key` function to return the target configuration key (`#583 <https://github.com/CWorthy-ocean/C-Star/pull/583>`_)
- rename `ContinuanceTransform` to `ContinuanceDirective` (`#583 <https://github.com/CWorthy-ocean/C-Star/pull/583>`_)
- rename `NestingTransform` to `NestingDirective` (`#583 <https://github.com/CWorthy-ocean/C-Star/pull/583>`_)

Miscellaneous
~~~~~~~~~~~~~

- N/A
