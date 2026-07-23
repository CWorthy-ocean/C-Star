.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- The return type of `Launcher.update_status` has been modified (`#579 <https://github.com/CWorthy-ocean/C-Star/pull/579>`_)
- Registration of _step-to-command_ mapping functions is removed. (`#585 <https://github.com/CWorthy-ocean/C-Star/pull/585>`_)
- `cstar.orchestration.converter` is removed (`#601 <https://github.com/CWorthy-ocean/C-Star/pull/601>`_)
- ROMS-specific adapters in `cstar.orchestration.adapter` moved to `cstar.applications.roms_marbl.adapter` (`#601 <https://github.com/CWorthy-ocean/C-Star/pull/601>`_)

New features
~~~~~~~~~~~~


- Add model_reference_data to namelist pydantic model (not used in this repo, but needed by Forge) (`#586 <https://github.com/CWorthy-ocean/C-Star/pull/586>`_)
- added `cstar admin clean` CLI command (`#589 <https://github.com/CWorthy-ocean/C-Star/pull/589>`_)
- Add `Directive.workplan` attribute for access to run context information from within the `Directive` (`#584 <https://github.com/CWorthy-ocean/C-Star/pull/584>`_)

Bug Fixes
~~~~~~~~~


- Fix failure to cache updated status during status checks (`#579 <https://github.com/CWorthy-ocean/C-Star/pull/579>`_)
- Fix bug filling templates with `str(None)` (`#588 <https://github.com/CWorthy-ocean/C-Star/pull/588>`_)
- Mitigate test regressions (`#596 <https://github.com/CWorthy-ocean/C-Star/pull/596>`_)
- Fix attempt to use local file paths that do not exist. (`#597 <https://github.com/CWorthy-ocean/C-Star/pull/597>`_)
- Fix bug in `test_compose` hidden by `@pytest.mark.skip` (`#601 <https://github.com/CWorthy-ocean/C-Star/pull/601>`_)
- Fix circular import when importing RomsMarbl pieces from Forge (`#603 <https://github.com/CWorthy-ocean/C-Star/pull/603>`_)

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
- Rely on `threading.Event` alone during service shutdown to avoid inconsistent behavior / service hangs. (`#592 <https://github.com/CWorthy-ocean/C-Star/pull/592>`_)
- Fix unnecessarily long start-up times due to loading application libraries automatically (`#590 <https://github.com/CWorthy-ocean/C-Star/pull/590>`_)
- Avoid starting prefect server unnecessarily when running tests (`#591 <https://github.com/CWorthy-ocean/C-Star/pull/591>`_)
- Reduce startup time by instantiating `CStarSystemManager` at runtime. (`#593 <https://github.com/CWorthy-ocean/C-Star/pull/593>`_)
- Reduce usage of remote blueprints in test suite (`#600 <https://github.com/CWorthy-ocean/C-Star/pull/600>`_)
- Mitigate use of deprecated `typing.Optional`. (`#602 <https://github.com/CWorthy-ocean/C-Star/pull/602>`_)
- Improved logging, input validation, and test coverage for `file_system` module (`#597 <https://github.com/CWorthy-ocean/C-Star/pull/597>`_)

Miscellaneous
~~~~~~~~~~~~~

- N/A
