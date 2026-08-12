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


- Add ``LocalComputeSpec`` and ``SlurmComputeSpec`` models exposing available overridable per-launcher compute settings. (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)
- Implement overriding global compute settings on per-step level via workplan config (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)

Bug Fixes
~~~~~~~~~

- N/A

Improvements
~~~~~~~~~~~~


- Add feature flag to enable the asynchronous job proxy for the local launcher (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)
- Enable simulated walltime timeouts in the local launcher (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)

Miscellaneous
~~~~~~~~~~~~~

- N/A
