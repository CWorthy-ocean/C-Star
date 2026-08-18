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


- Fix unit tests failing in HPC environment (`#626 <https://github.com/CWorthy-ocean/C-Star/pull/626>`_)
- Fixes a bug where the ``continue-from`` directive would look in the wrong directory if PIO is enabled (`#629 <https://github.com/CWorthy-ocean/C-Star/pull/629>`_)
- Fixes a bug where the ``continue-from`` directive would obtain the wrong file if partitioning was enabled and multiple restart file timestamps were present (`#629 <https://github.com/CWorthy-ocean/C-Star/pull/629>`_)

Improvements
~~~~~~~~~~~~


- Add feature flag to enable the asynchronous job proxy for the local launcher (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)
- Enable simulated walltime timeouts in the local launcher (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)

Miscellaneous
~~~~~~~~~~~~~

- Pin ``compilers<2`` for laptop installs; see [here](https://github.com/conda-forge/mpich-feedstock/issues/142) for details (`#630 <https://github.com/CWorthy-ocean/C-Star/pull/630>`_)
