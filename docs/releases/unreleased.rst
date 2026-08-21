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
- The NestIc app will now do CDF-5 conversion of the new IC file if PIO is enabled (default: true) (`#636 <https://github.com/CWorthy-ocean/C-Star/pull/636>`_)
- Add ability to write different namelist versions based on ucla-roms codebase tag (`#638 <https://github.com/CWorthy-ocean/C-Star/pull/638>`_)
- Add support for upcoming ucla-roms 0.5.0 namelist changes (`#638 <https://github.com/CWorthy-ocean/C-Star/pull/638>`_)

Bug Fixes
~~~~~~~~~


- Fix unit tests failing in HPC environment (`#626 <https://github.com/CWorthy-ocean/C-Star/pull/626>`_)
- Fixes a bug where the ``continue-from`` directive would look in the wrong directory if PIO is enabled (`#629 <https://github.com/CWorthy-ocean/C-Star/pull/629>`_)
- Fixes a bug where the ``continue-from`` directive would obtain the wrong file if partitioning was enabled and multiple restart file timestamps were present (`#629 <https://github.com/CWorthy-ocean/C-Star/pull/629>`_)
- Normalize run-id casing at intake to fix mixed-case run breakage (`#632 <https://github.com/CWorthy-ocean/C-Star/pull/632>`_)
- Fix bug where slurm account, walltime, and queue env vars are not updated with compute customizations (`#634 <https://github.com/CWorthy-ocean/C-Star/pull/634>`_)
- Depending on already-completed SLURM jobs should now seamlessly drop the dependencies (`#633 <https://github.com/CWorthy-ocean/C-Star/pull/633>`_)

Improvements
~~~~~~~~~~~~


- Add feature flag to enable the asynchronous job proxy for the local launcher (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)
- Enable simulated walltime timeouts in the local launcher (`#608 <https://github.com/CWorthy-ocean/C-Star/pull/608>`_)

Miscellaneous
~~~~~~~~~~~~~

- Pin ``compilers<2`` for laptop installs; see [here](https://github.com/conda-forge/mpich-feedstock/issues/142) for details (`#630 <https://github.com/CWorthy-ocean/C-Star/pull/630>`_)
- Revert compilers<2, add compiler-specific mpich packages to ensure macos has the exact compilers needed (`#631 <https://github.com/CWorthy-ocean/C-Star/pull/631>`_)
- Silence sqlalchemy errors related to Prefect shutdown (`#637 <https://github.com/CWorthy-ocean/C-Star/pull/637>`_)
