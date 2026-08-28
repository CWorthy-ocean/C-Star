.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- ``roms_marbl`` blueprint schema is now **3.0.0** (single migration step from 2.1.0; ``cstar blueprint run`` migrates automatically, or run ``cstar blueprint migrate``): (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)

  - ``model_params`` is removed. ``time_step`` is now the namelist's ``time_stepping.dt`` (migration seeds it into ``namelist_overrides``); ``use_pio`` moves to ``partitioning.use_pio``.

- ``time_step`` is removed from ``Discretization``/``ROMSDiscretization``; previously serialized ``ROMSSimulation.to_dict()`` payloads carrying ``discretization.time_step`` no longer load. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- ``roms_runtime_settings`` derives ``ntimes`` from the effective (post-override) ``dt`` and now also writes ``param_settings.np_xi``/``np_eta`` from the partitioning, so the runtime namelist always matches the scheduled cores. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Prefect removed from dependencies (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)

New features
~~~~~~~~~~~~


- ``namelist_overrides`` blueprint field: a ``group -> {key: value}`` mapping deep-merged onto the runtime namelist **after** C-Star's derived settings, so user values win. Partial nested overrides touch only the keys they name; list values replace the existing list wholesale; unknown groups/keys fail loudly against the versioned namelist schema; overriding ``np_xi``/``np_eta`` inconsistently with ``partitioning`` is an error. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Early (blueprint-validation-time) checking of ``namelist_overrides`` names: unknown groups/keys are an error when ``code.roms`` pins a release version, and a warning against the best-guess schema for unpinned refs; ``np_xi``/``np_eta`` conflicts with ``partitioning`` error at validation time regardless. Values are still validated at runtime against the merged namelist. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- In-development auto-tiling support on ``partitioning``: ``auto_tiling`` (requires ``use_pio``) and ``n_cores`` (usable in place of ``n_procs_x``/``n_procs_y``; consistency enforced when all three are given). Build-time check requires ``#define MPI_MASKING`` when ``auto_tiling`` is enabled. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Automatic blueprint schema migration during ``cstar blueprint run`` is now the default, and ``cstar blueprint migrate`` is always mounted (feature flags ``CSTAR_FF_CLI_BP_MIGRATE_AUTO``/``CSTAR_FF_CLI_BP_MIGRATE_SHOW`` removed). (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- New ``CSTAR_DISABLE_MIGRATION=1`` escape hatch: guarantees a blueprint is never modified — commands fail early if the blueprint is not at the current schema version. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- ROMS namelists now support the ``&PIO_SETTINGS`` group (``pio_stride``) added in ucla-roms 0.6.0. (`#650 <https://github.com/CWorthy-ocean/C-Star/pull/650>`_)

Bug Fixes
~~~~~~~~~


- ``RuntimeParameterSet``'s model validator silently disabled the inherited ``ParameterSet`` locked/hash check (Pydantic v2 same-name validator shadowing): a ``locked: true`` parameter set with no hash validated cleanly. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- ``cstar blueprint run`` exited with code 0 when the blueprint failed validation; it now exits 1. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Serialized blueprints/workplans omitted ``schema_version`` whenever it equaled the current default (``exclude_defaults``); it is now always written so persisted artifacts self-describe. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- The local launcher's job proxy script used bash-only syntax (arrays) but is executed with ``sh``, so every local step failed immediately with a syntax error on systems where ``sh`` is dash (Ubuntu/Debian, including CI). The script is now POSIX sh. (`#650 <https://github.com/CWorthy-ocean/C-Star/pull/650>`_)
- Fix race condition error when performing simultaneous serializations of run records (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)
- Fix bug causing run history list to contain duplicates (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)
- Fix ``asyncio.run`` called from running thread bug (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)
- Fix ``slugify`` checks-for-empty before stripping bug (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)
- Fix latest status not written to sentinel in ``SlurmLauncher`` bug (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)
- Fix unexpectedly re-creating assets when reloading and running a workplan by run-id (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)
- Transformed-workplan artifacts are no longer written to a doubled ``<run_id>/<run_id>/`` directory; they now appear directly under the run directory. (`#651 <https://github.com/CWorthy-ocean/C-Star/pull/651>`_)

  - developer note: ``prepare_workplan`` no longer takes a ``run_id`` parameter; its ``output_dir`` argument is now the run-specific root directory where artifacts are written.


Improvements
~~~~~~~~~~~~


- ``deep_merge`` gains a ``replace_lists`` option (used by the namelist override path) so a shorter override list is not element-wise merged with stale values. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- The ``use_pio``/``PARALLEL_IO`` cppdefs check is generalized into ``_validate_cppdef_flag(define, enabled, ...)``, shared by the new ``MPI_MASKING`` check. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Auto-migration of an already-current blueprint is a no-op: nothing is persisted to the state directory and the original file is used as-is. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Fixed reliance on an implicit dependency installed by pydantic (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)
- ``build_and_run_dag`` split into ``build_dag`` and ``run_dag`` to support proper plan reloading (`#642 <https://github.com/CWorthy-ocean/C-Star/pull/642>`_)

Miscellaneous
~~~~~~~~~~~~~

- New versioned template ``blueprint.3.0.0.yaml`` and generated ``roms_marbl_schema.3.0.0.json``; ``docs/schemas/index.rst``, ``docs/blueprints.rst``, and the tutorial blueprints/notebook updated to the 3.0.0 shape; stale ``ModelParameterSet`` API listing removed. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Test fixtures (``blueprint_complete.yaml``, ``blueprint_template.yaml``, unit-test conftest data) migrated to 3.0.0. (`#643 <https://github.com/CWorthy-ocean/C-Star/pull/643>`_)
- Made a migrate-CLI test assertion robust to console line-wrapping, which could split the expected error message at an arbitrary point depending on the tmp-path length (flaky in CI). (`#650 <https://github.com/CWorthy-ocean/C-Star/pull/650>`_)
