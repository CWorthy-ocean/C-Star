
Contributor Guide
=================

Conda environment
-----------------

Install **one** of the following conda environments, depending on
whether you are working on a supported HPC system (environment
management by Linux Environment Modules) or a generic machine like a
laptop (environment managed by conda)::

   conda env create -f environment-hpc.yml  # conda environment for supported HPC system
   # conda env create -f environment-laptop.yml  # conda environment for generic machine

Activate the conda environment::

   conda activate cstar-env

Install ``C-Star`` in the same environment::

   pip install -e ".[dev,docs]"

This conda environment is useful for any of the following steps:

1. Running the example notebooks (also requires ``conda install jupyterlab``)
2. Contributing code and running the testing suite
3. Building the documentation locally

Running the tests
-----------------

You can check the functionality of the C-Star code by running the test
suite::

   conda activate cstar-env
   cd C-Star
   pytest cstar/tests/unit_tests

Do not run a bare ``pytest`` at the repository root: it also collects the
integration suite, which compiles ROMS and downloads data. Select tests
by path, as described below.

Integration tests
~~~~~~~~~~~~~~~~~

The integration suite lives in ``cstar/tests/integration_tests`` and is
organized in tiers, one directory each. There are no ``pytest`` markers
for this; a tier is selected by path.

**Tier 1: forge inputs** (``forge/``)
   Runs the forge application with real ``roms-tools`` against the pinned
   source data, without building ROMS. It checks the generated input
   files (classic-format netCDF that ParallelIO can read), the emitted
   ``ucla_roms`` blueprint, the rendered namelist and ``cppdefs``, and
   compares the namelist to a golden file. It takes about a minute (about
   ten seconds per case after the first fetch) and runs on Ubuntu and
   macOS for every pull request::

      python -m pytest cstar/tests/integration_tests/test_fixtures.py cstar/tests/integration_tests/forge

**Tier 2: end to end** (``e2e/``)
   Runs a two-step workplan (forge, then ``ucla_roms`` through
   ``blueprint: {from_step: ...}``) with ``cstar workplan submit`` and the
   local launcher. ROMS, MARBL and ParallelIO are compiled and ROMS is
   run with ``mpirun -n 4`` on the generated inputs, with ParallelIO
   enabled. A negative control (``test_pio_input_guard.py``) feeds a
   NETCDF4 input to a ParallelIO build and expects it to be refused.
   ``ucla_roms/test_prebuilt_case.py`` (Tier 2b) runs the prebuilt
   ``cstar_blueprint_test_case`` through ``cstar blueprint run`` (no
   ParallelIO, partitioned inputs) in the same CI job.
   ``e2e/test_workplan_shapes.py`` covers DAG shapes and dependency-failure
   propagation with ``hello_world`` steps (seconds, no compile). This tier takes
   around ten minutes and runs on Ubuntu for every pull request::

      python -m pytest cstar/tests/integration_tests/e2e

**Tier 3: extended** (``extended/``)
   Not part of the pull request checks. The weekly
   ``integration_extended.yaml`` workflow runs the suite on both operating
   systems, Python 3.12 and 3.13, against ucla-roms ``0.12.0`` and
   ``main``. The ``CSTAR_IT_ROMS_REF`` environment variable overrides the
   ucla-roms ref of the model spec. Failures are an early warning for
   upstream changes, not a merge blocker.

Running the integration tiers requires ``nccopy`` (from ``libnetcdf``,
installed with ``environment-laptop.yml``), ``mpirun`` and the compilers
from the conda environment. The first run needs network access to fetch
the ETOPO5 topography and the ERA5 correction file (stored in the
``roms-tools`` pooch cache) and the pinned test data (stored in
``pooch.os_cache("cstar_integration_test_data")``).

Shared pieces are in ``cstar/tests/integration_tests``:

- ``data_registry.py``: a ``pooch`` registry of the upstream source data
  from the ``CWorthy-ocean/roms-tools-test-data`` repository, the only
  source of upstream data for the suite. It is pinned to a commit
  (``TEST_DATA_COMMIT``) with a sha256 per file (``REGISTRY``).
- ``cases.py``: the run window constants and ``FORGE_CASES``, the
  domain and forcing pairings shared by the tiers (``unified`` and
  ``constants`` are ``bgc_mode: marbl`` builds; ``cdr_lite`` is a
  ``bgc_mode: cdr_lite`` build with a simple-mode CDR forcing).
- ``catalog/``: a test-local catalog layer with a small domain
  (``na-test-8x8``), forcing specs that point at the pinned data
  (``test-glorys-era5-unified``, ``test-glorys-era5-constants`` and the
  physics-only ``test-glorys-era5-physics``) and a
  minimal output spec (``test-minimal``). ``${TEST_DATA}`` in these files
  is replaced by the data cache directory. Model specs come from the
  bundled catalog; ``roms-marbl-0.12-default`` pins ucla-roms 0.12.0.
- ``conftest.py``: the ``integration_test_data``, ``test_catalog_root``,
  ``test_catalog``, ``forge_blueprint_factory`` and ``cstar_shim`` fixtures.
- ``cli_harness.py``: ``make_shim``, ``make_cli_env`` and ``run_cstar``,
  which run the real ``cstar`` CLI as a subprocess against this checkout.

Adding an integration case
^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Add a ``DomainSpec`` and a ``ForcingSpec`` under
   ``cstar/tests/integration_tests/catalog/`` (a file per spec,
   ``DomainSpec/<name>.yaml`` or ``ForcingSpec/<name>.yaml``), pointing at files from
   the data registry through ``${TEST_DATA}``.
2. Add a ``ForgeCase`` entry to ``FORGE_CASES`` in ``cases.py``, with
   ``compile_time_overrides`` if the case changes ``cppdefs``,
   ``run_time_overrides`` if it changes namelist settings, and ``bgc_mode``
   (and ``cdr``, which ``cdr_lite`` requires) if it is not a MARBL build.
3. Add a matching entry to ``EXPECTATIONS`` in
   ``forge/test_forge_inputs.py``. The ``run`` fixture is parametrized on
   its keys, so the existing tests then cover the new case.
4. Generate the namelist golden with ``UPDATE_GOLDEN=1`` (below), review
   it, and commit it.

Updating the namelist goldens
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The rendered namelists are compared to
``forge/fixtures/golden_namelist_<case>.nml``. After an intentional change,
regenerate them deliberately, review the diff (it is a behavior change)
and commit it::

   UPDATE_GOLDEN=1 python -m pytest cstar/tests/integration_tests/forge -k golden

The protocol is also described in the docstring of
``forge/test_forge_inputs.py``. The forge unit tests use the same
``UPDATE_GOLDEN=1`` convention for their own goldens.

Updating the test data pin
^^^^^^^^^^^^^^^^^^^^^^^^^^

To move to newer upstream test data, change ``TEST_DATA_COMMIT`` in
``data_registry.py`` to the new ``roms-tools-test-data`` commit. If the
files changed, update their sha256 entries in ``REGISTRY`` (``pooch``
rejects a file whose hash does not match), and regenerate the goldens if
the inputs affect them. Pinning to a commit rather than ``main`` keeps a
data change a visible, reviewable diff.

Test data in CI
^^^^^^^^^^^^^^^

The integration workflows cache both pooch directories (``roms-tools`` and
``cstar_integration_test_data``) with ``actions/cache``. The cache key
includes the hash of ``data_registry.py``, so a data bump invalidates it.
GitHub evicts caches that are idle for seven days, and caches saved on
``main`` are readable by pull requests, so runs on ``main`` keep the cache
warm. A cache miss only costs a re-download.

Data roles: ``roms-tools-test-data`` is the source of all upstream data;
``cstar_blueprint_test_case`` is the older ``ucla_roms``-only case used by
``ucla_roms/test_prebuilt_case.py``; ``cstar_blueprint_roms_marbl_example`` is
used by the documentation tutorials only.

Contributing code
-----------------

If you have written new code, you can run the tests as described in the
previous step. You will likely have to iterate here several times until
all tests pass. The next step is to make sure that the code is formatted
properly. Activate the environment (created above) and run all linters
as follows::

   conda activate cstar-env
   pre-commit run --all-files

Some things will automatically be reformatted, others may need manual
fixes. Follow the instructions in the terminal until all checks pass.
Once you got everything to pass, you can stage and commit your changes
and push them to the remote github repository.

Adding Tests for New Code
~~~~~~~~~~~~~~~~~~~~~~~~~

Please ensure that your additions are covered by appropriate tests to
maintain code quality and reliability. Follow these guidelines:

- **What to Test**:

  - Test any new properties, functions, and methods in API using unit
    tests (``cstar/tests/unit_tests``)
  - If adding entirely new functionality or complex multi-step
    processes, add appropriate integration tests
    (``cstar/tests/integration_tests``)

- **Best Practices**:

  - Focus on areas with multiple options or combinations of behavior,
    using parameterizations or distinct tests to cover every option
  - Group related tests (e.g. testing different outcomes of the same
    method under different conditions) in test classes
  - Consider edge cases, such as unexpected input or failure scenarios.
  - Write tests that help identify specific issues quickly (e.g., as if
    a random ``return`` statement was added in your code).

- **Using Fixtures**:

  - Use fixtures to set up any expensive operations
  - Ensure the fixture logic is itself independently tested.

- **Useful ``pytest`` Tips**:

  - Run specific tests by specifying file paths, directories, or test
    names:

    .. code:: bash

       pytest path/to/test_file.py

  - Select tests by path rather than by marker: the repository defines no
    ``pytest.mark`` categories, and ``-m`` selection does not work here.

Deprecations and compatibility
------------------------------

Users keep workplans, blueprints, catalogs and shell scripts for months, so
anything a user may have written down -- a CLI command, a directive key, a
catalog layout, a configuration setting -- is renamed or removed in two steps:
first deprecated, then removed. Blueprint schema changes follow their own
rule (a major ``schema_version`` with a migration, see
:doc:`developers/custom_applications`) and are not covered here.

**Deprecating.** The old form keeps working, and the code that detects it
calls ``cstar.base.deprecation.deprecated``:

.. code:: python

   from cstar.base.deprecation import deprecated

   deprecated(
       f"The {cls.key()!r} key {cls.KEY_BRY_PATH!r}",
       repr(cls.KEY_PATH),
       removal="0.20.0",
   )

which warns once per process::

   The 'nest-from' key 'bry_path' is deprecated and stops working in
   cstar-ocean 0.20.0; use 'path' instead.

- Call it where the old form is first read: at load time, or in the
  validation ``cstar workplan check`` runs. A warning that only appears in a
  step's log on the compute node comes too late.
- Name the replacement in ``replacement``, and when a command does the
  migration (``cstar admin migrate-catalog``), name the command.
- ``removal`` is a minor release at least two minor releases after the first
  release that ships the warning: a deprecation first released in 0.18.0 is
  removed in 0.20.0 at the earliest. Pass it as a string literal.
- The message goes out as a ``CstarDeprecationWarning`` (a
  ``FutureWarning``, so Python shows it by default) and as a log line. The CLI
  shows only the log line.
- Add a bullet under **Breaking Changes** in the pull request:
  ``Deprecated: <what>, removed in <release>; use <replacement>.`` Pull request
  descriptions become the release notes.

**Removing.** When the removal release comes, delete the code that still
honors the old form, but keep the code that detects it, and raise an error
naming the migration command, or the edit to make if there is no command.
Never let an old form silently stop meaning anything: a catalog file in a
removed layout must not just disappear from listings, and an unknown key
must not be ignored. Add a ``Removed: <what>; <migration>.`` bullet under
**Breaking Changes**.

``ci/check_deprecations.py`` makes sure removals happen. It reads every
``deprecated`` call under ``cstar/`` (and every ``deprecation_message`` call,
for text C-Star prints without Python, such as the ``cstar env shell-init``
function), and the Finalize Release Notes workflow runs it before it opens the
release pull request: a release that a deprecation is due in is refused until
the removal lands on ``main``. A forgotten removal then stops the release, not
the user. ``cstar/tests/unit_tests/base/test_deprecation.py`` runs the same
check on every pull request against the newest finalized release.

Building the documentation locally
----------------------------------

Activate the environment::

   conda activate cstar-env

Then navigate to the docs folder and build the docs via::

   cd docs
   make fresh
   make html

You can now open ``docs/_build/html/index.html`` in a web browser via::

   open _build/html/index.html
