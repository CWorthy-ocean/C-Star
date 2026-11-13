
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
   ``integration_extended.yaml`` workflow runs the suite against ucla-roms
   ``0.14.0`` and ``main``, on Python 3.12, 3.13 and 3.14 on Ubuntu and on
   Python 3.14 on macOS. The ``CSTAR_IT_ROMS_REF`` environment variable
   overrides the ucla-roms ref of the model spec. Failures are an early
   warning for upstream changes, not a merge blocker.

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
  bundled catalog; ``roms-marbl-0.14-default`` pins ucla-roms 0.14.0.
- ``conftest.py``: the ``integration_test_data``, ``test_catalog_root``,
  ``test_catalog``, ``forge_blueprint_factory`` and ``cstar_shim`` fixtures.
- ``cli_harness.py``: ``make_shim``, ``make_cli_env`` and ``run_cstar``,
  which run the real ``cstar`` CLI as a subprocess against this checkout.

Adding an integration case
^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Add a ``DomainSpec`` and a ``ForcingSpec`` under
   ``cstar/tests/integration_tests/catalog/`` (a file per spec,
   ``specs/DomainSpec/<name>.yaml`` or ``specs/ForcingSpec/<name>.yaml``), pointing at files from
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

Before you open an issue or a pull request, read the
`[C]Worthy AI policy <https://github.com/CWorthy-ocean/.github/blob/main/AI_POLICY.md>`_.
Every pull request description carries one ``AI disclosure:`` line, which
the pull request template provides. Keep each pull request focused on one
thing, and open an issue first for anything larger than a fix. A pull request
that changes something users could notice also adds a release note; see
`Writing a release note`_.

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

Writing a release note
----------------------

A pull request that changes something users could notice adds a release note:
a file in the ``changes/`` directory named ``+<name>.<type>.rst``, where
``<name>`` is any short label, such as ``+river-count.bugfix.rst``. Commit it
with your code. The ``+`` means the pull request number is filled in at
release time, from the merge commit, so the note links to your pull request.
If you already know the number you can use it instead:
``<PR number>.<type>.rst``. At release time the notes are collected into
``docs/releases/<version>.rst`` and the files are deleted, so don't edit the
files in ``docs/releases/`` yourself.

The type decides the section the note appears under:

.. list-table::
   :header-rows: 1

   * - Type
     - Section
     - For
   * - ``breaking``
     - Breaking Changes
     - a workplan, blueprint, catalog, command or script that worked now
       fails or needs an edit: a renamed or removed option, a key that is now
       rejected, a file an earlier release can no longer read
   * - ``output``
     - Changes to Output
     - the same blueprint or workplan still runs, but what C-Star writes
       differs: generated input files, the namelist, model output, or the
       layout of a run directory
   * - ``deprecation``
     - Deprecations
     - something that still works but will be removed (see
       `Deprecations and compatibility`_)
   * - ``feature``
     - New features
     - new capabilities
   * - ``bugfix``
     - Bug Fixes
     - code that did not do what it should, and now does
   * - ``improvement``
     - Improvements
     - better behavior, messages or performance of existing code
   * - ``misc``
     - Miscellaneous
     - documentation, CI, packaging and test changes worth mentioning

Write the note for someone reading the release notes, not for the reviewer:
what changed for them and anything they need to do, in one line. Details for
the reviewer go in the pull request description. If the reader needs more, add
``-`` sub-bullets on the following lines. The note is reStructuredText, like
this page: code goes in double backticks, and Markdown's single backticks and
``[text](url)`` links do not render. For example,
``changes/+shared-output-names.bugfix.rst``, added by pull request 888:

.. code-block:: rst

   Chained steps whose output files share a name no longer crash input staging with ``FileExistsError``.
   - Same-named forcing files whose time records follow on are staged side by side, the later one as ``<name>_2.nc``.
   - Any other shared input name is refused with one error listing the files.

becomes, in the release notes:

.. code-block:: rst

   Bug Fixes
   ~~~~~~~~~

   - Chained steps whose output files share a name no longer crash input staging with ``FileExistsError``. (`#888 <https://github.com/CWorthy-ocean/C-Star/pull/888>`_)

     - Same-named forcing files whose time records follow on are staged side by side, the later one as ``<name>_2.nc``.
     - Any other shared input name is refused with one error listing the files.

Each note is its own file, so a pull request with several notes adds several
files, such as ``+groom-switch.feature.rst``, ``+groom-wizard.feature.rst``
and ``+esper-docs.misc.rst``. A change that both breaks a setup and changes
output gets one note of each type.

A pull request with nothing for users to read about, such as a test-only
change or a typo fix, needs no note; ask a maintainer to add the
``no-changelog`` label. The ``towncrier check`` job on the pull request fails
until it has a note or the label.

To preview the notes as they will appear, run this in ``cstar-env``
(``towncrier`` comes with ``pip install -e ".[dev]"``)::

   towncrier build --draft --version Unreleased

Notes named with a ``+`` show up without their pull request link until the
release.

Deprecations and compatibility
------------------------------

Users keep workplans, blueprints, catalogs and shell scripts for months, so
anything a user may have written down -- a CLI command, a directive key, a
catalog layout, a configuration setting -- is renamed or removed in two steps:
first deprecated, then removed. Blueprint schema changes follow their own
rule (a major ``schema_version`` with a migration, see
:doc:`developers/custom_applications`) and are not covered here.

Renaming or restructuring a field of a catalog spec (ModelSpec, DomainSpec,
ForcingSpec, OutputSpec, CdrSpec) needs a registered spec migration: a
``SchemaAdapter`` added to ``SPEC_MIGRATIONS`` in
``cstar/applications/forge/migration.py``, a major bump of that kind in
``SPEC_SCHEMA_VERSIONS`` (``cstar/catalog/spec_schema.py``), and the bundled
specs restamped. The adapter must leave content that is already current
unchanged. An additive optional key is a minor bump with no adapter.

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
- Add a ``deprecation`` release note (see `Writing a release note`_):
  ``Deprecated: <what>, removed in <release>; use <replacement>.``

**Removing.** When the removal release comes, delete the code that still
honors the old form, but keep the code that detects it, and raise an error
naming the migration command, or the edit to make if there is no command.
Never let an old form silently stop meaning anything: a catalog file in a
removed layout must not just disappear from listings, and an unknown key
must not be ignored. Add a ``breaking`` release note:
``Removed: <what>; <migration>.``

``ci/check_deprecations.py`` makes sure removals happen. It reads every
``deprecated`` call under ``cstar/`` (and every ``deprecation_message`` call,
for text C-Star prints without Python, such as the ``cstar env shell-init``
function), and the Finalize Release Notes workflow runs it before it opens the
release pull request: a release that a deprecation is due in is refused until
the removal lands on ``main``. A forgotten removal then stops the release, not
the user. ``cstar/tests/unit_tests/base/test_deprecation.py`` runs the same
check on every pull request against the newest finalized release.

Making a release
----------------

For maintainers:

1. Run the *Finalize Release Notes* workflow from the repository's Actions tab
   with the new version, for example ``0.19.0``. It refuses a release that a
   deprecation is due in (see above). It renames each ``+<name>`` file in
   ``changes/`` after the pull request that added it, stopping with an error
   if it can't find one. It then turns those files into
   ``docs/releases/0.19.0.rst``, deletes them, lists the new file first in
   ``docs/releases.rst``, and opens a *Finalize release notes for 0.19.0* pull
   request from the ``next_release_notes`` branch. If that branch is left over
   from an earlier attempt, the workflow stops; delete the branch and run it
   again.
2. Edit the notes on that pull request if they need it: merge overlapping
   notes, reorder, fix wording.
3. Merge it. This creates the tagged GitHub release with those notes, which
   publishes the package to PyPI.

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
