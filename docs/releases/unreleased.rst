.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- ``--resume`` can no longer be combined with ``--var`` or ``--varfile``; a resumed run always continues with the variables it was started with. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)
- Upscaler blueprints now default to ``pio: true``, so the upscaled CDR forcing file is written as CDF-5 and ``nccopy`` must be available on ``PATH``. Set ``pio: false`` to keep the previous NETCDF4 output. (`#705 <https://github.com/CWorthy-ocean/C-Star/pull/705>`_)

New features
~~~~~~~~~~~~


- ``cstar workplan run my_workplan.yaml --resume`` resumes the run that workplan started, deriving the run-id from the workplan name; ``--run-id <id> my_workplan.yaml --resume`` uses the explicit id. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)
- A workplan path given with ``--resume`` is checked against the run's recorded original workplan; a changed, missing or unreadable record is reported as a usage error naming ``--run-id <id>`` as the way to resume the run as recorded. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)
- New ``pio`` field on upscaler blueprints: when enabled (the default), the CDR forcing file is written as NETCDF4 and then converted to CDF-5 with ``nccopy -k cdf5``, keeping the original output filename (``output/upscaled_cdr.nc``). (`#705 <https://github.com/CWorthy-ocean/C-Star/pull/705>`_)

Bug Fixes
~~~~~~~~~


- Reading or writing a file with an unrecognized extension no longer prints "Using default persistence mode ``yaml`` for file ``{path}``" to the terminal. (`#703 <https://github.com/CWorthy-ocean/C-Star/pull/703>`_)
- ``--resume`` no longer refuses a correctly built codebase whose git index metadata is stale (for example after C-Star's copy-based staging); modifications are now detected by file contents. (`#704 <https://github.com/CWorthy-ocean/C-Star/pull/704>`_)
- ``--resume`` no longer refuses a codebase because its ``checkout_target`` branch has moved on the remote, or because the compute node has no network access. (`#704 <https://github.com/CWorthy-ocean/C-Star/pull/704>`_)
- A checkout target that is an annotated git tag now resolves to its commit, so such checkouts are recognized as up to date. (`#704 <https://github.com/CWorthy-ocean/C-Star/pull/704>`_)
- A name that is both a branch and a tag resolves to the branch, matching what ``git checkout`` did when the codebase was cloned. (`#704 <https://github.com/CWorthy-ocean/C-Star/pull/704>`_)

Improvements
~~~~~~~~~~~~


- The run-directory backup of the original workplan now has a single owner shared by the run preparation and the resume check. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)
- All eight bundled ModelSpecs and the example blueprint pin the C-Star 0.15.0 commit; the six specs that pinned forge commit 692e04ce move forward to it. Their ``cppdefs.opt.j2`` differed from the current one only by the two flag-gated blocks added for ucla-roms 0.8.0 (``UPSTREAM_TS_LAND_CURV``, ``PARABOLIC_SPLINES``, rendered as ``#undef`` when a spec does not set the flag) and a comment; older ROMS releases ignore both macros, so no rendered build changes. ``marbl_in`` is identical at both pins. (`#707 <https://github.com/CWorthy-ocean/C-Star/pull/707>`_)
- ``bundled_template_dir`` maps the current directory form and the legacy ``templates/<stage>`` form onto the bundled copy; ``BUNDLED_TEMPLATES_DIRECTORY`` names the prefix. (`#707 <https://github.com/CWorthy-ocean/C-Star/pull/707>`_)
- When a codebase cannot be adopted on resume, the error names the failed check: target not present in the clone, HEAD at a different commit (both hashes shown), tracked files modified, build artifacts missing, or git's own error for an unreadable repository. (`#704 <https://github.com/CWorthy-ocean/C-Star/pull/704>`_)
- Setup-time detection of local modifications is content-based as well, avoiding unnecessary recompiles caused by stale index metadata. (`#704 <https://github.com/CWorthy-ocean/C-Star/pull/704>`_)
- The NETCDF4 → CDF-5 conversion is now a shared utility used by both the ``nest_ic`` and ``upscaler`` applications. (`#705 <https://github.com/CWorthy-ocean/C-Star/pull/705>`_)
- Extracted ``migrate_forge_blueprint_data`` and ``migrate_forcing_inputs`` from ``cstar/applications/forge/blueprint.py`` into a new ``cstar/applications/forge/migration.py``. (`#709 <https://github.com/CWorthy-ocean/C-Star/pull/709>`_)
- Fetched template pins are cached under ``CSTAR_CACHE_HOME/forge-templates`` and reused on later runs. (`#713 <https://github.com/CWorthy-ocean/C-Star/pull/713>`_)
- A cache entry whose files no longer match the blueprint's ``file_hashes`` is re-fetched; branch pins and blueprints without hashes are never cached. (`#713 <https://github.com/CWorthy-ocean/C-Star/pull/713>`_)
- A cache directory that cannot be written (read-only or over quota on shared HPC filesystems) is logged and skipped rather than failing the build. (`#713 <https://github.com/CWorthy-ocean/C-Star/pull/713>`_)
- Namelist-consistency violations are centralized in one place and raise ``NamelistConsistencyError``, a ``ValueError`` that names the rule, namelist section and keys involved. (`#714 <https://github.com/CWorthy-ocean/C-Star/pull/714>`_)

Miscellaneous
~~~~~~~~~~~~~

- Documentation overhaul, pulling in Forge docs: (`#701 <https://github.com/CWorthy-ocean/C-Star/pull/701>`_)

  - Landing page: "How it works" (applications and blueprints, the three-step forge -> ROMS-MARBL flow, workplans, the catalog) plus the original principles; the sidebar is reorganized (see below).
  - Getting Started: installation rewritten around conda (laptop/HPC tabs, verify, after-install steps, brief source install); new "Registering for datasets" page (Copernicus Marine, TPXO, user-staged sources, where files go).
  - Terminology: entries for application (with the built-in ones), ROMS-MARBL blueprint, Forge, forge blueprint, spec, catalog, wizard, step, directive and working directory; the HPC deployment walkthrough tightened.
  - Laptop examples: new "End to end: a new domain to a running simulation" (wizard -> ``cstar blueprint run`` forge blueprint -> ``cstar blueprint run`` ROMS-MARBL blueprint); the two notebooks reframed as "Understanding a ROMS-MARBL blueprint" and "Understanding the basics of a workplan", with trimmed prerequisites (``cstar env register-kernel``), C-SON Forge references replaced, stale claims corrected, links pointing at source files so nbsphinx resolves them, and a stray copied code cell removed.
  - User Guide: Blueprints split into the core page plus ``blueprints/roms_marbl`` and a new ``blueprints/forge`` (sections, example, ``cstar forge run`` options, what Forge writes); Workplans refreshed (``Step.directives``, ``--var``/``--varfile`` runtime variables, ``{{<scope>: <step>}}`` directory placeholders, ``clobber``/``resume`` workflow overrides) with a new ``workplans/directives`` subpage (``apply-overrides``, ``continue-from``, ``nest-from``, deprecated keys, ordering, example, ``cstar blueprint run --directives``); new ``catalog`` page (contents, layers, ``CSTAR_CATALOG``, sharing, adding entries, legacy location); new ``wizard`` page (web app, login node over SSH, notebook form; each card; where saves go).
  - Forge section reduced to an overview, ``forge/specs`` (the five spec kinds, ``model.yaml`` in detail, the bundled model specs, the example notebook) and ``forge/source_datasets`` (downloaded, streamed and user-provided sources, pre-staging). Removed pages: getting_started, installation_hpc, machine_config, installation_changes, catalog, model_spec, reference, source_data.
  - Deployment: new ``hpc`` page (scheduler configuration, per-system source-data cache and working-directory table, ``PYTHONNOUSERSITE``, Jupyter kernels, wizard over SSH).

- Reference: new ``api-forge`` autosummary page (blueprint models, application, resolver and ModelSpec, catalog, execution). (`#701 <https://github.com/CWorthy-ocean/C-Star/pull/701>`_)

  - For Developers: ``developers/forge_internals``, ``developers/forge_input_data``, ``developers/forge_source_data``, ``developers/catalog_design`` (moved or extracted from the Forge pages) and a new ``developers/forge_templates`` (rendering, commit/hash pinning, bundled-vs-fetch staging); ``custom_applications`` points at the internals page as its worked example.
  - Banners: the Sphinx prolog "early phase of development" attention block and the README warning are removed, along with the "in development" snippet; the workplan state and compute-environment sections describe the current behaviour factually instead.

- Workplan guide and ``--resume`` help text describe resuming by workplan path and the ``--var``/``--varfile`` restriction. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)
- Added a test that an unrecognized extension resolves to YAML without printing anything. (`#703 <https://github.com/CWorthy-ocean/C-Star/pull/703>`_)
- The example blueprint's ``content_hash`` is restamped (``3b085d48`` to ``82e577f0``) since the pin is part of the hashed content. (`#707 <https://github.com/CWorthy-ocean/C-Star/pull/707>`_)
- Tests: the fast-path eligibility test expects every bundled spec to qualify for both stages; the fetched-content mismatch test hides the bundled copy to reach the fetch path; the offline staging fixture maps both directory forms; location assertions read ``C-Star.git``. (`#707 <https://github.com/CWorthy-ocean/C-Star/pull/707>`_)
- Docs: the templates developer page, the internals known-gaps list and the specs page describe the C-Star pin instead of the archived repository. (`#707 <https://github.com/CWorthy-ocean/C-Star/pull/707>`_)
- Blueprint guide describes how a resumed run adopts existing codebases. (`#704 <https://github.com/CWorthy-ocean/C-Star/pull/704>`_)
- Developer docs: corrected the descriptions of the app-boundary test, ``content_hash()``, GLORYS layout selection and the source alias table; replaced the template re-pin "known gap" with the CI test that enforces it; trimmed ``forge_input_data`` to the registry contract; marked ``catalog_design``'s forward-looking sections as unimplemented. (`#711 <https://github.com/CWorthy-ocean/C-Star/pull/711>`_)
- ``ForgeRunner.run``'s docstring records why forge processing stays inline on the event loop rather than on a worker thread. (`#711 <https://github.com/CWorthy-ocean/C-Star/pull/711>`_)
- ``pyproject.toml`` now names its setuptools build backend explicitly; the missing key was tolerated by pip but broke check-manifest's PEP 517 detection. (`#712 <https://github.com/CWorthy-ocean/C-Star/pull/712>`_)
- A ``check-manifest`` pre-commit hook verifies the sdist includes every tracked file not deliberately excluded. ``check-manifest`` moves from the build requirements to the ``dev`` extra. (`#712 <https://github.com/CWorthy-ocean/C-Star/pull/712>`_)
- The publish workflow checks that the installed wheel carries the eight bundled model specs, the two forge templates and the wizard files, and smoke-tests ``cstar forge --help``. (`#712 <https://github.com/CWorthy-ocean/C-Star/pull/712>`_)
- The forge templates developer page gains a "Staging cache" section. (`#713 <https://github.com/CWorthy-ocean/C-Star/pull/713>`_)

