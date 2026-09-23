.. _forge-internals:

Forge internals
==================

The primary architecture reference for Forge, describing the current state
of the code inside C-Star.

The big picture
----------------------

Forge is split into two layers along a hard boundary:

- **Authoring**: the catalog of reusable specs (Model/Domain/Forcing/Output
  specs, ``cstar.catalog``), a **resolver**
  (``cstar.applications.forge.resolve``) that assembles them into a single
  reviewable file, and a **wizard** UI (``cstar.wizard``).
- **Execution** (``cstar.applications.forge``): an **engine** that turns
  that file into ROMS-MARBL input NetCDFs, a namelist, and a downstream
  blueprint, plus the **executor** that does the actual work. Execution
  never touches the catalog.

The file that crosses the boundary is ``ForgeBlueprint`` -- **the forge
application's own blueprint**. Terminology trap to avoid: C-Star also has an
existing, unrelated ``roms_marbl`` application whose blueprint
(``RomsMarblBlueprint``) forge *emits as an output artifact*. "Building a
blueprint" means producing that downstream artifact, not forge's own input.

.. code-block:: text

    catalog specs  -+
    (Model/Domain/  |-> build_forge_blueprint() -> ForgeBlueprint -> process_forge_blueprint(cfg, host)
     Forcing/Output)|         (resolver)         (.yaml,               (engine -> executor)
                    |                             portable)           |
    wizard UI ------+                                                 v
                                                        input NetCDFs, namelist.nml,
                                                        cppdefs.opt, roms_marbl blueprint

Directory map
--------------------

.. code-block:: text

    cstar/
    +-- applications/
    |   +-- forge/                     # The forge application (execution engine)
    |   |   +-- app.py                     # ForgeRunner/ForgeApplication (C-Star application)
    |   |   +-- blueprint.py               # ForgeBlueprint -- the forge application's blueprint
    |   |   +-- engine.py                  # process_forge_blueprint(); ForgeBlueprintExecutor Protocol;
    |   |   |                              # sources_to_forcing_override()
    |   |   +-- executor.py                # ForgeExecutor -- the processing engine
    |   |   +-- host.py                    # HostPaths -- frozen host-boundary contract injected into the executor
    |   |   +-- input_data.py              # Input file generation
    |   |   +-- source_datasets.py         # Dataset download and preparation
    |   |   +-- source_registry.py         # Dataset alias map / provenance metadata (stdlib-only)
    |   |   +-- glorys_subchunk.py         # Just-in-time kerchunk subchunking for GLORYS
    |   |   +-- settings.py                # Template rendering
    |   |   +-- namelist_model.py          # RunTimeSettings + build_namelist
    |   |   +-- user_files.py              # User-provided netCDF attachments (grid/river/CDR)
    |   |   +-- util.py                    # Shared helpers, memory/timing instrumentation
    |   |   +-- xarray_lockfix.py          # xarray/dask locking workaround
    |   |   +-- _yaml_representers.py      # PyYAML Enum representer registration (import side effect)
    |   |   +-- templates.py               # bundled_template_dir(): ModelSpec templates/<stage> -> the
    |   |   |                              # bundled copy, plus hashing it (see forge_templates)
    |   |   +-- resolve.py                 # resolver: build_forge_blueprint(...) (authoring, not execution)
    |   |   +-- config.py                  # Host/path resolution, system detection (host glue, not execution)
    |   |   +-- models.py                  # Spec classes (ModelSpec, etc.) (authoring, not execution)
    |   |   +-- runtime.py                 # run_blueprint(...): executes a forge_blueprint.yaml given
    |   |                                  # already-parsed option values (called by cli/forge's 'run')
    |   +-- core.py                    # get_application() / register_application / ApplicationDefinition
    |   +-- roms_marbl/                # the roms_marbl application forge's output feeds into
    +-- catalog/                       # Bundled + layered spec catalog (see docs/developers/catalog_design.rst)
    |   +-- bundled/
    |   |   +-- ModelSpec/{model}/model.yaml    # Code repos, templates, settings, defaults
    |   |   +-- DomainSpec/{grid}/Domain.yaml   # Grid definitions
    |   |   +-- ForcingSpec/{name}/Forcing.yaml # Forcing source configurations
    |   |   +-- OutputSpec/{name}/Output.yaml   # Output configurations
    |   |   +-- blueprints/                     # Example blueprints (bundled, read-only layer;
    |   |                                        # user saves go to the user catalog layer instead)
    |   +-- domain_catalog.py          # DomainCatalog / LayeredCatalog
    +-- wizard/                        # Wizard presentation layer (shared UI kit + Voila front-end)
    |   +-- wizard.py                      # ForgeBlueprintWizard (ipywidgets UI)
    |   +-- _voila_app.ipynb               # Voila app notebook -- internal; served via 'cstar forge wizard'
    |   +-- forge-blueprint-wizard.ipynb   # user-facing wizard notebook (copied out via 'cstar forge copy-notebook')
    |   +-- ui/
    |       +-- shell.py                   # AppShell: brand header + page nav + page stack
    |       +-- components.py              # WIZARD_CSS + card/field_row/field_grid/subsection/chip/banner helpers
    |       +-- labels/                    # Per-page label glossary (label_for/section_for)
    |       +-- catalog_bar.py             # CatalogBar: catalog-location input + two-click Reload confirm
    |       +-- branding.py                # [C]Worthy palette tokens, header bar, favicon, page title
    +-- cli/
    |   +-- forge/                     # 'cstar forge run'/'wizard'/'copy-notebook'/'show-paths' typer sub-app
    |   +-- environment/register_kernel.py  # 'cstar env register-kernel'
    +-- additional_files/templates/forge/  # Bundled render templates (compile-time/run-time);
                                            # see forge_templates.rst.

``glorys_subchunk.py`` is called from ``input_data.py`` and is covered by the
boundary guard test (below) like any other execution module: the guard's
module list is every ``.py`` file in ``cstar/applications/forge`` minus a
small, fixed exclusion set for the authoring/host modules (``resolve``,
``models``, ``config``, ``runtime``, ``app``, ``__init__``), so a new
execution module is picked up without editing that set.

``ForgeBlueprint`` -- the forge blueprint
------------------------------------------------

Defined in ``cstar/applications/forge/blueprint.py``, which subclasses
``cstar.orchestration.models.Blueprint`` -- this is what makes forge a real
C-Star application (see `Forge as a real C-Star application`_ below), not
just a Pydantic model that happens to carry an ``application`` string.

Top-level shape: ``forge_blueprint_version`` (int, bump only on breaking
change; currently 8) - ``application`` (=``"forge"``, C-Star app
discriminator, required by the ``Blueprint`` base) - ``name``/``description``
(required top-level fields on the ``Blueprint`` base; ``name`` is the single
user-editable canonical name -- ``casename``/``working_dir``/
``B_{name}.yaml``/netCDF stems all derive from it) - ``run`` (start/end date,
model_reference_date) - ``domain`` (``grid_name``, grid_kwargs,
topography_source, open_boundaries, partitioning, nesting) - ``forcing``
(flat: initial_conditions, a single boundary section (``BoundaryForcing``,
mirroring ``InitialConditions``), surface/tidal/river lists,
resolved_datasets) - ``cdr`` (``CdrSpec``: a 5-mode CDR-forcing selection --
none/simple/yaml/netcdf/upscaled -- carrying the compiled roms-tools
``CDRForcing`` kwargs or a user-provided netCDF ref; its own composable
catalog spec, independent of ``forcing``) - ``datasets`` (host-independent
list of resolved dataset keys) - ``model_settings`` (flat dict: cppdefs + ~35
namelist sections) - ``code`` (roms/marbl repos +
``templates_compile_time``/``_run_time`` repo refs) - ``composition``
(which catalog specs produced this + overrides layer) - ``provenance``
(generated_at, content_hash, notes). The ``Blueprint`` base also adds
``state``/``schema_version`` (its own versioning metadata, distinct from
``forge_blueprint_version``) and injects a ``$schema`` key on serialization
(stripped back out on load).

Older blueprint files load transparently: a ``model_validator(mode="before")``
(``migrate_forge_blueprint_data``) migrates v2/v3 layouts (removed
``identity`` sub-model, removed ``ensemble_id``), the v4->v5
``do_cdr``->``do_cdr_output`` rename, the v6->v7 CDR move
(``forcing.cdr_forcing``/``cdr_forcing_file`` -> the top-level ``cdr``
section, mode inferred), and the v7->v8 BGC-sources move
(``initial_conditions.bgc_source`` rewrapped as a one-item ``bgc_sources``
list; ``forcing.boundary``'s flat, type-discriminated
``BoundaryForcingItem`` list split into a single ``BoundaryForcing`` section
with ``source`` + ``bgc_sources``, mirroring ``InitialConditions``) to the
current shape, reproducing derived names bit-for-bit. ``model_name``/
``grid_name`` live in ``composition.model.name``/``domain.grid_name``;
``grid_name`` is results-affecting -- ``SourceDatasets`` keys cache
filenames off it.

- **``working_dir``** (default ``~/cstar/_forge_bp_runs``) is the single
  per-run artifact root -- everything the executor *produces* lands under
  it. It's host/location, not results-affecting, so it's excluded from
  ``content_hash``. Redeclared as ``str`` (the ``Blueprint`` base's is
  ``Path``) to preserve sentinel expansion -- see
  ``ForgeBlueprint._resolve_out_dir``, which overrides the base's eager
  ``expanduser()``/``resolve()`` for exactly this reason.
- **``content_hash()``** -- sha256 over everything *except*
  ``forge_blueprint_version``, ``name``, ``description``, ``composition``,
  ``provenance``, ``working_dir``, ``state``, ``schema_version``,
  ``$schema`` (see ``_HASH_EXCLUDE``); each code repo's ``location`` (fetch
  address) and ``file_hashes`` (a derived cache, not independent content);
  each user-provided file's ``location`` (host path, not its pinned
  ``content_hash``); and, on ``initial_conditions``/``boundary``, the
  execution-environment knobs ``bypass_validation`` and each bgc source's
  ``serialize_dask`` -- none of these change what the run produces, only how
  or where it's produced. Stamped on ``to_yaml``; ``verify_content_hash``
  warns (doesn't block) on a mismatched hand-edit at load.

Render templates: ``code.templates_compile_time``/``_run_time`` pin a git
commit (``code.templates_commit``) and, per file, the sha256 of its content
at that commit (``file_hashes``, authored in the bundled ModelSpecs). See
:doc:`forge_templates` for the pinning format and the fast-path-vs-fetch
staging logic at ``configure_build``.

Forge as a real C-Star application
-------------------------------------------

``cstar/applications/forge/app.py`` (excluded from the ``test_forge_app_boundary.py``
guard described below -- like ``runtime.py``, it's disposable host-resolution glue)
defines the pieces the :doc:`custom-applications contract <../custom_applications>`
requires:

- ``ForgeRunner(BlueprintRunner[ForgeBlueprint])`` -- ``run()`` delegates to
  ``cstar.applications.forge.runtime.process`` (host resolution) ->
  ``process_forge_blueprint`` -> ``ensure_source_data``/``generate_inputs``/
  ``configure_build``, then reports ``ExecutionStatus.COMPLETED``. Scope:
  generates inputs and emits the downstream ``roms_marbl`` blueprint
  (``B_{name}.yaml``), then stops -- the existing ``roms_marbl`` application
  consumes that blueprint separately.
- ``ForgeApplication`` -- ``@register_application``-decorated
  ``ApplicationDefinition`` wiring ``ForgeBlueprint`` + ``ForgeRunner``
  together under ``name = "forge"``.

Forge is discovered by ``cstar.applications.core.get_application`` as a
**built-in** application, the same way ``roms_marbl`` is: ``get_application``
imports the in-tree
``cstar.applications.forge`` package, whose ``__init__.py`` imports
``app.py`` so its ``@register_application`` decorator runs. No entry point
or environment variable is involved -- an installed ``cstar-ocean`` is the
whole requirement.

Two ways to run a forge blueprint:

1. ``cstar blueprint run forge_blueprint.yaml`` -- the app-framework path
   (defaults only; no forge-specific options), the no-frills front door.
2. ``cstar forge run forge_blueprint.yaml`` -- the ``cli/forge`` typer
   sub-app, a native typer command with the full executor option set,
   calling ``cstar.applications.forge.runtime.run_blueprint``. Reach for
   this for per-run options ``cstar blueprint run`` doesn't expose (stage
   selection, ``--clobber``, dask tuning, ``--only-inputs``, verbosity).

The call chain end to end
---------------------------------

**Authoring (catalog -> resolver/wizard -> blueprint):**

1. ``wiz = ForgeBlueprintWizard()`` (``cstar/wizard/wizard.py``) -- scans
   the catalog via ``cstar.catalog.default_catalog``, populates dropdowns;
   entries from lower layers (e.g. the bundled catalog) are shown with a
   ``(bundled)`` badge. The notebook entry point loads
   ``cstar/wizard/_voila_app.ipynb`` (served via ``cstar forge wizard``) or
   the standalone ``forge-blueprint-wizard.ipynb`` (via ``cstar forge
   copy-notebook``); either shows a catalog-location bar above the wizard
   (auto-loads the default layered stack -- your writable ``~/cstar/catalog``/
   ``CSTAR_CATALOG`` layer over the read-only bundled catalog; Reload
   rebuilds a fresh wizard against a different single local
   path/``"local"``/GitHub URL/http URL, or several ``os.pathsep``-separated
   locations to build a new ``LayeredCatalog``, keeping the previous wizard
   on failure). Saves (blueprints, workplans) and catalog registrations land
   in the stack's writable top layer -- never inside the installed package.
   *Presentation:* ``ForgeBlueprintWizard.widget`` lays the same widget
   objects out as numbered section cards (``cstar.wizard.ui.components.card``),
   label/control field rows (``field_row`` -- clears the widget's own
   ``description``, mirrors its ``layout.display`` so existing hide/show
   code still hides the whole row) and independently collapsible panes
   (``open_accordion``). Every visible label, symbol, unit and hint comes
   from ``cstar/wizard/ui/labels/blueprint-wizard.yaml`` via
   ``label_for(key)`` (key namespaces: bare widget attr, ``grid.<k>``,
   ``forcing.row.<key>``, ``ic.<attr>``, ``boundary.<attr>``,
   ``settings.<section>[.<field>]``, ``buttons.<name>``); a missing key
   falls back to the raw name, and the wizard's UI-label tests check every
   key names a real widget or namelist field. ``_rebuild()`` also refreshes
   the sticky status bar, per-card chips and accordion titles
   (``_update_status``). The CSS travels inside the widget tree so the
   plain-notebook path is styled too; only the Voila page column
   (``body[data-voila] .forge-shell``) is shell-specific.
2. User picks a domain -> ``_on_domain()`` prefills
   grid/boundaries/partitioning/dates from ``catalog.domain_data(name)``.
3. Every edit -> ``_rebuild()`` -> ``build_forge_blueprint(**self._gather())``
   (``cstar/applications/forge/resolve.py``) -- reads the single
   consolidated ``model.yaml`` directly as a dict (no Pydantic here;
   ``code`` + flat ``model_settings``, no embedded forcing/output defaults
   -- a ForcingSpec and OutputSpec must always be supplied explicitly),
   resolves dataset keys via ``source_registry``, computes pure-derived
   settings (CFL ``dt``, ``v_sponge``, etc.), returns a ``ForgeBlueprint``.
4. ``wiz.config.to_yaml(path)`` writes the portable ``forge_blueprint.yaml``.

**Execution (blueprint -> engine -> executor), same machine or a different one:**

5. ``cstar blueprint run forge_blueprint.yaml`` (or ``cstar forge run ...``)
   -- resolves the host via ``cstar.applications.forge.config.resolve_host()``
   (machine tag, ``source_data_cache``, ``working_dir`` override).
6. ``cstar.applications.forge.engine.process_forge_blueprint(cfg, host, ...)``
   builds a ``ForgeExecutor`` via ``ForgeExecutor.from_forge_blueprint(cfg,
   host)`` and drives: ``ensure_source_data()`` -> ``generate_inputs()`` ->
   ``configure_build()``.
7. Outputs land under ``host.working_dir``: input NetCDFs, ``namelist.nml``,
   ``cppdefs.opt``, and the emitted downstream ``roms_marbl`` blueprint YAML
   (``B_{name}.yaml``, persisted once by ``configure_build()`` -- there is
   no per-stage blueprint file).

``ForgeExecutor`` never imports ``cstar.applications.forge.config``/
``cstar.catalog``/``cstar.applications.forge.resolve``/``cstar.wizard`` --
verified both by grep and by a dedicated boundary-guard test
(``cstar/tests/unit_tests/applications/forge/test_forge_app_boundary.py``,
an AST-based check). The guard walks every ``.py`` file in
``cstar/applications/forge`` except a small, fixed exclusion set for the
authoring/host modules themselves (``resolve``, ``models``, ``config``,
``runtime``, ``app``, ``__init__``) and asserts none of the rest imports
``cstar.catalog``, ``cstar.applications.forge.resolve``,
``cstar.applications.forge.config``, ``cstar.applications.forge.runtime``, or
``cstar.wizard``. Its known-violations allowlist (for pre-existing breaks
still being worked off) is currently empty. ``namelist_model.py`` and
``util.py`` are same-package siblings inside ``cstar.applications.forge`` and
are covered by this module list like any other execution module.

Versioned namelist schemas (ucla-roms 0.5.0+)
-------------------------------------------------------

ucla-roms 0.5.0 made its first breaking namelist change (``nrpf_rst``
removed from ``&BASIC_OUTPUT_SETTINGS``; ``&PARTICLES_SETTINGS``
``output_period``/``nrpf`` renamed to
``output_period_particles``/``nrpf_particles``); 0.6.0 added
``&PIO_SETTINGS`` (``pio_stride``, required under ``PARALLEL_IO``); 0.7.0
adds ``&CDR_TRACER_OUTPUT_SETTINGS`` and ``&CDR_GAS_EXCH_OUTPUT_SETTINGS``
(ucla-roms PR #351 -- two dedicated CDR output streams). C-Star versions the
namelist schema by ucla-roms release (``cstar.roms.namelist``:
``RomsNamelist`` for < 0.5.0, ``RomsNamelistV0_5_0`` for 0.5.0 <= ucla-roms
< 0.6.0, ``RomsNamelistV0_6_0`` for 0.6.0 <= ucla-roms < 0.7.0,
``RomsNamelistV0_7_0`` for >= 0.7.0, selected by
``namelist_schema_for_ref(ref)`` -- semver tags select exactly; branch
names/hashes warn and fall back to the latest schema). Forge mirrors this in
``namelist_model.py``: ``RunTimeSettings`` (legacy), ``RunTimeSettingsV0_5_0``,
``RunTimeSettingsV0_6_0`` (adds ``pio_settings``), and
``RunTimeSettingsV0_7_0`` (adds ``cdr_tracer_output``/``cdr_gas_exch_output``),
selected by ``run_time_settings_for_ref(roms_ref)``, where ``roms_ref`` is
the blueprint's pinned ``code.roms.commit`` (threaded resolver -> executor ->
``write_roms_namelist``). C-Star's registry is the single source of
version-boundary truth -- forge only maps its result to the matching
settings class. The forge **settings vocabulary is version-stable**: YAML
keys (``particles.output_period``, ``particles.nrpf``) don't change; only
the ``serialization_alias`` to namelist names differs per version, and
``nrpf_rst`` (still present in the shared ``OutputSpec/standard``) is
silently ignored for 0.5.0+ models via ``extra="ignore"``. One ModelSpec per
tagged ucla-roms release: ``roms-marbl-0.5-default`` pins ``0.5.0``,
``roms-marbl-0.6-default`` pins ``0.6.4``, ``roms-marbl-0.7-default`` pins
``0.7.0``, ``roms-marbl-0.8-default`` pins ``0.8.0`` (adds the
``parabolic_splines``/``upstream_ts_land_curv`` advection cppdefs flags, PR
#361, with no new settings tier -- it still resolves to
``RunTimeSettingsV0_7_0``); older specs stay fixed and keep emitting
byte-identical legacy namelists. ``version_gated_section_names()``
(``namelist_model.py``) collects every section modeled by at least one
non-legacy tier (``pio_settings``, ``cdr_tracer_output``,
``cdr_gas_exch_output``) -- used by the wizard's ``_SettingsEditor``
(``cstar/wizard/wizard.py``) to skip rendering a widget for a
version-gated section absent from the *active* schema.

ucla-roms 0.5.0 also added a run-start precheck (``check_output_divides_rst``):
each enabled output stream's ``nrpf x output_period`` must evenly divide
``output_period_rst`` (vacuous for monthly restarts / a 0 period). Three
bundled OutputSpecs conform for every stream -- ``daily-restarts`` (the
wizard default, see ``_DEFAULT_OUTPUT_SPEC``), ``weekly-restarts``, and
``monthly-restarts`` (upstream's own convention: ``monthly_restarts=T``,
``output_period_rst=0``). ``OutputSpec/standard`` predates the precheck and
is kept unchanged for blueprints that reference it -- enabling its his/avg
streams under a 0.5.0+ model trips the precheck. A guard test
(``test_bundled_output_specs_satisfy_roms_divides_rst_precheck``) pins the
conforming specs, including ``roms-marbl-0.5-default``'s ModelSpec-owned
sponge/particles streams. The nesting extract stream is resolve-time-derived
(child DomainSpec metadata ``period`` x a seeded ``nrpf``), so it's enforced
at authoring time instead: ``check_extract_divides_rst``
(``namelist_model.py``), called from the resolver and gated to >= 0.5.0
pins.

``models.py`` vs ``blueprint.py``
------------------------------------------

The forcing/IC item models (``BoundaryForcing``, ``SurfaceForcingItem``,
``InitialConditions``, ``BgcSourceItem``, ``OpenBoundaries``, etc.) are
defined once, in ``cstar/applications/forge/blueprint.py``; ``models.py``
imports and re-exports them -- single source of truth, no duplication. What
``models.py`` owns is the ``model.yaml`` *wrapper* shape (``ModelSpec``,
``ModelCode``, ``ModelTemplates``, ``load_models_yaml``) used by
``DomainCatalog.load_model_spec()`` for full Pydantic validation at
catalog-registration time -- a heavier, separate path from the resolver's
plain-dict read of the same file. The guarded ``cstar.applications.forge``
execution modules never import ``cstar.applications.forge.models``.

What guards drift today: a roms-tools option coverage test and a
resolver/executor settings-parity assertion in the forge blueprint test
suite.

Known gaps / open items
-------------------------------

1. **The flat-staging contract with ``AdditionalCode`` is verified only by
   hash.** Rendering reads ``template_dir/<file>`` directly, so it relies on
   C-Star staging fetched files flat; the per-file hash check catches a wrong
   layout as a mismatch, but every staging test patches ``AdditionalCode`` --
   no test stages from the real remote. (This path only runs for a blueprint
   whose pinned commit doesn't match the bundled templates; see
   :doc:`forge_templates`.)
2. **No real-generated-data integration test** (actual GLORYS/ERA5/TPXO
   network fetch with no roms-tools mocking) -- the golden tests below mock
   roms-tools construction classes.

Golden fixtures
-----------------------

Two committed goldens pin the resolved-settings and namelist contracts
(treat any diff as a behavior change to justify, not noise):

- **Settings-level**: ``test_golden_model_settings_test_tiny`` diffs
  resolved ``model_settings`` against
  ``golden_model_settings_test-tiny.json``. No regeneration hook -- update
  manually. Three sibling tests pin the same comparison for each
  versioned-namelist schema tier: ``test_golden_model_settings_test_tiny_roms050``,
  ``_roms060``, and ``_roms070`` (``roms-marbl-0.{5,6,7}-default``, against
  ``golden_model_settings_test-tiny-roms0{50,60,70}.json``).
- **Byte-exact namelist**: ``TestGoldenNamelist::test_golden_namelist_test_tiny``
  drives the real ``generate_inputs()`` -> ``configure_build()`` chain (real
  ``write_roms_namelist``; only roms-tools construction classes are mocked)
  and diffs the rendered ``namelist.nml`` against
  ``golden_namelist_test-tiny.nml`` (host-rooted absolute paths normalized
  to a ``<WORKDIR>`` token). Three sibling tests pin the versioned-namelist
  schemas against the same test-tiny domain/forcing/output setup:
  ``test_golden_namelist_test_tiny_roms050`` (``roms-marbl-0.5-default``,
  ``golden_namelist_test-tiny-roms050.nml``),
  ``test_golden_namelist_test_tiny_roms060`` (``roms-marbl-0.6-default``,
  adds ``&PIO_SETTINGS``, ``golden_namelist_test-tiny-roms060.nml``), and
  ``test_golden_namelist_test_tiny_roms070`` (``roms-marbl-0.7-default``,
  adds ``&CDR_TRACER_OUTPUT_SETTINGS``/``&CDR_GAS_EXCH_OUTPUT_SETTINGS``,
  ``golden_namelist_test-tiny-roms070.nml``). Regenerate one at a time via
  ``UPDATE_GOLDEN=1 pytest <path> -k <test name>`` (the run intentionally
  fails after writing; rerun without the env var to confirm). To select
  *only* the legacy test, use ``-k "golden_namelist_test_tiny and not
  roms050 and not roms060 and not roms070"`` -- a bare ``-k
  golden_namelist_test_tiny`` matches all four.
