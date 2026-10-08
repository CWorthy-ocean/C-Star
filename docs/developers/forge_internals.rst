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
    |   |   +-- migration.py               # forge_blueprint_version forward-migrations (split out so
    |   |   |                              # importing blueprint.py alone stays light)
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
    |   |                                  # already-parsed option values (called by cli/forge's 'run');
    |   |                                  # publish_emitted_blueprint(): copies B_{name}.yaml to output/
    |   +-- core.py                    # get_application() / register_application / ApplicationDefinition
    |   +-- roms_marbl/                # the roms_marbl application forge's output feeds into
    +-- catalog/                       # Bundled + layered spec catalog (see docs/developers/catalog_design.rst)
    |   +-- bundled/
    |   |   +-- ModelSpec/{model}/model.yaml    # Code repos, templates, settings, defaults
    |   |   +-- DomainSpec/{grid}/Domain.yaml   # Grid definitions
    |   |   +-- ForcingSpec/{name}/Forcing.yaml # Forcing source configurations
    |   |   +-- OutputSpec/{name}/Output.yaml   # Output configurations
    |   |   +-- blueprints/forge/{name}.yaml    # Example blueprints (bundled, read-only layer;
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
change; currently 10) - ``application`` (=``"forge"``, C-Star app
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
(``ForgeProvenance``: the ``Blueprint`` base's ``generated_at``/``generated_by``/
``derived_from`` plus ``content_hash``, ``notes`` and the legacy version
fields). The ``Blueprint`` base also adds
``state``/``schema_version`` (its own versioning metadata, distinct from
``forge_blueprint_version``) and injects a ``$schema`` key on serialization
(stripped back out on load).

Older blueprint files load transparently: a ``model_validator(mode="before")``
(``cstar.applications.forge.migration.migrate_forge_blueprint_data``, imported
lazily by ``blueprint.py`` so a plain import of the blueprint schema stays
light) migrates v2/v3 layouts (removed
``identity`` sub-model, removed ``ensemble_id``), the v4->v5
``do_cdr``->``do_cdr_output`` rename, the v6->v7 CDR move
(``forcing.cdr_forcing``/``cdr_forcing_file`` -> the top-level ``cdr``
section, mode inferred), the v7->v8 BGC-sources move
(``initial_conditions.bgc_source`` rewrapped as a one-item ``bgc_sources``
list; ``forcing.boundary``'s flat, type-discriminated
``BoundaryForcingItem`` list split into a single ``BoundaryForcing`` section
with ``source`` + ``bgc_sources``, mirroring ``InitialConditions``), and the
v8->v9 ``working_dir`` strip (the old default-form
``~/cstar/_forge_bp_runs/<name>`` is removed so the blueprint takes the base
class's default; a deliberately set path is kept) and the v9->v10 CDR_LITE
rename (``model_settings.cdr_tracer_output`` -> ``cdr_lite_output``, its
``do_cdr_tracer_output`` flag -> ``do_cdr_lite_output``; a section or flag
carrying both names raises) to the current shape, reproducing derived names
bit-for-bit. The rename is applied on every load, not only to pre-v10 files:
workplan blueprint overrides merge onto an already-v10 dump and re-validate, so
a legacy name can re-enter current-version data there (next to an existing
``cdr_lite_output`` it fails at schedule time with the "both" message). The same bump turns a
hand-authored composition spec's ``name: null`` into ``""`` (``SpecRef.name`` is a
plain string; also applied on every load) and adds the
``provenance.generated_by``/``derived_from`` keys, which a v9 install rejects as
unknown, so the bump makes it say "upgrade cstar-ocean" instead.
``model_name``/``grid_name`` live in ``composition.model.name``/``domain.grid_name``;
``grid_name`` is results-affecting -- ``SourceDatasets`` keys cache
filenames off it.

- **``working_dir``** is the single per-run artifact root -- everything the
  executor *produces* lands under it. ``ForgeBlueprint`` does not redeclare
  it: it inherits the ``Blueprint`` base field (``Path | None``, default
  ``None``), and the base property ``effective_working_dir`` resolves the
  default, ``CSTAR_DATA_HOME/blueprint_runs/forge/<name>``. It's
  host/location, not results-affecting, so it's excluded from
  ``content_hash``.
- **``content_hash()``** -- sha256 over everything *except*
  ``forge_blueprint_version``, ``name``, ``description``, ``composition``,
  ``provenance``, ``working_dir``, ``state``, ``schema_version``,
  ``$schema`` (see ``_HASH_EXCLUDE``); each code repo's ``location`` (fetch
  address) and ``file_hashes`` (a derived cache, not independent content);
  each user-provided file's ``location`` (host path, not its pinned
  ``content_hash``); and, on ``initial_conditions``/``boundary``, the
  execution-environment knobs ``bypass_validation`` and each bgc source's
  ``serialize_dask`` -- none of these change what the run produces, only how
  or where it's produced. Recorded on ``to_yaml``; ``verify_content_hash``
  warns (doesn't block) on a mismatched hand-edit at load.
- **``stamp_provenance(tool)``** -- the one place a blueprint's provenance is
  stamped. When the recomputed ``content_hash`` equals the recorded one and a
  ``generated_by`` is recorded it returns ``self`` (a matching hash alone is not
  enough: ``to_yaml_str`` records it on every write, stamped or not); otherwise a
  copy with the new hash, ``generated_at`` now (UTC) and a fresh ``generated_by``
  from ``new_generated_by(tool)`` (the tool, a uuid4 minted once, the system, and
  ``generation_versions()``: the ``cstar-ocean`` and ``roms-tools`` versions),
  with the legacy ``forge_version``/``cstar_version``/``roms_tools_version``
  cleared. ``to_yaml_str`` only serializes and fingerprints
  (``provenance`` last, an empty ``derived_from`` dropped), so a producer stamps
  first; the wizard does so in ``ForgeBlueprintWizard._save_config``, which
  every write of its config to disk goes through, and carries the stamp through
  re-resolves (``build_forge_blueprint(provenance=...)``).
- **``emitted_provenance(blueprint, run_id=..., working_dir=...)``** -- the one
  place the *emitted* ``roms_marbl`` blueprint's provenance is assembled (and
  ``emitted_blueprint_description(name, description)`` its description), built
  from ``producer_ref``, ``composition_refs`` and ``new_generated_by``. Unlike a
  forge blueprint's, it is minted afresh by every run, never carried over:
  ``generated_by`` is a forge event happening now (tool ``forge``, a uuid4, the
  system, ``generation_versions()``, the workplan ``run_id`` if the process
  inherited ``CSTAR_RUNID``, and the working directory), and ``derived_from`` is
  the forge blueprint (``producer_ref``: name and ``content_hash``, the same
  reference ``ForgeApplication.emitted_blueprint`` predicts as the producer)
  followed by a ``CatalogSpecRef`` for each *named* ``composition`` entry, in the
  order model, domain, forcing, cdr, output.

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
  ``configure_build``, publishes the emitted blueprint (below), then reports
  ``ExecutionStatus.COMPLETED``. Scope: generates inputs and emits the
  downstream ``roms_marbl`` blueprint (``B_{name}.yaml``), then stops -- the
  existing ``roms_marbl`` application consumes that blueprint separately.
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

Both publish the emitted blueprint through
``cstar.applications.forge.runtime.publish_emitted_blueprint``, which copies
``blueprints/B_{name}.yaml`` (not the settings sidecar) to
``<working_dir>/output/``: the location a workplan's deferred ``from_step``
reference looks in, and the one ``ForgeApplication.emitted_blueprint``
documents. ``cstar forge run`` prints that copy as the blueprint to run, unless
it stopped before ``configure_build`` (``--no-configure``, ``--only-inputs``).

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
4. Save (``wiz._save_config(path)``) stamps the provenance and writes the portable
   ``forge_blueprint.yaml`` (``stamp_provenance`` then ``to_yaml``).

**Execution (blueprint -> engine -> executor), same machine or a different one:**

5. ``cstar blueprint run forge_blueprint.yaml`` (or ``cstar forge run ...``)
   -- resolves the host via ``cstar.applications.forge.config.resolve_host()``
   (machine tag, ``source_data_cache``, and the working directory: the
   blueprint's ``effective_working_dir`` or the ``--working-dir`` override,
   used as written).
6. ``cstar.applications.forge.engine.process_forge_blueprint(cfg, host, ...)``
   builds a ``ForgeExecutor`` via ``ForgeExecutor.from_forge_blueprint(cfg,
   host)`` and drives: ``ensure_source_data()`` -> ``generate_inputs()`` ->
   ``configure_build()``, which it hands the emitted blueprint's provenance,
   minted once for the call (``emitted_provenance``; ``run_id`` is read from the
   ``CSTAR_RUNID`` a workplan step inherits, and is empty for a standalone run).
7. Outputs land under ``host.working_dir``: input NetCDFs, ``namelist.nml``,
   ``cppdefs.opt``, and the emitted downstream ``roms_marbl`` blueprint YAML
   (``B_{name}.yaml``, persisted once by ``configure_build()`` -- there is
   no per-stage blueprint file). The emitted blueprint leaves ``working_dir``
   unset, so it runs under
   ``CSTAR_DATA_HOME/blueprint_runs/roms_marbl/<name>``. Its ``description``
   says it was generated by forge from the forge blueprint of that name, and its
   ``provenance`` is the one the engine handed ``configure_build``; ``persist()``
   leaves the block out when none was supplied (a direct use of the executor),
   so such a file stays loadable by a C-Star that predates the field.
8. The entry point publishes a copy to ``<working_dir>/output/B_{name}.yaml``
   (``publish_emitted_blueprint``).

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

Versioned namelist schemas (ucla-roms 0.4.0+)
-------------------------------------------------------

ucla-roms 0.4.0 added ``nt_cdr_oae``/``nt_cdr_dor`` (passive CDR tracer
counts) to ``&PARAM_SETTINGS``, both defaulting to 0 (the Fortran
initializer) so a namelist without them still validates; 0.5.0 made the
first breaking namelist change (``nrpf_rst`` removed from
``&BASIC_OUTPUT_SETTINGS``; ``&PARTICLES_SETTINGS``
``output_period``/``nrpf`` renamed to
``output_period_particles``/``nrpf_particles``); 0.6.0 added
``&PIO_SETTINGS`` (``pio_stride``, required under ``PARALLEL_IO``); 0.7.0
adds ``&CDR_TRACER_OUTPUT_SETTINGS`` and ``&CDR_GAS_EXCH_OUTPUT_SETTINGS``
(ucla-roms PR #351 -- two dedicated CDR output streams); 0.9.0 renames the
former ``&CDR_LITE_OUTPUT_SETTINGS`` (PR #372: five keys renamed, plus PR
#370's ``wrt_gas_exchange``; now compiled without MARBL/CDR_FORCING) and adds
the optional ``&CDR_LITE_SETTINGS``. C-Star versions the
namelist schema by ucla-roms release (``cstar.roms.namelist``:
``RomsNamelist`` for < 0.4.0, ``RomsNamelistV0_4_0`` for 0.4.0 <= ucla-roms
< 0.5.0 (adds the CDR tracer counts to ``&PARAM_SETTINGS``),
``RomsNamelistV0_5_0`` for 0.5.0 <= ucla-roms < 0.6.0, ``RomsNamelistV0_6_0``
for 0.6.0 <= ucla-roms < 0.7.0, ``RomsNamelistV0_7_0`` for 0.7.0 <= ucla-roms
< 0.9.0, ``RomsNamelistV0_9_0`` for >= 0.9.0 (a ``RomsNamelistV0_6_0``
subclass, since a subclass can't drop V0_7_0's renamed group), selected
by ``namelist_schema_for_ref(ref)`` -- semver tags select exactly; branch
names/hashes warn and fall back to the latest schema). Forge mirrors this in
``namelist_model.py``: ``RunTimeSettings`` (legacy, < 0.4.0),
``RunTimeSettingsV0_4_0`` (adds ``param.nt_cdr_oae``/``param.nt_cdr_dor``),
``RunTimeSettingsV0_5_0``, ``RunTimeSettingsV0_6_0`` (adds ``pio_settings``),
``RunTimeSettingsV0_7_0`` (adds ``cdr_lite_output``/``cdr_gas_exch_output``),
and ``RunTimeSettingsV0_9_0`` (``cdr_lite_output`` with the 0.9 key names and
``wrt_gas_exchange``, plus ``cdr_lite``; a ``RunTimeSettingsV0_6_0`` subclass
like its C-Star counterpart),
selected by ``run_time_settings_for_ref(roms_ref)``, where ``roms_ref`` is
the blueprint's pinned ``code.roms.commit`` (threaded resolver -> executor ->
``write_roms_namelist``). C-Star's registry is the single source of
version-boundary truth -- forge only maps its result to the matching
settings class. The forge **settings vocabulary is version-stable**: YAML
keys (``particles.output_period``, ``particles.nrpf``,
``cdr_lite_output.do_avg``) don't change; only
the ``serialization_alias`` to namelist names differs per version, and
``nrpf_rst`` (still present in the shared ``OutputSpec/standard``) is
silently ignored for 0.5.0+ models via ``extra="ignore"``. The one
exception is ``cdr_lite_output``, which was named ``cdr_tracer_output``
(enable flag ``do_cdr_tracer_output``) before ucla-roms 0.9.0's CDR_LITE
rename: forge blueprint v10 migrates stored blueprints, and the resolver
renames the old section in an OutputSpec/ModelSpec/override with a warning
(``normalize_legacy_sections``, the only place the old names live). Total tracer
count (``n_tracers``, threaded into ``build_namelist``/``render_roms_settings``
to size the per-tracer mixing/diffusion arrays) is derived by
``n_tracers_from_param``: T + S + ``ntrc_bio`` + ``nt_passive`` +
``2*nt_cdr_oae + nt_cdr_dor`` (each OAE tracer is an ALK/DIC pair, mirroring
ucla-roms ``param.F90``); below 0.4.0, a non-zero ``nt_cdr_oae``/
``nt_cdr_dor`` in the settings dict is rejected outright (``ParamCfg``'s
before-validator) rather than silently miscounted. One ModelSpec per tagged
ucla-roms release: ``roms-marbl-0.5-default`` pins ``0.5.0``,
``roms-marbl-0.6-default`` pins ``0.6.4``, ``roms-marbl-0.7-default`` pins
``0.7.0``, ``roms-marbl-0.8-default`` pins ``0.8.0`` (adds the
``parabolic_splines``/``upstream_ts_land_curv`` advection cppdefs flags, PR
#361, with no new settings tier -- it still resolves to
``RunTimeSettingsV0_7_0``), ``roms-marbl-0.9-default`` pins ``0.9.0``
(``RunTimeSettingsV0_9_0``, which models the ``cdr_lite`` section; the spec does
not declare it, see below); older specs stay
fixed and keep emitting
byte-identical legacy namelists. ``version_gated_section_names()``
(``namelist_model.py``) collects every section modeled by at least one
non-legacy tier (``pio_settings``, ``cdr_lite_output``,
``cdr_gas_exch_output``, ``cdr_lite``) -- used by the wizard's ``_SettingsEditor``
(``cstar/wizard/wizard.py``) to skip rendering a widget for a
version-gated section absent from the *active* schema, and by
``prune_version_gated_sections()``, which drops such a section from the
settings dict before the CDR output nets and the output-stream precheck read
it -- called by both the resolver and the executor's ``configure_build``
(the net for stored blueprints; it logs what it drops at INFO). Dropping a
section whose enable switch is on (e.g. ``cdr_lite.cdr_online_carbonate_sensitivity``
under a 0.8 pin) raises instead: the pinned release can't honor it. ``param`` is
modeled by every tier (with a different sub-model above/below 0.4.0), so it is
never section-gated; no bundled ModelSpec declares ``nt_cdr_oae``/``nt_cdr_dor``,
so the editor shows no widget for them, but a loaded blueprint's values are
carried through unchanged.

``CDR_LITE`` (ucla-roms >= 0.9.0, formerly ``CDR_TRACER``) is a
resolver-owned cppdef: ``check_cdr_lite_sections`` turns it on when
``cdr_lite.cdr_online_carbonate_sensitivity`` is set (MARBL computes the
carbonate sensitivities from its ALT_CO2 state), and rejects the
combinations ucla-roms would abort on at init (online sensitivity without
MARBL; ``wrt_gas_exchange`` without ``CDR_LITE``; CDR-lite output or gas
exchange with no CDR-lite tracers). Compiling ``CDR_LITE`` with
file-based sensitivities (``ddic_dco2``/``ddic_dalk`` forcing) is not yet
supported, so a user ``cppdefs.cdr_lite`` without online sensitivity is
rejected at resolve time. Unlike ucla-roms 0.7/0.8's stream, 0.9's
``cdr_lite_output`` does not force ``CDR_FORCING`` on: the per-tier rows of
``CDR_OUTPUT_SECTIONS`` and of the precheck section table apply only when
the row's settings class is the pinned tier's own annotation for that section.
``&TRACER_DIFF2`` is read from 0.9.0 on (earlier releases skipped it through
an ``#if define`` typo), so a nonzero ``tracer_diff2.tnu2_default`` only
takes effect there; every bundled ModelSpec uses 0.0.

The bundled ModelSpecs do not declare ``cdr_lite`` yet, so the knob is absent
from the wizard and ``&CDR_LITE_SETTINGS`` is written at its schema default
(off): CDR-lite tracers (``nt_cdr_oae``/``nt_cdr_dor`` > 0) also need per-tracer
surface-flux forcing (``CDR_OAE_DIC<n>_flx``/``CDR_DOR_DIC<n>_flx`` on
``CDR_time``) that Forge does not generate, and ROMS aborts looking it up in the
forcing files. The CDR-lite BGC-mode follow-up wires that forcing up. The
plumbing above (the ``cppdefs.cdr_lite`` derivation, ``check_cdr_lite_sections``,
``RunTimeSettingsV0_9_0.cdr_lite``) stays in place, and the section can still be
set through ``run_time_overrides``.

ucla-roms 0.5.0 also added a run-start precheck (``check_output_divides_rst``):
each enabled output stream's ``nrpf x output_period`` must evenly divide
``output_period_rst`` (vacuous for monthly restarts / a 0 period). ``cstar.roms.precheck``
is the sole home for this and the sibling restart-period rule -- both C-Star
(``ROMSSimulation.roms_runtime_settings``) and Forge (the resolver, and the
executor's ``configure_build`` net for stored blueprints) call straight into
it rather than keeping their own copies:

* ``check_output_streams_divide_rst`` reproduces the full ``do_precheck`` call
  list (every stream, including the nesting ``extract`` stream) against
  either a live ``RomsNamelistBase`` or a canonical namelist-vocabulary
  mapping; ``applies_to(schema)`` owns the ">= 0.5.0" schema gate, and a
  violation raises ``NamelistConsistencyError`` (a ``ValueError`` subclass)
  carrying the canonical ``section``/``keys`` of the offending stream.
* ``check_restart_period_divisible_by_dt`` is a separate, ungated C-Star
  reproducibility convention (not a ucla-roms abort -- ucla-roms' restart
  trigger is a running-clock threshold, not a step-count division): restart
  writes must land on a timestep.

Three bundled OutputSpecs conform for every stream -- ``daily-restarts`` (the
wizard default, see ``_DEFAULT_OUTPUT_SPEC``), ``weekly-restarts``, and
``monthly-restarts`` (upstream's own convention: ``monthly_restarts=T``,
``output_period_rst=0``). ``OutputSpec/standard`` predates the precheck and
is kept unchanged for blueprints that reference it -- enabling its his/avg
streams under a 0.5.0+ model trips the precheck. A guard test
(``test_bundled_output_specs_satisfy_roms_divides_rst_precheck``) pins the
conforming specs, including ``roms-marbl-0.5-default``'s ModelSpec-owned
sponge/particles streams. The nesting extract stream is resolve-time-derived
(child DomainSpec metadata ``period`` x a seeded ``nrpf``); the resolver runs
the same canonical checker as everything else, but catches
``NamelistConsistencyError`` and, when ``exc.section == "extract_data_settings"``,
appends a hint naming the DomainSpec ``period`` knob before re-raising --
Forge's ``forge_field_for(section, key)`` (``namelist_model.py``) is the more
general version of that translation, a reverse lookup from a
``NamelistConsistencyError``'s canonical section/key back to the forge
settings-dict field the wizard edits.

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
  manually. Four sibling tests pin the same comparison for each
  versioned-namelist schema tier: ``test_golden_model_settings_test_tiny_roms050``,
  ``_roms060``, ``_roms070``, and ``_roms090`` (``roms-marbl-0.{5,6,7,9}-default``,
  against ``golden_model_settings_test-tiny-roms0{50,60,70,90}.json``).
- **Byte-exact namelist**: ``TestGoldenNamelist::test_golden_namelist_test_tiny``
  drives the real ``generate_inputs()`` -> ``configure_build()`` chain (real
  ``write_roms_namelist``; only roms-tools construction classes are mocked)
  and diffs the rendered ``namelist.nml`` against
  ``golden_namelist_test-tiny.nml`` (host-rooted absolute paths normalized
  to a ``<WORKDIR>`` token). Four sibling tests pin the versioned-namelist
  schemas against the same test-tiny domain/forcing/output setup:
  ``test_golden_namelist_test_tiny_roms050`` (``roms-marbl-0.5-default``,
  ``golden_namelist_test-tiny-roms050.nml``),
  ``test_golden_namelist_test_tiny_roms060`` (``roms-marbl-0.6-default``,
  adds ``&PIO_SETTINGS``, ``golden_namelist_test-tiny-roms060.nml``),
  ``test_golden_namelist_test_tiny_roms070`` (``roms-marbl-0.7-default``,
  adds ``&CDR_TRACER_OUTPUT_SETTINGS``/``&CDR_GAS_EXCH_OUTPUT_SETTINGS``,
  ``golden_namelist_test-tiny-roms070.nml``), and
  ``test_golden_namelist_test_tiny_roms090`` (``roms-marbl-0.9-default``,
  ``&CDR_LITE_OUTPUT_SETTINGS``/``&CDR_LITE_SETTINGS`` in place of the
  CDR_TRACER group, ``golden_namelist_test-tiny-roms090.nml``). Regenerate one at a time via
  ``UPDATE_GOLDEN=1 pytest <path> -k <test name>`` (the run intentionally
  fails after writing; rerun without the env var to confirm). To select
  *only* the legacy test, use ``-k "golden_namelist_test_tiny and not
  roms050 and not roms060 and not roms070 and not roms090"`` -- a bare ``-k
  golden_namelist_test_tiny`` matches all five.
