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
  that file into UCLA-ROMS input NetCDFs, a namelist, and a downstream
  blueprint, plus the **executor** that does the actual work. Execution
  never touches the catalog.

The file that crosses the boundary is ``ForgeBlueprint`` -- **the forge
application's own blueprint**. Terminology trap to avoid: C-Star also has an
existing, unrelated ``ucla_roms`` application whose blueprint
(``UclaRomsBlueprint``) forge *emits as an output artifact*. "Building a
blueprint" means producing that downstream artifact, not forge's own input.

Two invariants keep the boundary honest. A forge blueprint **snapshots** the
values it took from the catalog (grid parameters, forcing selections, settings)
rather than referencing catalog entries, so a saved blueprint keeps producing
the same domain however the catalog is later edited, moved or relayered. And
the **executor never reads the catalog**: only the resolver and the wizard do,
which a boundary-guard test enforces.

.. code-block:: text

    catalog specs  -+
    (Model/Domain/  |-> build_forge_blueprint() -> ForgeBlueprint -> process_forge_blueprint(cfg, host)
     Forcing/Output)|         (resolver)         (.yaml,               (engine -> executor)
                    |                             portable)           |
    wizard UI ------+                                                 v
                                                        input NetCDFs, namelist.nml,
                                                        cppdefs.opt, ucla_roms blueprint

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
    |   |   +-- input_generator.py         # Input file generation
    |   |   +-- source_datasets.py         # Dataset download and preparation
    |   |   +-- source_registry.py         # Dataset alias map / provenance metadata (stdlib-only)
    |   |   +-- glorys_subchunk.py         # Just-in-time kerchunk subchunking for GLORYS
    |   |   +-- settings.py                # Template rendering
    |   |   +-- namelist_model.py          # RunTimeSettings + build_namelist
    |   |   +-- user_files.py              # User-provided netCDF attachments (grid/river/CDR)
    |   |   +-- util.py                    # Shared helpers, memory/timing instrumentation
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
    |   +-- ucla_roms/                 # package of the ucla_roms application forge's output feeds into
    +-- roms/
    |   +-- outputs.py                 # Names and models of the files a ROMS step writes
    |   +-- file_system.py             # RomsFileSystemManager: run-directory layout
    +-- catalog/                       # Bundled + layered spec catalog (see docs/catalog_wizard/catalog.rst)
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

``glorys_subchunk.py`` is called from ``input_generator.py`` and is covered by the
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
- **``content_hash()``** -- ``bp-v<N>:`` (``N`` = ``HASH_RECIPE_VERSION``,
  ``cstar/base/provenance.py``, bumped with any change to the recipe) plus the
  sha256 of the fields that differ from their defaults, so an additive field
  with a default changes no recorded hash (``Blueprint._hashed_content``). It
  hashes everything *except* the base metadata (``BLUEPRINT_METADATA_FIELDS``:
  ``application``, ``name``, ``description``, ``provenance``, ``working_dir``,
  ``state``, ``schema_version``) and ``$schema``;
  ``forge_blueprint_version`` and ``composition`` (``_HASH_EXCLUDE``); each
  code repo's ``location`` (fetch
  address) and ``file_hashes`` (a derived cache, not independent content);
  each user-provided file's ``location`` (host path, not its pinned
  ``content_hash``); and, on ``initial_conditions``/``boundary``, the
  execution-environment knobs ``bypass_validation`` and each bgc source's
  ``serialize_dask`` -- none of these change what the run produces, only how
  or where it's produced. Recorded on ``to_yaml``; ``verify_content_hash``
  warns (doesn't block) on a mismatched hand-edit at load, and skips a hash
  recorded under another scheme (another release's recipe).
- **``stamp_provenance(tool)``** -- the one place a blueprint's provenance is
  stamped. When the recomputed ``content_hash`` equals the recorded one (never
  true for a hash of another scheme, so the first save after an upgrade stamps
  anew) and a ``generated_by`` is recorded it returns ``self`` (a matching hash alone is not
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
  place the *emitted* ``ucla_roms`` blueprint's provenance is assembled (and
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
defines the pieces the :doc:`custom-applications contract <custom_applications>`
requires:

- ``ForgeRunner(BlueprintRunner[ForgeBlueprint])`` -- ``run()`` delegates to
  ``cstar.applications.forge.runtime.process`` (host resolution) ->
  ``process_forge_blueprint`` -> ``ensure_source_data``/``generate_inputs``/
  ``configure_build``, publishes the emitted blueprint (below), then reports
  ``ExecutionStatus.COMPLETED``. Scope: generates inputs and emits the
  downstream ``ucla_roms`` blueprint (``B_{name}.yaml``), then stops -- the
  existing ``ucla_roms`` application consumes that blueprint separately.
- ``ForgeApplication`` -- ``@register_application``-decorated
  ``ApplicationDefinition`` wiring ``ForgeBlueprint`` + ``ForgeRunner``
  together under ``name = "forge"``.

Forge is discovered by ``cstar.applications.core.get_application`` as a
**built-in** application, the same way ``ucla_roms`` is: ``get_application``
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
   ``cppdefs.opt``, and the emitted downstream ``ucla_roms`` blueprint YAML
   (``B_{name}.yaml``, persisted once by ``configure_build()`` -- there is
   no per-stage blueprint file). The emitted blueprint leaves ``working_dir``
   unset, so it runs under
   ``CSTAR_DATA_HOME/blueprint_runs/ucla_roms/<name>``. Its ``description``
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

Versioned namelist schemas
--------------------------

Each ucla-roms release that changes ``namelist.nml`` gets its own schema
**tier**: a class that models exactly the keys that release reads, because an
older ROMS stops on a key it doesn't know.

- **C-Star** keeps the tiers in ``cstar/roms/namelist.py`` (``RomsNamelist`` and
  ``RomsNamelistV0_4_0`` through ``RomsNamelistV0_12_0``, each a subclass of an
  earlier one), registered in ``NAMELIST_SCHEMA_REGISTRY``.
  ``namelist_schema_for_ref`` picks the tier for a ucla-roms pin: a release tag
  selects that release; a commit hash the release it descends from (``git
  describe``); a branch, an unresolvable hash, or a release newer than
  ``NEWEST_KNOWN_UCLA_ROMS`` gets the newest tier with a warning.
- **Forge** mirrors the tiers in ``namelist_model.py`` (``RunTimeSettings`` through
  ``RunTimeSettingsV0_12_0``), selected by ``run_time_settings_for_ref``. C-Star's
  registry is the single source of the version boundaries. Forge has no ucla-roms
  clone, so ``process_forge_blueprint`` and ``configure_build`` resolve a hash pin
  to its release tag with ``release_ref_for_remote``; the resolver, called on every
  wizard keystroke, does not, and uses the newest tier.
- **Forge's setting names are stable across tiers.** Only the namelist spelling a
  field serializes to changes per release. A key that matches a setting only by
  case or by its namelist spelling (``LLm``, ``akv_bak``) is rejected rather than
  ignored. ``namelist_field_for`` is the one map from a Forge setting to its
  namelist group and key.
- **Version-gated sections** (``pio_settings``, ``cdr_lite_output``,
  ``cdr_gas_exch_output``, ``cdr_lite``) exist only in some tiers.
  ``prune_version_gated_sections`` drops one the pinned release doesn't read,
  and raises if it was switched on, since that release can't honor it.

Rules that span sections (the CDR-LiTE switches, ``bgc_mode``, CDR forcing without
MARBL, upscaled CDR) are ``check_*`` functions in ``namelist_model.py``, each called
by the resolver, ``validate_run_time_sections`` and ``configure_build``.
``bgc_mode`` (``marbl``, ``none`` or ``cdr_lite``) is stored only as the
``marbl``/``cdr_lite`` cppdefs and read back with ``bgc_mode_from_cppdefs``. In
``cdr_lite`` mode the CDR tracer counts are read from the generated CDR forcing
during input generation, not at resolve time. The output-period checks ucla-roms
runs at start-up live once, in ``cstar.roms.precheck``, and both C-Star and Forge
call them.

A namelist key from a new ucla-roms release therefore means a new tier on both
sides, not an edit to an existing one, plus a golden namelist for it (see below).

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

Golden fixtures
---------------

Committed goldens pin what Forge produces for the small ``test-tiny`` domain, and a
diff is a behavior change to justify, not noise:

- ``golden_model_settings_test-tiny*.json``: the resolved ``model_settings``.
- ``golden_namelist_test-tiny*.nml``: the rendered ``namelist.nml``, byte for
  byte, from the real ``generate_inputs()`` -> ``configure_build()`` chain with
  only the roms-tools classes mocked.

Each comes in one variant per namelist tier (``-roms050`` through ``-roms0120``)
plus a ``bgc_mode: cdr_lite`` variant, all under
``cstar/tests/unit_tests/applications/forge/fixtures/``. Regenerate one at a time
with ``UPDATE_GOLDEN=1`` and ``-k <test name>``; the run fails after writing, and a
rerun without the variable confirms it. The integration tests keep namelist goldens
of their own; see :doc:`../contributing`.
