.. _forge-specs:

Specs
=====

A forge blueprint is composed from **specs**: named, reusable catalog
entries, each covering one aspect of a domain. Specs are intended to provide
modular, shareable building blocks for the different aspects of a complete
ocean model setup, so that you don't have to remake every individual choice
whenever you start a new model.

The wizard presents them as dropdowns, the resolver merges the chosen ones
(plus your edits) into a blueprint, and the blueprint records the resulting
values in full. Editing a spec afterwards does not change blueprints already
built from it.

Specs live in the :doc:`catalog <../../catalog_wizard/catalog>` as one file per entry,
``<Kind>/<name>.yaml``. C-Star bundles a small set of each kind to start
from; save your own from the wizard's Review section or copy a bundled file
and edit it.

Types of specs
--------------

ModelSpec (``ModelSpec/<name>.yaml``)
   A trusted model configuration: the pinned ROMS, MARBL and PIO code,
   the default biogeochemistry and PIO modes, and the
   model-level default settings. Choosing a model preset in the wizard
   selects one of these. Detailed below.

DomainSpec (``DomainSpec/<name>.yaml``)
   A grid: its horizontal extent, resolution and rotation (``grid_kwargs``),
   number of vertical levels, which boundaries are open, the processor
   layout, and a default run window. The bundled ``wio-toy`` domain is:

   .. literalinclude:: ../../../cstar/catalog/bundled/DomainSpec/wio-toy.yaml
      :language: yaml

ForcingSpec (``ForcingSpec/<name>.yaml``)
   Which datasets supply the initial conditions and the surface, boundary,
   tidal and river forcing, including biogeochemical sources and the
   interpolation method for each. The bundled entries pair GLORYS and ERA5
   with different biogeochemical sources (unified climatology, GLODAP and
   WOA, or ESPER); ``glorys-era5-physics`` carries no biogeochemical source,
   for ``bgc_mode`` ``none`` and ``cdr_lite``. See :doc:`source_datasets` for
   the sources.

OutputSpec (``OutputSpec/<name>.yaml``)
   Which fields the model writes, how often, and how files are packed, per
   output stream, plus the restart frequency. The bundled ``standard``,
   ``daily-restarts``, ``weekly-restarts`` and ``monthly-restarts`` entries
   differ in restart cadence, with every stream chosen so restart boundaries
   never truncate a partially written file.

CdrSpec (``CdrSpec/<name>.yaml``)
   Optional carbon dioxide removal forcing. Nothing is bundled; the wizard's
   Run setup section creates one from a netCDF file or by hand.

Model specs in detail
---------------------

A ModelSpec file has this shape:

.. code-block:: yaml

   bgc_mode: marbl   # marbl, none or cdr_lite; the default for the wizard's biogeochemistry choice
   use_pio: false    # default for the wizard's ParallelIO checkbox

   code:
     roms:
       location: https://github.com/CWorthy-ocean/ucla-roms.git
       commit: 0.8.0            # or branch: main
     marbl:                     # optional; needed when bgc_mode can be marbl
       location: https://github.com/CWorthy-ocean/MARBL.git
       commit: marbl0.45.0-max-it-10
     pio:                       # optional; needed when use_pio can be true
       location: https://github.com/CWorthy-ocean/ParallelIO.git
       commit: 2.7.1-fork
     templates_commit: <C-Star commit>
     templates_compile_time:
       directory: cstar/additional_files/templates/forge/compile-time
       files: [cppdefs.opt.j2]
       file_hashes: {cppdefs.opt.j2: <sha256>}
     templates_run_time:
       directory: cstar/additional_files/templates/forge/run-time
       files: [marbl_in]
       file_hashes: {marbl_in: <sha256>}

   model_settings:
     cppdefs:
       sponge_tune: false
       nhy_forcing: true
       nox_forcing: true
     # one section per namelist group whose defaults are model-level

``bgc_mode`` and ``use_pio``
   Build modes rather than settings. They prepopulate the wizard, and Forge
   derives the matching compile-time switches from them and decides whether
   the MARBL and PIO code are pulled in, so PIO needs a ``code.pio`` pin.
   ``bgc_mode: cdr_lite`` builds ucla-roms' dedicated CDR-lite tracers
   without MARBL: it needs ucla-roms 0.10.0 or later and a CDR forcing, and
   takes no biogeochemical forcing.

``archived``
   Optional, default ``false``. An archived spec stays fully usable: blueprints
   that name it load and run unchanged, and it appears in catalog listings.
   The only effect is that the wizard's model dropdown hides it unless
   "Show archived" is checked, so a catalog can retire a spec from day-to-day
   choice without breaking blueprints that compose it. A spec saved from the
   wizard is not archived, even when its base was.

``code``
   The repositories to build, each a ``location`` with a ``commit`` or
   ``branch``, and the render templates: the compile-time template that
   becomes ``cppdefs.opt`` and the run-time files copied alongside the
   namelist. Templates are pinned to a commit and, per file, to a checksum of
   its content, so a model spec always renders with the templates it was
   validated against even as C-Star's bundled copy moves on. Details are in
   :doc:`../../developers/forge_templates`.

   The ucla-roms pin decides which namelist Forge writes. A release tag
   (``commit: 0.12.0``) selects that release's namelist exactly; a commit hash
   selects the release it descends from; a branch gets the newest namelist
   C-Star has, with a warning. A release newer than the newest one your C-Star
   knows (``cstar --version`` prints it) also gets the newest namelist C-Star
   has, with a warning naming both releases: if that release changed the
   namelist, ROMS may stop on the generated file or run on the default of a
   key it lacks. Upgrade C-Star before adopting a newer ucla-roms release.

``model_settings``
   The model-level defaults, one top-level key per namelist section or
   scalar (``cppdefs``, ``param``, ``tides``, ``marbl_bgc``, ...). This is the
   same structure as ``model_settings`` in the blueprint. Every compile-time
   template listed must have a matching section here; a ``cppdefs.opt.j2``
   template requires a ``cppdefs`` section, and a model spec that lacks it
   fails to load. Values that depend on the domain or forcing (open-boundary
   switches, the MARBL and tides switches, the processor grid) are set by the
   resolver and are left out of the spec; only genuinely model-level
   defaults belong here. Keys are Forge's lowercase setting names (``llm``,
   ``np_xi``, ``wrt_z``), not the ROMS namelist spelling (``LLm``,
   ``NP_XI``, ``wrt_Z``).

Settings that a newer ROMS knows and an older one does not are declared only
by the specs for that ROMS version; the template renders them off when a
spec omits them, so older specs are unaffected by additions.

The bundled model specs
~~~~~~~~~~~~~~~~~~~~~~~

``roms-marbl-0.12-default`` (ucla-roms 0.12.0) is the wizard's default. Older
``roms-marbl-0.N-default`` entries pin earlier releases (``roms-marbl-0.11-default``
stays for ucla-roms 0.11.0, which predates the
``cdr_lite.cdr_carbonate_sensitivity_time_interpolation`` switch;
``roms-marbl-0.10-default`` stays for ucla-roms 0.10.1, which predates the
constant-iron option;
``roms-marbl-0.9-default`` stays for ucla-roms 0.9.1, which predates the
``cdr_lite`` mode's minimum), and ``pio-dev`` tracks
ucla-roms ``main`` with PIO enabled. The bundled domains and the example
blueprint use ``roms-marbl-0.12-default``.

``roms-marbl-0.3-default`` through ``roms-marbl-0.8-default`` and
``cson_roms-marbl_v0.1`` are marked ``archived: true``. They remain
loadable, and blueprints that name them keep working, but the wizard's model
dropdown lists them only when "Show archived" is checked (loading a blueprint
that uses one checks the box for you). ``archived`` works the same way in a
spec of your own catalog.

.. literalinclude:: ../../../cstar/catalog/bundled/ModelSpec/roms-marbl-0.12-default.yaml
   :language: yaml
   :lines: 1-35

To load a ModelSpec file into a validated object from Python, see
:doc:`../../reference/python`.
