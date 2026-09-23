.. _forge-specs:

Specs
=====

A forge blueprint is composed from **specs**: named, reusable catalog
entries, each covering one aspect of a domain. The wizard presents them as
dropdowns, the resolver merges the chosen ones (plus your edits) into a
blueprint, and the blueprint records the resulting values in full. Editing a
spec afterwards does not change blueprints already built from it.

Specs live in the :doc:`catalog <../catalog>` as one directory per entry,
``<Kind>/<name>/<file>.yaml``. C-Star bundles a small set of each kind to
start from; save your own from the wizard's Review section or copy a bundled
directory and edit it.

Kinds
-----

ModelSpec (``ModelSpec/<name>/model.yaml``)
   A trusted model configuration: the pinned ROMS, MARBL and PIO code, the
   render templates, the default biogeochemistry and PIO modes, and the
   model-level default settings. Choosing a model preset in the wizard
   selects one of these. Detailed below.

DomainSpec (``DomainSpec/<name>/Domain.yaml``)
   A grid: its horizontal extent, resolution and rotation (``grid_kwargs``),
   number of vertical levels, which boundaries are open, the processor
   layout, and a default run window. The bundled ``wio-toy`` domain is:

   .. literalinclude:: ../../cstar/catalog/bundled/DomainSpec/wio-toy/Domain.yaml
      :language: yaml

ForcingSpec (``ForcingSpec/<name>/Forcing.yaml``)
   Which datasets supply the initial conditions and the surface, boundary,
   tidal and river forcing, including biogeochemical sources and the
   interpolation method for each. The bundled entries pair GLORYS and ERA5
   with different biogeochemical sources (unified climatology, GLODAP and
   WOA, or ESPER). See :doc:`source_datasets` for the sources.

OutputSpec (``OutputSpec/<name>/Output.yaml``)
   Which fields the model writes, how often, and how files are packed, per
   output stream, plus the restart frequency. The bundled ``standard``,
   ``daily-restarts``, ``weekly-restarts`` and ``monthly-restarts`` entries
   differ in restart cadence, with every stream chosen so restart boundaries
   never truncate a partially written file.

CdrSpec
   Optional carbon dioxide removal forcing. Nothing is bundled; the wizard's
   Run setup section creates one from a netCDF file or by hand.

Model specs in detail
---------------------

A ``model.yaml`` has this shape:

.. code-block:: yaml

   bgc_mode: marbl   # marbl or none; the default for the wizard's biogeochemistry choice
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
     templates_commit: <commit>
     templates_compile_time:
       directory: templates/compile-time
       files: [cppdefs.opt.j2]
       file_hashes: {cppdefs.opt.j2: <sha256>}
     templates_run_time:
       directory: templates/run-time
       files: [marbl_in]
       file_hashes: {marbl_in: <sha256>}

   model_settings:
     cppdefs:
       sponge_tune: false
       nhy_forcing: true
       nox_forcing: true
     # one section per namelist group whose defaults are model-level

``bgc_mode`` and ``use_pio``
   Build modes rather than settings. They prepopulate the wizard, and the
   resolver derives the corresponding compile-time switches from them and
   decides whether the MARBL and PIO code are pulled in. Requesting PIO from
   a model with no ``code.pio`` pin is an error.

``code``
   The repositories to build, each a ``location`` with a ``commit`` or
   ``branch``, and the render templates: the compile-time template that
   becomes ``cppdefs.opt`` and the run-time files copied alongside the
   namelist. Templates are pinned to a commit and, per file, to a checksum of
   its content, so a model spec always renders with the templates it was
   validated against even as C-Star's bundled copy moves on. Details are in
   :doc:`../developers/forge_templates`.

``model_settings``
   The model-level defaults, one top-level key per namelist section or
   scalar (``cppdefs``, ``param``, ``tides``, ``marbl_bgc``, ...). This is the
   same structure as ``model_settings`` in the blueprint. Every compile-time
   template listed must have a matching section here; a ``cppdefs.opt.j2``
   template requires a ``cppdefs`` section, and a model spec that lacks it
   fails to load. Values that depend on the domain or forcing (open-boundary
   switches, the MARBL and tides switches, the processor grid) are set by the
   resolver and are left out of the spec; only genuinely model-level
   defaults belong here.

Settings that a newer ROMS knows and an older one does not are declared only
by the specs for that ROMS version; the template renders them off when a
spec omits them, so older specs are unaffected by additions.

The bundled model specs
~~~~~~~~~~~~~~~~~~~~~~~

``roms-marbl-0.8-default`` (ucla-roms 0.8.0) is the wizard's default. Older
``roms-marbl-0.N-default`` entries pin earlier releases, ``pio-dev`` tracks
ucla-roms ``main`` with PIO enabled, and ``cson_roms-marbl_v0.1`` is the
configuration the toy domain and the example blueprint use.

.. literalinclude:: ../../cstar/catalog/bundled/ModelSpec/roms-marbl-0.8-default/model.yaml
   :language: yaml
   :lines: 1-36

Working with model specs in Python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`~cstar.applications.forge.models.ModelSpec` loads a ``model.yaml``
into a validated object; see :doc:`../api-forge`.
