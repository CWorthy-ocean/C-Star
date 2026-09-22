.. _forge-model-spec:

Model specification
======================

The ``ModelSpec`` abstraction is designed to formalize and preserve a notion
of a trusted model configuration by aggregating the information required to
build and configure a particular model as a named entity.

Model specifications are defined per-model in
``cstar/catalog/bundled/ModelSpec/<model>/model.yaml`` (see :doc:`reference`).
Models are discovered by scanning ``catalog/ModelSpec/*/model.yaml``.

Each model includes:

- Code repository configurations (ROMS, MARBL, PIO) and template refs
  (compile-time and run-time)
- Per-run build-mode toggles (``bgc_mode``, ``use_pio``)
- Model-specific physics/numerics settings defaults (``model_settings``)

Everything a Domain/Forcing/Output spec already owns (grid/IC/forcing source
selection, output write-lists, open-boundary and tidal/river presence, grid
partitioning, etc.) is deliberately *not* duplicated here -- those values
come from the selected ``DomainSpec``/``ForcingSpec``/``OutputSpec`` catalog
entries (directories read as plain dicts; of the specs, only ``ModelSpec`` is
also a Python class) and are merged in by the resolver
(``build_forge_blueprint``) when it assembles a ``ForgeBlueprint``.

``model.yaml`` schema
------------------------

Here's a view of the schema:

.. code-block:: yaml

   bgc_mode: marbl  # marbl|none -- prepopulates the wizard; resolver derives cppdefs.marbl from it
   use_pio: false  # prepopulates the wizard's PIO checkbox; resolver derives cppdefs.use_pio from it

   code:
     roms:
       location: https://github.com/org/repo.git
       commit: <hash>  # or 'branch: main' instead

     marbl:  # optional
       location: https://github.com/CWorthy-ocean/MARBL.git
       commit: marbl0.45.0-max-it-10

     pio:  # optional; required if use_pio can be set true
       location: https://github.com/CWorthy-ocean/ParallelIO.git
       commit: 2.7.1-fork

     # Render templates are fetched from the standalone cstar-forge GitHub repo
     # (at its repo root `templates/`), decoupled from this ModelSpec.
     # `templates_commit` pins the commit they're fetched from (defaults to
     # branch `main` if omitted); `directory` is relative to that repo root.
     templates_commit: <cstar-forge-commit-sha>
     templates_compile_time:
       directory: "templates/compile-time"
       files:
         - cppdefs.opt.j2
     templates_run_time:
       directory: "templates/run-time"
       files:
         - marbl_in

   model_settings:
     cppdefs:
       sponge_tune: false
       nhy_forcing: true
       nox_forcing: true
     # ...one section per model_settings namelist key (lateral_visc, vertical_mixing,
     # tracer_diff2, bottom_drag, param, bgc, blk_frc, tides, marbl_bgc, etc.)

Field descriptions
~~~~~~~~~~~~~~~~~~~~

``bgc_mode``
    Per-run BGC toggle (``marbl`` or ``none``). Prepopulates the wizard's BGC
    dropdown; the resolver uses it to derive ``model_settings.cppdefs.marbl``
    (and gate ``nhy_forcing``/``nox_forcing``) and to decide whether
    ``code.marbl`` is populated. Not itself part of ``model_settings`` -- it's
    a build mode, not a namelist section.

``use_pio``
    Per-run ParallelIO (PIO) build toggle. Prepopulates the wizard's PIO
    checkbox; the resolver uses it to derive ``model_settings.cppdefs.use_pio``
    and to decide whether ``code.pio`` is populated (raising if PIO is
    requested but the model has no ``code.pio`` pin).

``code``
    Code repository and template specifications:

    - ``roms``: ROMS source code repository (required; specify ``location``
      and ``branch`` or ``commit``)
    - ``marbl``: MARBL source code repository (optional; specify ``location``
      and ``branch`` or ``commit``)
    - ``pio``: ParallelIO source code repository (optional; specify
      ``location`` and ``branch`` or ``commit``)
    - ``templates_commit``: the ``cstar-forge`` GitHub repo commit that
      ``templates_compile_time``/``templates_run_time`` are fetched from
      (defaults to branch ``main`` when omitted). Render templates are not
      yet part of this bundled catalog -- they are still fetched by cloning
      the standalone ``cstar-forge`` repo at its ``templates/`` root (see
      :doc:`internals`); a copy is also bundled locally at
      ``cstar/additional_files/templates/forge/`` for other consumers, but
      this resolution path does not yet read from it.
    - ``templates_compile_time`` / ``templates_run_time``: each a
      ``directory`` (relative to the ``cstar-forge`` repo root) plus a
      ``files`` list. ``*.j2`` files have Jinja2 templating applied; files
      without that extension (e.g. ``marbl_in``) are copied as-is.

``model_settings``
    A flat dict of model-specific physics/numerics defaults, mirroring
    ``ForgeBlueprint.model_settings`` 1:1 (each top-level key is a namelist
    section or a scalar namelist value, e.g. ``cppdefs``, ``param``,
    ``tides``, ``marbl_bgc``; ``gamma2``, ``ubind``). Every compile-time
    ``.j2`` template file listed under ``code.templates_compile_time.files``
    must have a corresponding top-level key here (e.g. ``cppdefs.opt.j2``
    requires a ``cppdefs:`` section) -- this is enforced by a ``ModelSpec``
    validator. Many fields within these sections are still overwritten by
    the resolver at build time from Domain/Forcing/Output selections (e.g.
    ``param``'s grid-partitioning fields, ``cppdefs.obc_*``/``marbl``/
    ``tides``, ``tides.ntides``); they're included in ``model.yaml`` only
    where the *other* fields in that same section are real, model-level
    defaults.

You can add new models by creating a new directory under
``cstar/catalog/bundled/ModelSpec/<model>/`` containing a ``model.yaml``
with the schema above.

Settings
-----------

Forge curates default settings for each model configuration. These defaults
are used in the templating engine to generate source code and input files
with the correct parameters.

Settings are managed using:

1. Templated code files
2. A ``model_settings`` dict specifying defaults, consolidated into each
   model's ``model.yaml`` (see :doc:`reference`)
3. User override settings, merged in by the resolver
   (``build_forge_blueprint``)

Templates
~~~~~~~~~~~

A model specification in ``model.yaml`` references its code templates under
``code.templates_compile_time`` and ``code.templates_run_time``. ``directory``
is relative to the standalone ``cstar-forge`` GitHub repo root (these
templates still live at ``templates/`` in that repo, decoupled from any one
``ModelSpec``; a copy is also bundled locally at
``cstar/additional_files/templates/forge/``, not yet wired into this
resolution path -- see :doc:`internals`); ``code.templates_commit`` pins the
commit they're fetched from (defaults to branch ``main`` if omitted). For
example:

.. code-block:: yaml

   code:
     templates_commit: 692e04cebf1735951b377fcf44b1bde59a06bbc9
     templates_compile_time:
       directory: "templates/compile-time"
       files:
       - cppdefs.opt.j2
     templates_run_time:
       directory: "templates/run-time"
       files:
       - marbl_in

Compile-time options still use a Jinja2 template, ``cppdefs.opt.j2``, which
renders the ROMS CPP defines. For example:

.. code-block:: jinja

   {% if cppdefs.cdr_forcing|default(false) %}#define CDR_FORCING
   {% else %}#undef CDR_FORCING
   {% endif %}

Run-time options are no longer rendered from Jinja2 templates. Instead they
are written to a single ``namelist.nml`` by ``write_roms_namelist``, which
validates the settings into ``RunTimeSettings`` and serializes via C-Star's
``cstar.roms.namelist.RomsNamelist`` (itself f90nml-backed). The
``marbl_in`` file is copied as-is.

When Forge configures and builds the model for a new domain,
``render_roms_settings`` (in ``cstar/applications/forge/settings.py``) uses
the ``jinja2`` templating engine to replace keys in ``cppdefs.opt.j2`` with
values from the resolved ``model_settings`` dict (the same dict that ends up
on ``ForgeBlueprint.model_settings``).

Defaults
~~~~~~~~~~

The ``model_settings`` dict is initialized from the defaults curated
directly in each model's ``model.yaml`` -- there are no separate
``compile-time-defaults.yaml``/``run-time-defaults.yaml`` files; everything
is consolidated into one ``model.yaml`` per model.

For example, ``catalog/ModelSpec/cson_roms-marbl_v0.1/model.yaml`` includes a
``model_settings.cppdefs`` section with model-level defaults not otherwise
derived by the resolver:

.. code-block:: yaml

   model_settings:
     cppdefs:
       sponge_tune: false
       nhy_forcing: true
       nox_forcing: true

``parabolic_splines``/``upstream_ts_land_curv`` (ucla-roms >= 0.8.0, PR
#361) are the same kind of model-level default, but only declared by specs
whose ucla-roms version knows them (``roms-marbl-0.8-default``, ``pio-dev``);
the template renders ``#undef`` for either key when a spec omits it, so
older specs are unaffected.

``cppdefs.obc_*``/``marbl``/``co2_tvarying``/``sal_restore``/``tides``/
``cdr_forcing``/``use_pio`` are resolver-derived from the Domain/Forcing
selection and the model's ``bgc_mode``/``use_pio`` toggles, so they're
intentionally absent from ``model.yaml``. ``cdr_forcing`` is also raised to
true whenever CDR output is enabled -- ``cdr_output.do_cdr_output`` is
user-controllable and does not require an actual CDR forcing. The same
applies to ``cdr_tracer_output.do_cdr_tracer_output`` and
``cdr_gas_exch_output.do_cdr_gas_exch_output`` (ucla-roms >= 0.7.0, PR
#351): enabling either also raises ``cdr_forcing`` to true and additionally
requires ``bgc_mode == "marbl"`` -- the resolver raises a ``ValueError``
otherwise, since ucla-roms only compiles those output modules under ``MARBL
&& CDR_FORCING``. Unlike ``cdr_output``, neither is forced on by an active
CDR forcing mode; both stay off until a user explicitly enables them.

User override
~~~~~~~~~~~~~~~

User additions are permitted when building model domains. A user can pass
parameter values to ``build_forge_blueprint`` (the resolver) to override the
model's defaults, e.g.:

.. code-block:: python

   build_forge_blueprint(
       ...,
       compile_time_overrides={"cppdefs": {"sponge_tune": True}},
   )

The settings actually used are saved with the model's ``ForgeBlueprint``
(``model_settings``), not just the ``ModelSpec`` defaults.

Example notebook
-------------------

:doc:`model_spec_example` walks through loading a ``ModelSpec`` from the
catalog and inspecting its fields interactively.

.. toctree::
   :maxdepth: 1
   :hidden:

   model_spec_example
