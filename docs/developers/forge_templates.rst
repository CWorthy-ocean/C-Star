.. _forge-templates:

Forge internals: render templates and pinning
=================================================

Rendering templates
-----------------------

A model specification in ``model.yaml`` references its code templates under
``code.templates_compile_time`` and ``code.templates_run_time``.

Compile-time options use a Jinja2 template, ``cppdefs.opt.j2``, which renders
the ROMS CPP defines. For example:

.. code-block:: jinja

   {% if cppdefs.cdr_forcing|default(false) %}#define CDR_FORCING
   {% else %}#undef CDR_FORCING
   {% endif %}

Run-time options are not rendered from Jinja2 templates. Instead they are
written to a single ``namelist.nml`` by ``write_roms_namelist``, which
validates the settings into ``RunTimeSettings`` and serializes via C-Star's
``cstar.roms.namelist.RomsNamelist`` (itself f90nml-backed). The ``marbl_in``
file is copied as-is.

When Forge configures and builds the model for a new domain,
``render_roms_settings`` (in ``cstar/applications/forge/settings.py``) uses
the ``jinja2`` templating engine to replace keys in ``cppdefs.opt.j2`` with
values from the resolved ``model_settings`` dict (the same dict that ends up
on ``ForgeBlueprint.model_settings``).

Pinning and content hashes
------------------------------

Render templates are pinned to a commit of the template repository
(``code.templates_commit``, defaulting to branch ``main`` when omitted) and,
per file, to the sha256 of its content at that commit (``file_hashes``,
authored in the bundled ModelSpecs). ``directory`` (under
``code.templates_compile_time``/``code.templates_run_time``) is written
relative to that template repository's root. For example:

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

Each stage's ``file_hashes`` maps filename to the sha256 of that file's
content at ``templates_commit``. Regenerate a hash after editing a pinned
template with:

.. code-block:: console

   git show <commit>:<directory>/<file> | shasum -a 256

Staging: bundled copy vs. fetching the pinned commit
--------------------------------------------------------

``cstar/applications/forge/templates.py`` owns the single mapping from a
ModelSpec's ``templates/<stage>`` ``directory`` onto the copy of the same
templates bundled in this C-Star build
(``cstar/additional_files/templates/forge/<stage>``), plus content hashing of
that bundled copy. It is used by both consumers that must never duplicate
this mapping: ``models.py`` (a dev-time existence check) and ``executor.py``
(the staging logic described below).

At ``configure_build``, ``ForgeExecutor._stage_templates(stage)`` picks one
of two paths:

1. **Local fast path.** If the ModelSpec's ``templates_{stage}.file_hashes``
   is non-empty and the bundled copy
   (``cstar.applications.forge.templates.bundled_template_dir``) has every
   listed file with a matching sha256, the files are copied flat from the
   bundled copy straight into the run's working directory -- no git fetch.
   This only fires when the bundled templates genuinely are the pinned
   commit's files; a ModelSpec pinned at an older or different commit than
   what is currently bundled falls through to fetching instead of silently
   substituting newer bundled content.
2. **Fetch via** ``AdditionalCode``: a remote repository (location + commit
   or branch) is fetched, or a local directory is copied, exactly as before
   the fast path existed. ``_verify_template_hashes`` then checks the fetch
   against ``file_hashes`` -- a no-op when empty, which covers old blueprints
   and ModelSpecs that have not authored hashes yet. A mismatch between a
   fetched file's sha256 and its pinned hash raises ``ValueError``: the
   blueprint pins template content that the fetched commit does not match.

Bundled ModelSpecs still pin their ``templates_commit`` against the archived
``cstar-forge`` repository (``resolve.DEFAULT_TEMPLATE_REPO``); the local
fast path above is what lets most builds avoid fetching from it at all.
