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
     templates_commit: 6a0a4ee55b944db316e33f58ba7aa2403e880b92  # C-Star 0.15.0
     templates_compile_time:
       directory: cstar/additional_files/templates/forge/compile-time
       files:
       - cppdefs.opt.j2
     templates_run_time:
       directory: cstar/additional_files/templates/forge/run-time
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

The template repository is this one (``resolve.DEFAULT_TEMPLATE_REPO``), and every
bundled ModelSpec pins ``templates_commit`` to a C-Star release commit whose
templates are the bundled copy, so a release build never fetches. Blueprints
written against the standalone cstar-forge repository carry the legacy
``templates/<stage>`` directory form; ``bundled_template_dir`` maps both forms
onto the bundled copy, and such a blueprint still fetches its pinned forge
commit when the hashes differ.

Staging cache
~~~~~~~~~~~~~~~~

A commit pin (as opposed to a ``branch`` pin) with authored ``file_hashes`` is
content-addressed, so a fetch that verifies successfully is cached under
C-Star's cache home (``cstar.execution.file_system.DirectoryManager.cache_home``,
i.e. ``CSTAR_CACHE_HOME`` / ``XDG_CACHE_HOME``) at a key derived from the pin's
``location``, ``commit``, and ``directory``. A later run for the same pin copies
from that cache instead of fetching again; a cache entry whose files no longer
match ``file_hashes`` -- corrupted, or left partial by an interrupted earlier
run -- is treated as a miss and re-fetched. Branch pins and blueprints without
``file_hashes`` are never cached, since neither is content-addressed enough to
trust a cache entry without re-fetching to check it.

Each file is written into the cache via a temp-file-then-rename, so two runs
staging the same pin at once never see a partially-written file -- worst case
they both write the same, already-verified bytes. Failing to write the cache
(a read-only or over-quota ``CSTAR_CACHE_HOME``, common on shared HPC
filesystems) is logged and otherwise ignored: the run already has its verified
templates in the working directory, and simply re-fetches next time.
