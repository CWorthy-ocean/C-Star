.. _catalog:

The catalog
===========

The catalog holds the reusable pieces a forge blueprint is built from, and
the blueprints and workplans you save. It is a set of plain directory trees
of YAML files, so entries can be read, edited, copied and shared with
ordinary tools and kept in git.

What it contains
----------------

Specs, one directory per entry, under a directory per kind:

- ``ModelSpec/<name>/model.yaml``: a model configuration (code pins, render
  templates, default settings).
- ``DomainSpec/<name>/Domain.yaml``: a grid.
- ``ForcingSpec/<name>/Forcing.yaml``: a selection of forcing datasets.
- ``OutputSpec/<name>/Output.yaml``: output streams and frequencies.
- ``CdrSpec/<name>/...``: carbon dioxide removal forcing.

Plus ``blueprints/<application>/<name>.yaml`` for blueprints -- the directory
is the registered application name (``forge``, ``roms_marbl``, ...) and the
file name is the entry's name -- and ``workplans/`` for workplans saved from
the wizard. The directory name (for blueprints, the file name) is an entry's
name; there are no version numbers or identifiers beyond it. The older flat
``<name>.forge_blueprint.yaml`` and ``B_<name>.yaml`` files and
``blueprints/<machine>/<name>/B_*.yaml`` directories are still read for one
release, with a warning; ``cstar admin migrate-catalog <root>`` moves them
into the current layout. Each file under ``blueprints/<application>/`` is one
blueprint, so sidecar files (``settings_B_*.yaml``, ``_grid.yaml``) do not
belong there. :doc:`forge/specs` describes
what each spec kind contains.

Migrating an older catalog
--------------------------

Earlier versions kept blueprints in three other forms, which C-Star still
reads for one more release:

- ``blueprints/<name>.forge_blueprint.yaml``, a forge blueprint;
- ``blueprints/B_<name>.yaml``, a ROMS-MARBL blueprint;
- ``blueprints/<machine>/<name>/B_*.yaml``, a ROMS-MARBL blueprint in a
  per-machine directory.

Loading a catalog that holds any of them logs one warning listing them. To
move them into ``blueprints/<application>/<name>.yaml``, run:

.. code-block:: bash

   cstar admin migrate-catalog ~/cstar/catalog --dry-run   # report only
   cstar admin migrate-catalog ~/cstar/catalog

The catalog is read with validation suppressed, so an incomplete catalog can
still be migrated. Every destination ends in ``.yaml`` (a ``.yml`` source is
renamed). A blueprint whose destination already exists is left where it is
and reported, never overwritten. After a per-machine blueprint is moved, its
``<name>/`` and ``<machine>/`` directories are removed if they are empty;
whatever else lives there (a ``Build/`` directory, ``_grid.yaml``,
``settings_B_*.yaml``) is not a blueprint, so it is left in place and listed
for you to keep or delete.

Layers
------

A catalog is a stack of stores, read together and written at the top:

.. code-block:: text

   [0] your catalog     ~/cstar/catalog                     read and write
   [1] shared catalog   e.g. /project/shared/cstar-catalog  read only (optional)
   [2] bundled catalog  inside the installed package        read only

Listings show every layer, and the wizard marks entries from lower layers,
for example ``wio-toy (bundled)``. Names must be unique across the stack:
saving an edited bundled entry means saving it under a new name, so a
bundled entry is never silently shadowed by a local copy. If a name
collision does arise out of band, for example a package upgrade adds an
entry with a name you already used, the top layer wins and a warning is
logged.

Location
--------

Your catalog is ``~/cstar/catalog`` by default. It is placed in your home
directory on purpose, even on HPC systems where C-Star puts run data on
scratch: catalog entries are durable, hand-registered content that should
survive scratch purges.

The ``CSTAR_CATALOG`` environment variable replaces the stack. It is a list
of locations separated by the platform path separator (``:`` on Linux and
macOS); the first entry is the writable top layer and later entries are
read-only layers beneath it. The bundled catalog is always included at the
bottom and cannot be made the writable layer.

.. code-block:: console

   export CSTAR_CATALOG=~/cstar/catalog:/project/shared/cstar-catalog

Sharing a catalog
-----------------

A shared layer is just a directory colleagues can read, typically a git
clone. To contribute an entry, copy its directory from your catalog into a
clone of the shared repository and open a pull request. Because blueprints
record the values they were built from rather than references to the
catalog, moving or relayering catalogs never breaks an existing blueprint.

Adding your own entries
-----------------------

The simplest way is through the wizard: edit a spec in its section and use
**Save specs to catalog** in the Review section to store it under a new name.
You can also create the directory and YAML file by hand, following a bundled
entry of the same kind as a template; the catalog is rescanned when the
wizard is reloaded.

If you used the standalone ``cstar-forge`` package before, your old catalog
may still be at ``~/cstar-forge-data/catalog``. It is not read from there.
Move it to ``~/cstar/catalog``, or point ``CSTAR_CATALOG`` at it; a
one-time log message reminds you if the new location is empty and the old
one exists.
