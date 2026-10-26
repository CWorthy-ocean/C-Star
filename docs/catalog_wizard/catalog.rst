.. _catalog:

The catalog
===========

The catalog holds the reusable pieces a forge blueprint is built from, and
the blueprints and workplans you save. It is a set of plain directory trees
of YAML files, so entries can be read, edited, copied and shared with
ordinary tools and kept in git.

What it contains
----------------

Specs, one YAML file per entry, under a directory per kind:

- ``ModelSpec/<name>.yaml``: a model configuration (code pins, render
  templates, default settings).
- ``DomainSpec/<name>.yaml``: a grid.
- ``ForcingSpec/<name>.yaml``: a selection of forcing datasets.
- ``OutputSpec/<name>.yaml``: output streams and frequencies.
- ``CdrSpec/<name>.yaml``: carbon dioxide removal forcing.

Plus ``blueprints/<application>/<name>.yaml`` for blueprints -- the directory
is the registered application name (``forge``, ``ucla_roms``, ...) -- and
``workplans/`` for workplans saved from the wizard. The file name is an
entry's name; there are no version numbers or identifiers beyond it. Each
file under ``blueprints/<application>/`` is one blueprint, so sidecar files
(``settings_B_*.yaml``, ``_grid.yaml``) do not belong there. :doc:`../applications/forge/specs`
describes what each spec kind contains.

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
clone. To contribute an entry, copy its file from your catalog into a
clone of the shared repository and open a pull request. Because blueprints
record the values they were built from rather than references to the
catalog, moving or relayering catalogs never breaks an existing blueprint.

Adding your own entries
-----------------------

The simplest way is through the wizard: edit a spec in its section and use
**Save specs to catalog** in the Review section to store it under a new name.
You can also create the YAML file by hand, following a bundled entry of the
same kind as a template; the catalog is rescanned when the wizard is
reloaded.

.. _catalog-migrating:

Migrating an older catalog
--------------------------

Catalogs written by earlier versions of C-Star kept entries in other
places: each spec in a directory of its own (``ModelSpec/<name>/model.yaml``,
``DomainSpec/<name>/Domain.yaml`` with an ``Assets/`` directory beside it,
...), and blueprints as flat ``<name>.forge_blueprint.yaml`` and
``B_<name>.yaml`` files, per-machine ``blueprints/<machine>/<name>/``
directories, or a ``blueprints/roms_marbl/`` directory. C-Star reads them,
with a warning, until 0.20.0, which refuses such a catalog until it is
migrated. To move them into the current layout, run:

.. code-block:: bash

   cstar admin migrate-catalog ~/cstar/catalog --dry-run   # report only
   cstar admin migrate-catalog ~/cstar/catalog

Nothing is overwritten: an entry whose new location is already taken is
left where it is and reported, and so is anything that is not a spec or a
blueprint, such as a file you kept in a domain's ``Assets/`` directory.

If you used the standalone ``cstar-forge`` package, your old catalog may be
at ``~/cstar-forge-data/catalog``, which C-Star does not read. Move it to
``~/cstar/catalog``, or point ``CSTAR_CATALOG`` at it.
