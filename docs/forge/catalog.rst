.. _forge-catalog:

The domain catalog
======================

Forge's domain catalog (``cstar.catalog``, backed by
``cstar.catalog.domain_catalog``) is a layered store of validated
``ModelSpec``/``DomainSpec``/``ForcingSpec``/``OutputSpec``/``CdrSpec``
content, plus saved blueprints. This page describes the current
implementation and the longer-term design it is built toward.

Catalog entries have **no id, version, or hash** -- the directory name is the
entire identity. Cross-references are bare name strings and a blueprint's
``composition`` block (``SpecRef {name, origin, modified}`` + sparse
``overrides``), provenance-only and excluded from ``content_hash``.

Crucial invariant: blueprints snapshot resolved values (see the
`ForgeBlueprint design rationale`_ below), and the executor never touches the
catalog (enforced by a boundary-guard test, ``test_forge_app_boundary.py``).
Moving or relayering the catalog cannot break existing blueprints or runs.

Topology: layered stores
----------------------------

A catalog is an ordered stack of **stores**, each a plain directory tree:

.. code-block:: text

   [0] user            ~/cstar/catalog                    read-write (all writes go here)
   [1] group shared     /shared/project/cstar-catalog       read-only via fs, contribute via git PR
   [2] bundled          cstar/catalog/bundled (in-package)  always read-only

Reads union the layers. **Collision policy (hybrid):** writers enforce
stack-wide name uniqueness -- saving an edited bundled entry means saving
under a new name, so a user can never *deliberately* shadow a bundled entry
(whose future updates would then be silently masked). Collisions that arrive
**out-of-band** (a package upgrade or shared-layer pull introduces a name the
user already used) are tolerated on read: the top layer wins deterministically,
a warning is logged, and the source badge makes the situation visible. Every
listing carries a ``source`` so the wizard can badge entries. A shared store
is just a git clone somewhere colleagues can read; "contribute" means copying
an entry from layer 0 into a clone of layer 1 and pushing/opening a PR.

Default locations and configuration
--------------------------------------

- ``user_catalog_root()`` = ``~/cstar/catalog``, deliberately **home-anchored**
  (not the ``$SCRATCH``/``$WORK``-rebased layouts C-Star uses for job-scoped
  working data elsewhere) -- durable, user-registered content must survive
  HPC scratch purges.
- ``CSTAR_CATALOG`` (C-Star's registered environment variable, see
  ``cstar.base.env``) overrides the stack: an ``os.pathsep``-separated list of
  roots, first entry = the writable top layer, ``"local"`` selects the
  bundled catalog. Setting it to just the bundled catalog (``"local"``) as
  the *first* entry is rejected -- the bundled catalog is always read-only
  and cannot be the writable top.
- A catalog from a previous standalone ``cstar-forge`` install may exist at
  the legacy location, ``~/cstar-forge-data/catalog``; it is never used
  automatically, but a one-time log message points at it if the new default
  location is empty. Move the directory (or point ``CSTAR_CATALOG`` at it) to
  pick it back up -- entries are self-contained YAML files.
- ``default_catalog`` and ``catalog.blueprint`` are lazy (PEP 562):
  ``import cstar.catalog`` does not scan the filesystem; the stack is only
  built the first time ``default_catalog`` is accessed.

Wizard integration
----------------------

Spec dropdowns badge lower-layer entries (e.g. ``wio-toy (bundled)``) via
homogeneous ``(label, value)`` option tuples; blueprint/workplan saves and
spec registrations land in the user layer (directories created on demand);
collision errors surface as plain one-line messages naming the owning layer;
the catalog bar (:doc:`internals`) accepts a pathsep-separated stack and
reports per-layer counts, with a blank value falling back to the default
stack.

ForgeBlueprint design rationale
-----------------------------------

Two invariants, set when the catalog was first designed, still govern how it
can evolve:

1. **Snapshot, don't reference.** A resolved ``ForgeBlueprint`` embeds the
   values it needs (grid kwargs, forcing selections, settings) rather than a
   live reference back to the catalog entry that produced them. This is what
   makes catalog relayering, and any future catalog schema change, safe: a
   blueprint that has already been saved keeps working even if the catalog
   entry it was built from is edited, moved, or deleted.
2. **The executor never reads the catalog.** ``ForgeExecutor`` and everything
   it calls operate purely on the resolved ``ForgeBlueprint``; only the
   resolver/wizard (the "authoring" side) ever touches catalog directories.
   A dedicated boundary-guard test enforces this by AST inspection.

Longer-term architecture
----------------------------

Beyond the layered-store slice above, the plan of record for the catalog's
evolution rests on one core principle and a few concrete follow-on pieces.

Files-in-git stay canonical; a database is a derived index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **System of record:** YAML files in directory trees, each tree optionally a
  git repo. This keeps human-readable diffs, PR-based contribution, no
  services required on HPC, and offline operation.
- **Query layer (not yet implemented):** a per-user SQLite index (single
  file, stdlib, zero services) built by scanning the stores. A disposable
  cache -- gitignored, rebuildable, never authoritative. Corrupt or stale?
  Delete and rescan.

A client-server database, or SQLite-as-canonical, is deliberately avoided:
multi-writer SQLite on Lustre/NFS is exactly where its locking breaks (a
per-user local index sidesteps that); a canonical DB would kill the
git-based contribution flow and HPC-friendliness. If scale ever demands it
(an institutional registry, a web UI), the derived-index design upgrades
cleanly by pointing the same indexer at Postgres.

Prerequisite for relationship queries: real identity and typed references
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Stable entry identity.** Give every catalog entry (and every forge
   blueprint) an ``id`` -- a creation-time UUID (or ``name@<short-hash>``)
   plus the human name. IDs are minted once and never recomputed (immune to
   content-hash churn from additive optional fields). Content hashes remain
   integrity/dedup fingerprints once canonicalized; the join key is the ID.
2. **Typed references.** Extend ``SpecRef`` to carry the source entry's
   ``id`` (and the content hash of what was snapshotted) alongside
   ``name``/``origin``/``modified``. Snapshot-don't-reference is untouched --
   the ref is provenance metadata an indexer would turn into graph edges.

With those, a SQLite index becomes trivial: ``entries(id, kind, name, store,
path, content_hash, ...)`` plus ``edges(from_id, to_id, relation)``; queries
like "given a domain spec, list every blueprint built from it" become one
query in either direction.

Downstream outputs reporting back
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The longer-term design has the executor/run machinery drop a small **run
manifest** (blueprint id + content_hash, roms_marbl blueprint path, output
URIs, machine, timestamps, status) in the run's working directory and/or a
``runs/`` area of the user's catalog store. An indexer would ingest manifests
exactly like catalog entries, so "given a domain spec, show all outputs"
becomes a chain of edges: domain spec -> blueprints -> run manifests -> output
paths. The executor still never *reads* the catalog under this design; it
only emits one more self-describing artifact.

Assets: making blueprints-with-files portable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

User-provided netCDFs (grid/river/CDR attachments) are referenced today by
**absolute host path + content hash**; nothing is written into the catalog.
An opt-in "import into catalog" that copies an attached netCDF into the
user store's assets area, keyed by content hash, would let a blueprint's
file references become catalog-asset references in addition to absolute
paths -- making blueprints-with-attachments portable across machines. Large
file hygiene in shared git stores would then need git-lfs/DataLad, or an
"assets stay out of git" policy.

Open questions
------------------

- ID scheme (UUID vs. ``name@hash``), and whether IDs get stamped
  retroactively on bundled entries at first index build.
- Should the bundled catalog eventually shrink to a pure demo set, with the
  curated catalog becoming a shared git store?
- Shared-layer ergonomics: is a plain git clone enough, or is a dedicated
  ``cstar forge catalog`` subcommand group (init/where/list/contribute)
  worth building first?
- Run-manifest schema, and where the indexer lives -- must respect the
  executor/catalog boundary either way.
