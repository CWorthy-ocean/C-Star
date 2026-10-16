.. _catalog-design:

Catalog design notes
========================

ForgeBlueprint design rationale
-----------------------------------

Two invariants govern how the catalog can evolve:

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

.. note::

   Apart from the run manifest described under "Downstream outputs
   reporting back", nothing in this section is implemented. It records the
   plan of record for the catalog's evolution, for design discussion -- not
   the current behavior of ``cstar.catalog``.

The plan rests on one core principle and a few concrete follow-on pieces.

Files-in-git stay canonical; a database is a derived index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **System of record:** YAML files in directory trees, each tree optionally a
  git repo. This keeps human-readable diffs, PR-based contribution, no
  services required on HPC, and offline operation.
- **Query layer:** a per-user SQLite index (single file, stdlib, zero
  services) built by scanning the stores. A disposable cache -- gitignored,
  rebuildable, never authoritative; a corrupt or stale index is deleted and
  rescanned.

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

The run machinery now drops a **run manifest** (``cstar-run.yaml``) in each
run's directory, and each step process drops a **step record**
(``cstar-step.yaml``) in its working directory; the schemas are published
under ``docs/schemas/run/``. The manifest records the run's identity, one
attempt per scheduling (start time, system, package versions, status), each
step's definition hash, blueprint identity, status and record, and the run's
external dependencies, including the catalog entries and producer blueprints
found in the blueprints' provenance. The step record carries the outcome,
timestamps and, from the next release, the products with their kinds, sizes
and metadata. See :ref:`workplan_run_manifest`.

The executor still never *reads* the catalog; it only emits these
self-describing artifacts. An indexer that ingests manifests exactly like
catalog entries is still future work, and would turn "given a domain spec,
show all outputs" into a chain of edges: domain spec -> blueprints -> run
manifests -> output paths.

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

Unresolved design questions for the (also unimplemented) architecture above.

- ID scheme (UUID vs. ``name@hash``), and whether IDs get stamped
  retroactively on bundled entries at first index build.
- Should the bundled catalog eventually shrink to a pure demo set, with the
  curated catalog becoming a shared git store?
- Shared-layer ergonomics: is a plain git clone enough, or is a dedicated
  ``cstar forge catalog`` subcommand group (init/where/list/contribute)
  worth building first?
- Where the indexer that ingests run manifests lives -- it must respect the
  executor/catalog boundary either way.
