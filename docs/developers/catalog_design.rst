.. _catalog-design:

Catalog design notes
========================

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
