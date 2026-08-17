# Catalog: architecture plan of record

*Plan of record for the catalog's evolution. The short-term slice (§5) was **implemented
2026-08-14** (layered stores, branch `catalog`); §4 is the design it builds toward and §6 tracks
open questions for the next phase. §1–§2 are kept as the state-of-the-world analysis that
motivated the design (references are to `main @ c36ed09f`, pre-implementation).*

---

## 1. State before the layered-store work (main @ c36ed09f)

**Location & resolution.** The packaged catalog was `cstar_forge/catalog/`, resolved as
`Path(__file__).parent / "catalog"` (`domain_catalog.py:19`), so a conda/pip install put it in
`site-packages/cstar_forge/catalog/`. A module-level singleton `default_catalog = DomainCatalog()`
was constructed **at import time**. Users could point elsewhere only via the Python API
(`DomainCatalog(catalog_root=...)`) or the wizard's Catalog text box — no env var, no CLI flag.

**The site-packages write problem was threefold:**
1. Wizard default blueprint save path targeted `<packaged catalog>/blueprints/`.
2. Workplans targeted `<packaged catalog>/workplans/`.
3. "Save modified pieces to catalog" (`register_output`, `register_model_from_settings`,
   `register_domain_from_dict`, `register_forcing`) wrote directly into the packaged spec dirs —
   with no guard for non-local catalogs (GitHub-backed catalogs silently wrote under CWD).

**Half-built pieces that informed the design:** a vestigial `config.paths.catalog` already
defaulted to `~/cstar-forge-data/catalog` (PR #100) but had no consumers;
`DomainCatalog(initialize_catalog_from=...)` already implemented catalog seeding; read-only
**remote catalogs over fsspec** (GitHub/http, `GITHUB_TOKEN`) already worked.

**Identity & relationships (still true today):** catalog entries have **no id, version, or hash**
— the directory name is the entire identity. Cross-references are bare name strings and the
blueprint's `composition` block (`PieceRef {name, origin, modified}` + sparse `overrides`),
provenance-only and excluded from `content_hash`. Blueprint→(model, grid) attribution in
`roms_marbl_blueprint_df` is recovered by **filename regex convention**. No index file.

**Crucial invariant that made relocation safe:** blueprints snapshot resolved values
(DESIGN-RATIONALE rule 2, "snapshot, don't reference"), and the executor never touches the catalog
(enforced by `tests/test_forge_app_boundary.py`). Moving the catalog cannot break existing
blueprints or runs.

## 2. Interaction with user-provided netCDFs (merged as #121)

- Attached netCDFs (grid/river/CDR) are referenced by **absolute host path + content hash**
  (`UserProvidedFile{location, content_hash}`); nothing is written into the catalog. Catalog
  relocation doesn't interact with that feature. The fragility it introduces is *transport*
  (blueprints with attached files aren't self-contained across machines) — a future
  catalog-assets story could fix that, see §4.5.
- Design lesson that matters for §4: `content_hash()` hashes `model_dump(mode="json")` **without
  `exclude_none`**, so every additive optional field churns the stored hash of every shipped
  blueprint. If content hashes are ever used as **join keys** for relationship/output tracking,
  this instability must be fixed first (canonicalize by dropping `None` leaves before hashing, or
  introduce stable creation-time IDs — §4.3 chooses the latter).

## 3. Requirements

Short-term (**done**, §5): user-created blueprints/specs land somewhere durable and findable,
surviving package upgrades and env rebuilds, while shipped examples remain browsable.

Long-term:
- R1. Per-user "scratch" catalog for personal specs/blueprints. (**Done** — the user layer.)
- R2. Contribution of selected items to a shared/group or source catalog (collaboration).
- R3. Relationship queries in both directions (blueprints ⇄ modelspec/domainspec).
- R4. Downstream C-Star processes report back: given a domainspec, list all roms_marbl *outputs*.
- R5. HPC-friendly: no long-running database service; scientist-friendly: git > DB admin.

## 4. Long-term architecture (next phases)

### 4.1 Core principle: files-in-git stay canonical; a database is a *derived index*

- **System of record:** YAML files in directory trees, each tree optionally a git repo. This keeps
  human-readable diffs, PR-based contribution (which *is* the R2 workflow), no services on HPC,
  offline operation.
- **Query layer:** a per-user **SQLite index** (single file, stdlib, zero services) built by
  scanning the stores. A **disposable cache** — gitignored, rebuildable, never authoritative. All
  R3/R4 queries run against it. Corrupt or stale? Delete and rescan.

Why not a client-server DB or SQLite-as-canonical: multi-writer SQLite on Lustre/NFS is exactly
where its locking breaks (a per-user local index sidesteps that); a canonical DB kills the git
contribution flow (R2) and HPC-friendliness (R5). If scale ever demands it (institutional
registry, web UI), the derived-index design upgrades cleanly: point the same indexer at Postgres.

Prior art to borrow from, not adopt wholesale: **intake/intake-esm** (files + derived index),
**conda channels** (precedence-ordered layered sources), **STAC** (static catalogs with typed
links), **DataLad/git-annex** (only if large binary assets move into catalogs — §4.5).

### 4.2 Topology: layered stores *(implemented — see §5)*

A catalog is an ordered stack of **stores**, each a plain directory tree:

```
[0] user scratch      ~/cstar-forge-data/catalog          read-write (all writes go here)
[1] group shared      /shared/project/cstar-catalog       read-only via fs, contribute via git PR
[2] packaged/bundled  site-packages/cstar_forge/catalog   always read-only
```

Reads union the layers. **Collision policy (hybrid, decided 2026-08-14):** writers enforce
stack-wide name uniqueness — saving an edited bundled entry means saving under a new name, so a
user can never *deliberately* shadow a bundled entry (whose future updates would then be silently
masked). Collisions that arrive **out-of-band** (a package upgrade or shared-layer pull introduces
a name the user already used) are tolerated on read: the top layer wins deterministically, a
warning is logged, and the source badge makes the situation visible. Every listing carries a
`source` so the wizard can badge entries. A shared store is just a git clone somewhere colleagues
can read; "contribute" = copy an entry from layer 0 into a clone of layer 1 and push/PR. Later, a
`cstar forge catalog contribute <kind> <name>` helper can automate the copy+branch+PR.

Seeding (`initialize_catalog_from="local"`) survives only for standing up standalone catalogs
(e.g. bootstrapping a new group repo) — bundled content is never copied into the user layer, so
there is no re-sync/merge problem on upgrade.

### 4.3 Prerequisite for R3/R4: real identity and typed references

1. **Stable entry identity.** Give every catalog entry (and every forge blueprint) an `id` — a
   creation-time UUID (or `name@<short-hash>`) plus the human name. IDs are minted once and never
   recomputed (immune to the content-hash churn in §2). Content hashes remain integrity/dedup
   fingerprints once canonicalized; the join key is the ID.
2. **Typed references.** Extend `PieceRef` to carry the source entry's `id` (and the content hash
   of what was snapshotted) alongside `name/origin/modified`. Snapshot-don't-reference is
   untouched — the ref is provenance metadata the indexer turns into graph edges. Same for
   `Domain.yaml: model_name` → `model_id`.

With those, the SQLite index is trivial: `entries(id, kind, name, store, path, content_hash, ...)`
+ `edges(from_id, to_id, relation)`; both directions of R3 are one query.

### 4.4 R4: downstream outputs report back via breadcrumbs, not a service

The executor/run machinery drops a small **run manifest** (blueprint id + content_hash,
roms_marbl blueprint path, output URIs, machine, timestamps, status) in the run's working dir
and/or a `runs/` area of the user's scratch store. The indexer ingests manifests exactly like
catalog entries. "Given a domainspec, show all outputs" = domainspec →edges→ blueprints →edges→
run manifests → output paths. The executor still never *reads* the catalog; it only emits one more
self-describing artifact.

### 4.5 Assets (later): make blueprints-with-files portable

An opt-in "import into catalog" that copies an attached netCDF into the user store's assets area
keyed by content hash, letting `UserProvidedFile.location` be a catalog-asset reference in
addition to an absolute path (`add_asset_to_domain` is a starting point). Large-file hygiene in
shared git stores then needs git-lfs/DataLad or an "assets stay out of git" policy — decide when
it becomes real.

## 5. Implemented: the short-term slice (2026-08-14, branch `catalog`)

What shipped, relative to the plan above:

1. **Layered stores** (`cstar_forge/domain_catalog.py`): `DomainCatalog` gained
   `read_only`/`label` (non-local stores are always read-only — this also fixed the latent
   GitHub-catalog CWD-write bug) and a flat forge-blueprint scanner
   (`forge_blueprint_names`/`forge_blueprint_path` — the shipped `blueprints/*.forge_blueprint.yaml`
   files were previously invisible to any scanner). New `LayeredCatalog` facade: union reads,
   top-first resolution, `entry_source`/`collisions()`, writers that enforce stack-wide uniqueness
   (`FileExistsError` naming the owning layer) and delegate to the writable top store.
   `default_catalog_stack()` builds `[user (rw) → bundled (ro)]`; `CSTAR_FORGE_CATALOG`
   (os.pathsep-separated roots, first = writable top, `"local"` = bundled) overrides the stack.
2. **User layer location**: `user_catalog_root()` = `~/cstar-forge-data/catalog`, deliberately
   **home-anchored** — the `config.py` HPC layouts rebase data caches onto `$SCRATCH`/`$WORK`,
   which get purged; durable user-registered content must not live there. `config.paths.catalog`
   is now wired to it (the PR #100 vestige has a consumer; `default_catalog_inner_dir` deleted).
3. **Lazy everything**: `default_catalog`, `catalog.blueprint`, and `config.machine_config` are
   PEP 562 lazy — `import cstar_forge` no longer scans any catalog. Machine config now reads
   through the stack, so a user `Machines/<tag>.yaml` overrides the bundled one (top-first).
4. **Wizard**: piece dropdowns badge lower-layer entries (`wio-toy (bundled)`) via homogeneous
   `(label, value)` option tuples (`_dd_options`/`_dd_values`); blueprint/workplan saves and piece
   registrations land in the user layer (dirs created on demand); collision errors surface as
   plain one-line messages naming the owning layer; the catalog bar accepts a pathsep-separated
   stack and reports per-layer counts; blank = the default stack.
5. **Tests** (816 passing at merge): conftest forces `CSTAR_FORGE_CATALOG` to a throwaway temp dir
   (the suite must never touch a developer's real `~/cstar-forge-data`); new `LayeredCatalog`
   coverage (union/precedence/collision warning, write routing, read-only guards, env handling,
   import laziness, badge shapes).
6. **Packaging/docs**: explicit package-data entry for the bundled catalog tree; getting-started /
   machine-config / architecture-details updated; migration note for blueprints stranded in an old
   env's site-packages (copy the YAMLs — they're self-contained).

Deliberately *not* in this slice (next phases, in rough order): IDs + typed refs (§4.3), the
SQLite index, the `contribute` helper (R2), run manifests (§4.4), asset import (§4.5).

## 6. Open questions for the next phase

- ID scheme (UUID vs `name@hash`), and whether IDs get stamped retroactively on bundled entries at
  first index build.
- Should the bundled catalog eventually shrink to a pure demo set, with the curated catalog
  becoming a shared git store? Plausible now that layering exists; requires standing up a public
  catalog repo (the fsspec GitHub read path and the dormant `CWorthy-Demo` tests point that way).
- Shared-layer ergonomics: is a plain git clone enough, or do we want `cstar forge catalog`
  subcommands (init/where/list/contribute) first?
- Run-manifest schema and where the indexer lives (forge? C-Star? a small shared lib) — must
  respect the executor/catalog boundary.

*Resolved:* user-store default location (home-anchored `~/cstar-forge-data/catalog` — survives
HPC purges, already documented); collision policy (hybrid — see §4.2).
