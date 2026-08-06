# Caching System — Session Record (2026-08-06)

Working session on branch `caching-poc`: requirements in `caching.md` → research →
plan → full implementation with adversarial review. Raw machine transcript
snapshot: `notes/caching-session-transcript.jsonl` (Claude Code session
`1e23e336-a9c3-41c5-9159-a50d523e3aab`). Implementation plan document:
`~/.claude/plans/purrfect-skipping-lagoon.md`.

## 1. Research phase

### Question: can Prefect assets or task caching serve as the cache?

**Assets (`@materialize`)** — lineage/observability only, no skip/reuse
semantics; the asset UI is a Prefect Cloud feature. With C-Star's ephemeral
per-run servers (no long-running server possible on HPC), asset records die
with each run's server DB. Rejected.

**Task caching (empirically probed against Prefect 3.8.1 in the live env):**

Works better than expected:
- Standalone cached `@task` calls run with **no pre-existing server** — a
  temporary server auto-spawns (~4 s/process, sqlite in `$PREFECT_HOME`).
- **Cross-process cache hits work** via filesystem `key_storage` +
  filesystem result storage (second process returned `Cached(type=COMPLETED)`
  without executing the body). The ephemeral-server constraint is NOT the blocker.
- Key ergonomics close to spec: `INPUTS - "arg"` exclusion verified; policy
  arithmetic; `refresh_cache` ≈ `--no-cache`; SERIALIZABLE + FileSystemLockManager.

Deal-breakers (each verified):
1. Prefect caches **pickled return values, not files**. The persisted result
   is a cloudpickle blob (probe: a list of path strings); TB-scale artifacts
   remain unmanaged side effects. A hit hands other users stale absolute
   paths into the first runner's scratch.
2. Fixing that requires the task body to know its cache key (to write into a
   key-derived dir), but the key is computed internally as the transaction key
   (`compute_transaction_key`, task_engine.py) and not cleanly exposed — so we
   would compute keys ourselves anyway, leaving Prefect with only "skip if
   record exists".
3. Cache records are opaque: `storage_key` + serializer + `storage_block_id`
   only — no function, inputs, provenance, or file list. Inspect/promote needs
   our own metadata store regardless.
4. No dual-tier lookup; storage blocks must be `.save()`d **server-side**
   (verified TypeError), and the `storage_block_id` baked into each record
   ties a durable group cache to per-user ephemeral server DBs.
5. Architecture: workplan steps run as detached CLI child processes
   (`StepToRunRequestAdapter.adapt()` → `["cstar","blueprint","run",...]`)
   with no Prefect context; Prefect's repo footprint is 1 flow + 1 task.
   Forge must import the cache with no Prefect run at all.

**Verdict:** Prefect supplies the easy ~20 % (keying, skip-on-hit); the hard
80 % (artifact store, staging/atomicity, manifests, symlinks, tiers, promote,
CLI) gets built either way. Not an effort win.

### OSS survey — no standard package does this

- Return-value memoizers (joblib.Memory, diskcache, cachetools, klepto,
  cachier, locache, cache_to_disk, results-filecache): no file management,
  single store.
- `bmabey/provenance` — conceptually closest (decorator + tiered artifact
  stores) — **GitHub-archived Dec 2020**. Non-viable.
- DVC — real shared-cache machinery (`cache.shared group`, `cache.type
  symlink`) but git/CLI/pipeline-centric with file-dep keys; adopted its
  storage *design* (content-addressed store + link strategy + group perms),
  not the tool.
- Workflow engines (redun, Metaflow, snakemake, luigi/pytask): either
  output-existence semantics (the exact Forge bug) or a second orchestrator.

## 2. Design decisions (user-confirmed where noted)

- **Build custom, Prefect-agnostic** plain-Python layer in `cstar/caching/`.
- **Path args keyed by resolved path string** (user-confirmed); opt-in content
  sampling via `key_by={"grid_file": file_fingerprint}`; the fingerprint
  deliberately excludes the path so fingerprinted keys match across users.
- **Demo app ships in-package** (user-confirmed), hello_world pattern.
- Function identity = qualname + explicit `version=` bump (no source hashing).
- Lookup: group → personal → regenerate-into-personal-staging → atomic rename.
- Real files in cache storage; **symlinks** into output dirs (spec §6).
- Promote **copies** by default (`--delete-source` opt-in) so prior runs'
  symlinks survive; idempotent.
- Concurrency: atomic rename wins; loser adopts winner. Locking/checksums/
  eviction deferred.
- `CacheHandle` mirrors `ProcessHandle`'s shape but standalone.
- Decorator API mirrors Prefect ergonomics (`key_exclude` ≈ `INPUTS - "arg"`)
  to keep a migration path open.

## 3. What was implemented

| Area | Files |
|---|---|
| Core | `cstar/caching/{__init__,config,keys,models,store,decorator}.py` |
| Env/flags | `cstar/base/env.py` (`CSTAR_CACHE_PERSONAL_ROOT`, `CSTAR_CACHE_GROUP_ROOT`, `CSTAR_CACHE_DISABLE`), `cstar/base/feature.py` (`CSTAR_FF_CACHE`) |
| CLI | `cstar/cli/cache/{__init__,commands}.py` (list/show/promote/clear), registered in `cstar/cli/cli.py`; `--no-cache` on `cstar/cli/{blueprint,workplan}/run.py`; propagation in `cstar/orchestration/adapter.py` |
| Demo | `cstar/applications/cache_demo.py`, `cstar/additional_files/templates/bp/cache_demo/blueprint.1.0.0.yaml`, `Application.CACHE_DEMO` enum member |
| Docs | `docs/caching.rst` (+ index toctree) |
| Tests | `cstar/tests/unit_tests/caching/` (conftest + 6 modules, 90 tests) + adapter test in `test_cmd_converter.py` |

## 4. Bugs found and fixed during verification

Found by self-testing during the build:
- **StrEnum YAML round-trip**: `model_to_yaml` (full `yaml.Dumper`) writes
  StrEnums as python-tagged nodes `yaml.safe_load` rejects → `ReturnSpec`
  uses `use_enum_values=True`.
- **Provenance loss via `exclude_defaults=True`**: default-factory provenance
  fields were dropped at dump and re-fabricated from the *reader's* env
  (group manifests would claim the reader created them) → creation fields are
  now required and captured via `CacheProvenance.capture()`.
- **`--no-cache` cache poisoning**: bypass runs wrote *through* leftover cache
  symlinks in the output dir, mutating cache payloads → bypass unlinks
  cache-pointing symlinks first; snapshot diff also counts overwritten files.

Found by the adversarial review agent (all fixed + regression-tested):
1. **CRITICAL — JSON return values persisted through non-safe YAML**: a
   returned tuple/StrEnum wrote an unreadable manifest → `CacheCommitError`
   *after* the expensive function ran, and the wedged entry made every later
   call fail too. Fix: `json.loads(json.dumps(raw))` normalization; misses and
   hits now agree (tuple → list).
2. **MAJOR — nondeterministic keys across processes** for pydantic args with
   `set` fields (`model_dump(mode="json")` uses hash-randomized set order).
   Fix: python-mode dump recursed through the sorted-set tokenizer. Regression
   test spawns subprocesses under different `PYTHONHASHSEED`s.
3. **MAJOR — dead staging paths in restored values**: `str(path)` returns
   classified as JSON and persisted a path into the renamed-away staging dir.
   Fix: detected and refused with a warning; `ReturnKind.none` misses warn too.
4. Path-string keys are host/user-specific → documented; `file_fingerprint`
   no longer embeds the path.
5. `commit()` invalid-occupant retry now adopts a concurrent winner instead of
   raising after successful execution.
6. `--no-cache` no longer fails on unkeyable arguments.
7. Token collisions eliminated: set-vs-list, datetime-vs-isoformat-string,
   same-named enums across modules; `key_by` now reaches `**kwargs` params.
8. `place_symlinks` refuses output dirs inside the entry (self-link data
   loss) and reports path-blocked mkdirs clearly.
9. `cstar cache clear --all` reaps crashed runs' staging leftovers.

Reviewer-verified non-issues worth remembering: bool/int distinct in canonical
JSON; PyYAML quotes date-like strings on dump (safe round-trip); float
nan/inf/-0.0 repr stable; two-writer commit race adopt path correct;
`*args`/positional-only tokenization correct.

## 5. Verification results

- Full unit suite: **1409 passed, 13 skipped** (baseline before feature: 1400) — zero regressions.
- `ruff check` + `ruff format` + `mypy` clean on all new/changed files.
- End-to-end via `python -m cstar.cli.cli` (note: the env's `cstar` script is
  editable-installed from `C-Star-deps-overhaul`, not this worktree):
  generate ~3 s → all-hit rerun 0.00 s → promote by label → clear personal →
  group-tier hit → `--no-cache` regeneration with cache payload verifiably
  untouched (mtime + content checked).

## 6. Open items / future work

- Adopt in real targets: `ROMSInputDataset.partition/get`,
  `NestIcRunner._create_initial_conditions`, `CDRUpscaler.save`; replace
  cstar-forge filename-existence caching (separate repo).
- Hardening: checksums, per-key advisory locking, eviction policy,
  per-system group-root defaults in the `cstar/system` registry.
- `caching.md` (requirements doc) is truncated mid-sentence at line 111.
- All work is uncommitted on `caching-poc` as of this record.
