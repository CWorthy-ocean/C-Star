# C-Star Artifact Caching System — Implementation Plan

## Context

C-Star needs a generic cache for expensive file-producing sub-step operations (hours of compute, TBs of data — e.g. roms-tools dataset generation) so prior results are reused instead of regenerated. Requirements are in `caching.md` (note: that file is truncated mid-sentence at line 111): a personal ephemeral cache (SCRATCH on HPC) plus a group durable cache, lookup order group → personal → regenerate-into-personal, decorator opt-in with argument-based keys, real files in cache storage with symlinks into the user output dir, a `CacheHandle` return, post-run promote, and inspect/clear tooling. First real consumer is cstar-forge, whose current filename-existence "caching" reuses stale outputs when settings change. Prototype phase — validate architecture, don't over-harden.

## Build vs. buy: Prefect and the OSS field (evaluated per request, incl. empirical probes)

### Prefect assets (`@materialize`)
Lineage/observability only — no skip/reuse semantics — and the asset UI is a Prefect **Cloud** feature. With C-Star's ephemeral OSS servers (no long-running server possible on HPC), asset records die with each run's server DB. Not usable as the cache mechanism.

### Prefect task caching — the "Prefect-maximal" option, empirically probed (Prefect 3.8.1, this env)
The question: could deliberate `@task` adoption + refactoring file-generation side effects + extending the task decorator get us there? Probe results (standalone cached task, no server running, two processes):

**What works (verified):**
- Standalone `@task` calls run fine with no pre-existing server — a temporary server auto-spawns (~4 s per process, sqlite under `$PREFECT_HOME`). Cross-process cache hits work: second process returned `Cached(type=COMPLETED)` without executing the body, via filesystem `key_storage` + filesystem result storage.
- Key ergonomics are genuinely close to our spec: `INPUTS - "arg"` exclusion (verified), policy arithmetic, custom `cache_key_fn`, `refresh_cache`/`PREFECT_TASKS_REFRESH_CACHE` (= `--no-cache`), SERIALIZABLE isolation + `FileSystemLockManager`.

**What breaks (verified):**
1. **It caches return values, not files.** The persisted "result" is a cloudpickle blob of the return value (here: a list of path strings). The actual data files remain unmanaged side effects — on a hit, another user gets stale absolute paths into the first runner's directories. TB-scale artifacts are exactly what Prefect does not manage.
2. **Fixing #1 requires knowing the cache key inside the task** (write outputs into a key-derived cache dir). The key is computed in the engine as the transaction key (`compute_transaction_key`, `task_engine.py:270-296`) and is not cleanly exposed to the body — so we'd compute keys ourselves anyway, at which point Prefect contributes only "skip if record exists," the trivial part.
3. **Cache records are opaque**: a hash-named file containing only `storage_key`, serializer info, and a `storage_block_id`. No function name, no key material, no file list, no provenance → caching.md §3 (post-hoc identifiability) and the inspect/promote CLI would need a parallel metadata store of our own regardless.
4. **No dual-tier lookup.** One `key_storage` per policy. Group-then-personal would mean a custom tiered `WritableFileSystem` block — and blocks **must be `.save()`d server-side** (verified: `TypeError: Result storage configuration must be persisted server-side`), i.e. registered into each user's ephemeral server sqlite DB; custom block types need registration too. The `storage_block_id` baked into every cache record ties records to per-user server DBs — poison for a durable shared cache.
5. **Stability/coupling costs**: cloudpickle serialization for results (and pickle-fallback hashing for non-JSON-able inputs) is not guaranteed stable across Python/library versions — risky for a durable group cache; plus every cached call site (including Forge imports) drags in the temporary-server machinery.

**Net**: Prefect would supply ~the easy 20% (key computation + skip-on-hit, both small) while the hard 80% (artifact store layout, staging/atomicity, manifests, symlink placement, tier fallback, promote, CLI) gets built by us anyway — against Prefect's opaque records instead of our own. Not an effort win.

### OSS survey — is there a standard package for this?
No. Nothing found that combines decorator-level opt-in + multi-file artifact management + tiered personal/group stores + promote:
- **Return-value memoizers** (joblib.Memory, diskcache, klepto, cachier, locache, cache_to_disk): single store, pickled return values, no artifact/file management. joblib's arg hashing is a possible internal reuse for `keys.py` if numpy args show up (explicit tokenization still preferred for cross-version key stability).
- **`provenance` (bmabey)**: conceptually closest (decorator, tiered blobstores, artifact repo) — **GitHub-archived, last push Dec 2020**. Non-viable.
- **DVC**: real shared-cache machinery (`dvc cache dir` on shared storage, `cache.shared group` perms, `cache.type symlink`) — but repo/CLI/pipeline-centric (git-coupled `dvc.yaml` stages), single cache dir, keys from file-deps+params rather than Python function args. Adopting it means restructuring around DVC's model; instead we crib its design (content-addressed store + link strategy + group-permission conventions).
- **Workflow engines with run caching** (redun, Metaflow, snakemake, luigi/pytask): either output-existence semantics (exactly the Forge bug) or a second orchestrator competing with Prefect.

### Conclusion
Build the thin custom layer (~800 lines core): plain-Python decorator + on-disk YAML manifests, Prefect-agnostic (also required by the process boundary: steps run as detached CLI child processes — `StepToRunRequestAdapter.adapt()` at `cstar/orchestration/adapter.py:54` — with no Prefect context, and Forge imports it outside any flow). Prefect stays exactly where it is today. The decorator API deliberately mirrors Prefect's ergonomics (`key_exclude` ≈ `INPUTS - "arg"`) so a future migration path stays open if Prefect ever grows artifact-aware caching.

## Design decisions (user-confirmed)

- **Path args keyed by resolved path string** (fast, deterministic, no I/O); per-arg opt-in content sampling via `key_by={"grid_file": file_fingerprint}`. Documented tradeoff: changed content at same path → stale hit unless fingerprinted.
- **Demo app ships in-package** (`cstar/applications/cache_demo.py`, hello_world pattern), hand-runnable via `cstar blueprint run`.
- Function identity = `module.qualname` + explicit `version="1"` decorator param (bump to invalidate) — no source hashing.
- Concurrency: atomic staging-dir rename wins; loser discards staging and adopts winner's entry. Locking/checksums/eviction = documented future work.
- Promote **copies** by default (`--delete-source` opt-in) so symlinks in completed run dirs don't break.
- `CacheHandle` is a standalone pydantic model mirroring `ProcessHandle`'s shape (`orchestration.py:203`), not subclassing it.

## New modules

```
cstar/caching/
  __init__.py    # public API: cached_artifact, CacheHandle, CacheManager, CacheTier, file_fingerprint
  config.py      # personal_cache_root(), group_cache_root(), caching_disabled()
  keys.py        # tokenize(), compute_key(), file_fingerprint(), CacheKeyError
  models.py      # CacheManifest, CacheFileRecord, CacheProvenance, CacheEntry, CacheHandle, ReturnSpec
  store.py       # CacheStore (one tier), CacheManager (tiered lookup + promote), place_symlinks()
  decorator.py   # cached_artifact (sync + async wrappers)
cstar/cli/cache/
  __init__.py    # typer app, gated by CSTAR_FF_CACHE (mirror cstar/cli/admin/__init__.py:13)
  commands.py    # list / show / promote / clear
cstar/applications/cache_demo.py
cstar/additional_files/templates/bp/cache_demo/blueprint.1.0.0.yaml
cstar/tests/unit_tests/caching/   # + CI workflows already glob cstar/tests/unit_tests/*
```

## Key computation (`keys.py`)

- `inspect.signature(fn).bind(*args, **kwargs).apply_defaults()` → kwarg-order/default invariance. Auto-exclude `self`/`cls`/`output_param`, then `key_exclude`.
- `tokenize()`: None/bool/int/str as-is; float→repr; `Path`→resolved str; datetime→isoformat; Enum→value; pydantic→`model_dump(mode="json")`; mappings/sequences/sets recursed (sets sorted); anything else → `CacheKeyError` naming the arg and suggesting `key_exclude`/`key_by`.
- `key_by[name]` transform pre-tokenize; `key_extra` (mapping or callable over bound args) under reserved `"extra"` namespace.
- Material `{"function", "version", "args", "extra"}` → canonical JSON → sha256 hex. Returns `(key, material)`; material persisted verbatim in the manifest (post-hoc identifiability, caching.md §3/§7).
- `file_fingerprint(path)` → `{path, size, sha256 of first+last 1MiB}` (exported opt-in helper).

## On-disk layout & store (`store.py`)

```
<tier_root>/
  entries/<function_slug>/<key_hash>/
    manifest.yaml
    payload/<files...>
  staging/<key_hash>.<uuid4hex>/     # same FS as entries/ ⇒ atomic Path.replace
```

- `CacheStore(root, tier)`: `find()` (validates manifest deserializes + every file exists at recorded size, else None + warning), `begin_staging()`, `commit()` (write manifest into staging, atomic rename; on EEXIST/ENOTEMPTY → rmtree staging, return existing winner), `iter_entries()`, `resolve_prefix()` (≥8-char key prefix or exact label; ambiguous → error listing candidates), `remove()`, `total_size()`.
- `CacheManager(personal, group|None)`: `from_env()`, `lookup()` (group first, then personal; group permission errors degrade to personal + warning), `promote(entry, delete_source=False)` (copy payload+manifest into group staging, stamp `promoted_at/promoted_by`, atomic rename — the colocated manifest IS the group record), `iter_all()`.
- `place_symlinks(entry, output_dir)`: per file mkdir parents, unlink stale target, `symlink_to` payload path.
- Manifests via existing `cstar/orchestration/serialization.py` `serialize`/`deserialize` (the `WorkplanRun`/`TrackingRepository` YAML pattern).

Manifest fields (`models.py`, all pydantic): `schema_version`, `key`, `function`, `function_version`, `label`, `key_material`, `files: list[CacheFileRecord{relpath,size_bytes,sha256=""}]`, `return_spec`, `provenance{created_at, created_by, hostname, cstar_version, run_id, promoted_at, promoted_by}`.

`CacheHandle`: `key, function, hit, tier|None, paths` (symlinks in caller's output dir), `payload_paths` (real files), `created_at, provenance, result` (restored return value).

## Decorator (`decorator.py`)

```python
@cached_artifact(version="1", label="", key_exclude=(), key_extra=None,
                 key_by=None, output_param="output_dir",
                 manager_factory=CacheManager.from_env)
def generate_tiles(dataset_name: str, num_files: int, output_dir: Path) -> list[Path]: ...
handle = generate_tiles("gulf_of_maine", 3, output_dir=fsm.output_dir)  # -> CacheHandle
```

- Wrapped fn must accept a `Path` param named `output_param` (validated at decoration time). **Output discovery = injected staging dir + post-run scan**: on miss the decorator substitutes `staging/payload/` for the caller's output dir, runs the fn, records everything found there (catches sidecar files roms-tools writers emit; staging starts empty so the scan is exhaustive).
- Flow: compute key → if `caching_disabled()` (`--no-cache`): run into real output dir, return `CacheHandle(hit=False, tier=None)`, record nothing → else `lookup()`; hit: `place_symlinks` + restore return value → miss: run into staging, scan, commit, symlink. Fn exception → delete staging, re-raise. Sync + `iscoroutinefunction` async variants, both in phase 1.
- `ReturnSpec` restore: `Path`/`list[Path]`/`dict[str,Path]` under payload → store relpaths, rebase onto symlinked output paths on hit; JSON-serializable → stored verbatim; else `kind: none` + one-time warning (`handle.result` is None on hit).

## Config & flag wiring

- `cstar/base/env.py` (auto-discovered by `discover_env_vars()`): `ENV_CSTAR_CACHE_PERSONAL_ROOT` (GROUP_FS, `default_factory` → `hpc_data_directory()` per `env.py:128`, laptop fallback `cache_home()/artifact-cache`); `ENV_CSTAR_CACHE_GROUP_ROOT` (GROUP_FS, empty ⇒ group tier disabled — no PROJECT detection exists in the codebase, greenfield; per-system defaults in `cstar/system/manager.py` = future work); `ENV_CSTAR_CACHE_DISABLE` (GROUP_SIM, FLAG_OFF). Note: existing `ENV_CSTAR_CACHE_HOME` (`env.py:247`) is XDG `~/.cache` — wrong storage class for HPC; personal root deliberately avoids it, and stays disjoint from the git cache used by `CachedRemoteRepositoryStager` (`cstar/io/stager.py:179`).
- `cstar/base/feature.py`: `ENV_FF_CACHE = "CSTAR_FF_CACHE"` — gates **CLI subcommand registration only**; the decorator is always live (explicit opt-in; Forge importability).
- `cstar/entrypoint/utils.py`: `ARG_NO_CACHE = "--no-cache"` + help const.
- `--no-cache` typer.Option on `cstar/cli/blueprint/run.py` and `cstar/cli/workplan/run.py` using the four-part `--clobber` pattern (`cstar/cli/workplan/run.py:426-434`): `typer.Option(ARG_NO_CACHE, envvar=ENV_CSTAR_CACHE_DISABLE, callback=set_flag(ENV_CSTAR_CACHE_DISABLE), ...)`.
- Child-process propagation: in `StepToRunRequestAdapter.adapt()` after the clobber conditional (`adapter.py:69-70`): `if is_flag_enabled(ENV_CSTAR_CACHE_DISABLE): cmd_array.append(ARG_NO_CACHE)`.

## Management CLI (`cstar cache …`)

Registered in `cstar/cli/cli.py` `attach_subcommands`; import gated by `is_feature_enabled(ENV_FF_CACHE)` inside `cstar/cli/cache/__init__.py`. Rich tables; entries addressed by key prefix or label.

- `cstar cache list [--function] [--tier]` — short key (12), label, function tail, tier, created, #files, size.
- `cstar cache show KEY_PREFIX` — full manifest + entry dir.
- `cstar cache promote KEY_PREFIX [--delete-source] [--yes]`.
- `cstar cache clear [KEY_PREFIX] [--tier personal] [--all] [--yes] [--dry-run]` — group clears require explicit `--tier group`. Confirm-prompt style from `cstar/cli/admin/clean.py`.

## Demo app (`cache_demo`, in-package)

Follows `cstar/applications/hello_world.py` exactly (Blueprint + Runner + no-op SchemaAdapter 1.0.0→1.0.0 + `@register_application`).

- `CacheDemoBlueprint(Blueprint)`: `application="cache_demo"`, `dataset_name: str`, `num_files: int = 3`, `sleep_seconds: float = 2.0`, `working_dir`.
- Three decorated module-level functions (small files + short sleeps standing in for TB/hours):
  - `generate_summary(...) -> Path` — single file, path restore
  - `generate_tiles(...) -> list[Path]` — multi-file, list restore
  - `compute_stats(...) -> dict` — file + lightweight dict, JSON restore
- `CacheDemoRunner.run()`: `JobFileSystemManager(working_dir).prepare()`, call the three with `output_dir=fsm.output_dir`, log each handle (`hit`, `tier`, `key[:12]`), `add_state(ExecutionStatus.COMPLETED)`.
- Template blueprint at `cstar/additional_files/templates/bp/cache_demo/blueprint.1.0.0.yaml`.

End-to-end walkthrough (goes in docs):
```
export CSTAR_FF_CACHE=1
export CSTAR_CACHE_GROUP_ROOT=/path/to/project/cstar-cache   # optional
cstar blueprint run bp.yaml            # miss: ~8s, symlinks in output/
cstar blueprint run bp.yaml            # hit: instant, tier=personal
cstar cache list && cstar cache promote <key12>
cstar cache clear <key12> --yes
cstar blueprint run bp.yaml            # hit: tier=group
cstar blueprint run bp.yaml --no-cache # regenerates in place, no record
# edit dataset_name in bp.yaml         # -> new key -> miss (the Forge bug, fixed)
```

## Tests (`cstar/tests/unit_tests/caching/`, asyncio_mode=auto)

- `conftest.py`: `mock_cache_roots(tmp_path)` fixture patching the two root env vars (pattern: `mock_xdg_dirs`, `unit_tests/conftest.py:2236`); counting decorated fn helper.
- `test_keys.py`: determinism; positional/kwarg + kwarg-order + defaults invariance; `key_exclude`/`key_extra`/`version`/auto-exclusions; type tokens; `CacheKeyError`; `file_fingerprint` sensitivity.
- `test_store.py`: staging/commit roundtrip; `find` None on missing/corrupt/size-mismatch; commit race (pre-created winner → loser staging removed, winner returned); `resolve_prefix` exact/ambiguous/missing.
- `test_decorator.py`: miss→entry+symlinks+handle fields; second call hit (call counter unchanged); group beats personal; `--no-cache` passthrough (real files, no record, `tier is None`); all four return-restore kinds; exception cleans staging; async variant; missing output param → decoration-time TypeError.
- `test_promote.py`: copy + provenance stamp; group hit after promote; `--delete-source`; idempotent re-promote.
- `test_cli_cache.py`: `CliRunner` over all four commands incl. `--dry-run` and group-clear guard.
- `test_cache_demo.py`: run `CacheDemoRunner` twice — second run all hits, outputs are symlinks; `--no-cache` env → regular files.
- Existing `test_adapter.py`: `ENV_CSTAR_CACHE_DISABLE=1` ⇒ `ARG_NO_CACHE` in adapted command.

## Phasing (PR-sized)

1. **Core + personal tier**: `cstar/caching/` complete, env vars, `ARG_NO_CACHE` const, keys/store/decorator tests (`CacheManager` accepts `group=None`).
2. **Group tier + promote + CLI + flag plumbing**: `cstar cache` commands, `CSTAR_FF_CACHE`, `--no-cache` options, adapter propagation, promote/CLI tests.
3. **Demo app + docs**: `cache_demo`, template blueprint, demo test, docs page (usage, symlink-copy tradeoff → recommend `cp -rL`, group-root `chmod g+rwxs` setup note, why-not-Prefect rationale).
4. **Later (separate)**: adopt in real targets (`ROMSInputDataset.partition/get` `cstar/roms/input_dataset.py:193,364`, `NestIcRunner._create_initial_conditions`, `CDRUpscaler.save` `upscaler.py:314`), replace cstar-forge filename checks; then locking, checksums, eviction, per-system group roots.

## Verification

- `pytest cstar/tests/unit_tests/caching/` green; existing suites unaffected (`pytest cstar/tests/unit_tests/`).
- Manual e2e: run the demo walkthrough above on a laptop — confirm miss timing vs. instant hit, `ls -l` shows symlinks into the cache root, `cstar cache list/show/promote/clear` behave, `--no-cache` writes real files, and editing `dataset_name` forces a miss.
- Inspect a manifest.yaml by hand to confirm post-hoc identifiability (key material readable without recomputation).

## Key reused utilities

- `cstar/orchestration/serialization.py` — `serialize`/`deserialize` for manifests
- `cstar/base/env.py` — `EnvVar` descriptor pattern, `hpc_data_directory()`
- `cstar/execution/file_system.py` — `DirectoryManager`, `JobFileSystemManager`
- `cstar/base/utils.py` — `slugify` (:307), `utc_now` (:417)
- `cstar/base/feature.py` — `is_flag_enabled`, `is_feature_enabled`
- `cstar/cli/common.py` — `set_flag` callback; `cstar/cli/admin/__init__.py` FF gating pattern
