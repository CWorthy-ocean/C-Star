Artifact Caching
================

.. warning::
    The artifact cache is a prototype-phase feature. Its management CLI is
    gated behind the ``CSTAR_FF_CACHE`` feature flag, and on-disk formats may
    change between releases.

C-Star includes a generic cache for expensive file-producing operations
(e.g. input-dataset generation that takes hours and produces terabytes).
When a cached operation is invoked with inputs it has seen before, its
outputs are reused instead of regenerated.

Concepts
--------

Two storage tiers
    The **personal cache** is per-user, ephemeral storage — on HPC systems it
    defaults to ``<scratch>/cstar/artifact-cache`` (located via
    ``CSTAR_SCRATCH_DIRS``), elsewhere to ``<cache-home>/artifact-cache``.
    New results are always written here. The **group cache** is shared,
    durable storage (typically PROJECT-class) holding results a user has
    explicitly *promoted* for reuse by others. It is enabled by setting
    ``CSTAR_CACHE_GROUP_ROOT``.

Lookup order
    group cache → personal cache → regenerate (into the personal cache).

Real files live in the cache
    Cached files are stored inside cache storage; the run's output directory
    receives **symlinks** to them. This prevents a deleted output directory
    from corrupting the cache.

    .. warning::
        Because outputs are symlinks, copying an output directory with
        ``cp -r`` copies links, not data. Use ``cp -rL`` (or ``rsync -L``)
        to materialize real files when exporting results.

Keys are computed from inputs, not filenames
    A cache key is a SHA-256 digest of the producing function's identity, an
    explicit version string, and its (tokenized) arguments. Changing any
    input that affects outputs produces a different key — unlike
    filename-existence checks, stale results are not reused when settings
    change.

Manifests make entries auditable
    Every entry stores a human-readable ``manifest.yaml`` recording the key,
    the exact inputs it was computed from, the file list, and provenance
    (who, when, where, C-Star version). Entries can be inspected, promoted,
    and cleared after the fact without rerunning anything.

Quickstart with the demo application
------------------------------------

The ``cache_demo`` application generates a few small files with short sleeps,
standing in for hours-long TB-scale generation::

    export CSTAR_FF_CACHE=1
    # optional overrides; sensible defaults exist on HPC systems
    export CSTAR_CACHE_PERSONAL_ROOT=$SCRATCH/cstar-cache
    export CSTAR_CACHE_GROUP_ROOT=/path/to/project/cstar-cache

    cp cstar/additional_files/templates/bp/cache_demo/blueprint.1.0.0.yaml bp.yaml
    cstar blueprint run bp.yaml            # generates (seconds of "work")
    cstar blueprint run bp.yaml            # instant: served from personal cache

    cstar cache list                       # inspect entries
    cstar cache show demo-tiles            # full manifest by label (or key prefix)
    cstar cache promote demo-tiles --yes   # publish to the group cache
    cstar cache clear demo-tiles --yes     # drop the personal copy
    cstar blueprint run bp.yaml            # instant: served from the GROUP cache

    cstar blueprint run bp.yaml --no-cache # force regeneration, bypass the cache

Editing ``dataset_name`` in ``bp.yaml`` changes the cache keys, so the next
run regenerates — demonstrating that keys track inputs, not filenames.

Caching your own functions
--------------------------

Opt a file-producing function into caching with
:func:`cstar.caching.cached_artifact`. The function must accept an
output-directory parameter (``output_dir`` by default) and write **all** of
its file outputs beneath it::

    from pathlib import Path
    from cstar.caching import cached_artifact

    @cached_artifact(version="1", label="tidal-forcing")
    def generate_tidal_forcing(grid_name: str, ntides: int, output_dir: Path) -> Path:
        path = output_dir / f"{grid_name}_tides.nc"
        ...  # hours of work, TBs of data
        return path

    handle = generate_tidal_forcing("gulf", 10, output_dir=run_dir / "output")
    handle.hit      # True when served from cache
    handle.tier     # "group", "personal", or None (--no-cache)
    handle.paths    # files in your output dir (symlinks when cached)
    handle.result   # the function's (restored) return value

Key behavior is customizable:

- ``version="2"`` — bump whenever the function's logic changes its outputs;
  this invalidates all prior entries for the function.
- ``key_exclude=("verbose",)`` — omit arguments that do not affect outputs.
- ``key_extra={"roms_tools": rt_version}`` — mix additional facts into the
  key (also accepts a callable receiving the bound arguments).
- ``key_by={"grid_file": file_fingerprint}`` — key a ``Path`` argument by a
  sampled content fingerprint instead of its path string. By default path
  arguments are keyed **by resolved absolute path only**, which has two
  consequences: content changes at the same path are *not* detected, and two
  users whose copies of an input live at different paths get different keys
  (so path-keyed entries never match via the group cache). Use
  fingerprinting for arguments where either matters.

Return values are restored on cache hits when they are cheaply
reconstructable: paths under the output directory (single, list, or mapping)
are rebased onto the caller's output directory, and JSON-serializable values
are stored verbatim. Anything else yields ``handle.result = None`` on hits —
return paths or plain data rather than heavyweight objects.

Group cache setup
-----------------

The group root is used verbatim and should be provisioned once on durable
shared storage with group-writable permissions::

    mkdir -p /path/to/project/cstar-cache
    chgrp <group> /path/to/project/cstar-cache
    chmod g+rwxs /path/to/project/cstar-cache

Writes use unique staging directories with atomic renames, so concurrent
writers on a shared filesystem cannot publish partial entries; when two
processes race to produce the same key, the first commit wins and the loser
adopts it.

Design notes
------------

Why not Prefect?
    C-Star already uses Prefect, and Prefect task caching was evaluated
    first. It is ephemeral-server-compatible (cache records can live on a
    filesystem), but it caches *pickled return values*, not managed file
    artifacts: the data files themselves remain untracked, records are
    opaque (no inputs, provenance, or file lists), there is no group/personal
    tier or promote flow, and storage blocks must be registered in each
    user's server database. Prefect *assets* are Cloud-gated
    lineage/observability with no reuse semantics. The artifact cache is
    therefore plain Python — usable inside Prefect flows, detached child
    processes, and external tools (e.g. Forge) alike.

Known limitations (prototype)
    - No checksums (entries are validated by file existence and size), no
      eviction policy, and no cross-process locking: concurrent misses on
      the same key duplicate work (correctly) rather than blocking.
    - Path-typed arguments are keyed by path string unless fingerprinting is
      requested per-argument.
    - The cached function must write only beneath its output directory;
      files written elsewhere are not captured.
