"""Unit tests for :mod:`cstar.orchestration.caching`.

The client functions here stand in for preprocessing steps that do not exist
yet. They are deliberately plain — they take a resource, write a file, and
return where they wrote it — because the point of the decorator is that a
producer needs to know nothing about the cache to be cached.
"""

import shutil
from pathlib import Path

import pytest

from cstar.applications.roms_marbl.models import PartitioningParameterSet
from cstar.orchestration.artifact_cache import ArtifactCache, ArtifactKind, Tier
from cstar.orchestration.caching import (
    CachedCallError,
    cache_fileset,
    cached,
    fileset_for,
    fileset_identity,
    fileset_key,
)
from cstar.orchestration.models import Resource, VersionedResource

RUN_ID = "run-abc-123"

CALLS: list[str] = []
"""Names of the client functions that actually ran, so a hit is observable."""


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    """Clear the call log before each test."""
    CALLS.clear()


@pytest.fixture
def cache(tmp_path: Path) -> ArtifactCache:
    """Return a cache wired to isolated temporary roots.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    ArtifactCache
        Cache under test.
    """
    return ArtifactCache(tmp_path / "user", tmp_path / "shared")


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """Return a scratch directory standing in for the caller's workspace.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Directory the client functions write into.
    """
    work = tmp_path / "workspace"
    work.mkdir()
    return work


@pytest.fixture
def resource() -> VersionedResource:
    """Return a hashed resource declaration.

    Returns
    -------
    VersionedResource
        Resource under test.
    """
    return VersionedResource(
        location="https://example.org/data/boundary-2010.nc", hash="9f2c4e1a"
    )


@pytest.fixture
def geometry() -> PartitioningParameterSet:
    """Return a process grid.

    Returns
    -------
    PartitioningParameterSet
        Partition geometry under test.
    """
    return PartitioningParameterSet(n_procs_x=2, n_procs_y=2)


# ---------------------------------------------------------------------------
# Client functions, as they would be written without a cache
# ---------------------------------------------------------------------------


def _fetch(resource: Resource, run_id: str, workspace: Path) -> Path:
    """Write a file derived from ``resource`` and return where it went.

    Parameters
    ----------
    resource : Resource
        Declared input.
    run_id : str
        Run identifier.
    workspace : Path
        Directory to write into.

    Returns
    -------
    Path
        The file written.
    """
    CALLS.append("fetch")
    target = workspace / "boundary.nc"
    target.write_bytes(f"derived from {resource.location}".encode())
    return target


def _partition(
    resource: Resource,
    geometry: PartitioningParameterSet,
    run_id: str,
    workspace: Path,
) -> Path:
    """Write a directory of ranks and return it.

    Parameters
    ----------
    resource : Resource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid to split across.
    run_id : str
        Run identifier.
    workspace : Path
        Directory to write into.

    Returns
    -------
    Path
        The directory of ranks.
    """
    CALLS.append("partition")
    target = workspace / f"ranks-{geometry.n_procs_x}x{geometry.n_procs_y}"
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir()
    for rank in range(geometry.n_procs_x * geometry.n_procs_y):
        (target / f"rank{rank:03d}.nc").write_bytes(f"rank {rank}".encode())
    return target


# ---------------------------------------------------------------------------
# Single-file artifacts
# ---------------------------------------------------------------------------


def test_first_call_runs_and_caches(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A cold cache runs the work and keeps the result.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    fetch = cached(cache=cache)(_fetch)

    result = fetch(resource, RUN_ID, workspace)

    assert CALLS == ["fetch"]
    assert result.is_file()
    assert cache.user_root in result.parents


def test_second_call_skips_the_work(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The whole point: identical declared inputs do not run twice.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    fetch = cached(cache=cache)(_fetch)

    first = fetch(resource, RUN_ID, workspace)
    second = fetch(resource, RUN_ID, workspace)

    assert CALLS == ["fetch"]
    assert first == second
    assert first.parent == cache.user_root / RUN_ID


def test_another_run_hits_the_shared_tier(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A second run reuses the first run's published artifact.

    The path handed back lands in run-B's own workspace rather than the shared
    tier: a caller is given a path and nothing stops it writing there, so a
    shared hit is copied down before it is exposed.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    fetch = cached(cache=cache)(_fetch)
    first = fetch(resource, RUN_ID, workspace)
    cache.promote(first.name, RUN_ID)

    result = fetch(resource, "run-B", workspace)

    assert CALLS == ["fetch"]
    assert result.parent == cache.user_root / "run-B"
    assert cache.locate(result.name, Tier.SHARED).exists


def test_localization_can_be_declined(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A read-only consumer may take the shared path and skip the copy.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    first = cached(cache=cache)(_fetch)(resource, RUN_ID, workspace)
    cache.promote(first.name, RUN_ID)

    result = cached(cache=cache, localize=False)(_fetch)(resource, "run-B", workspace)

    assert cache.shared_root in result.parents


def test_promotion_is_opt_in(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A producer does not publish; a caller decides that separately.

    Publishing puts an artifact in a space everyone shares under a name
    addressed by that name alone, which is a decision about what is worth
    keeping — not something a step should do as a side effect of running.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    cached(cache=cache)(_fetch)(resource, RUN_ID, workspace)
    assert not cache.list_shared_artifacts()

    cached(cache=cache, promote=True)(_fetch)(resource, "run-B", workspace)
    assert [loc.name for loc in cache.list_shared_artifacts()]


def test_a_changed_input_is_a_different_artifact(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The key is derived from the declaration, so changing it misses.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    fetch = cached(cache=cache)(_fetch)
    fetch(resource, RUN_ID, workspace)

    other = VersionedResource(location=str(resource.location), hash="different")
    fetch(other, RUN_ID, workspace)

    assert CALLS == ["fetch", "fetch"]


def test_context_participates_in_the_key(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A code revision the resource cannot express still invalidates.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    old = cached(cache=cache, context={"transform": "v3"})(_fetch)
    new = cached(cache=cache, context={"transform": "v4"})(_fetch)

    old(resource, RUN_ID, workspace)
    new(resource, RUN_ID, workspace)

    assert CALLS == ["fetch", "fetch"]


def test_cache_factory_is_consulted_per_call(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Production wiring resolves the cache at call time, not import time.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    calls: list[int] = []

    def factory() -> ArtifactCache:
        calls.append(1)
        return cache

    fetch = cached(cache_factory=factory)(_fetch)
    fetch(resource, RUN_ID, workspace)
    fetch(resource, RUN_ID, workspace)

    assert len(calls) == 2


# ---------------------------------------------------------------------------
# Set artifacts
# ---------------------------------------------------------------------------


def test_a_geometry_makes_the_result_a_set(
    cache: ArtifactCache,
    resource: VersionedResource,
    geometry: PartitioningParameterSet,
    workspace: Path,
) -> None:
    """A partition geometry among the arguments selects set semantics.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid.
    workspace : Path
        Directory the client writes into.
    """
    partition = cached(cache=cache)(_partition)

    result = partition(resource, geometry, RUN_ID, workspace)

    assert result.is_dir()
    assert len(sorted(result.glob("*.nc"))) == 4
    assert cache.read_manifest(RUN_ID).artifacts[result.name].kind is ArtifactKind.SET


def test_a_set_is_reused_across_runs(
    cache: ArtifactCache,
    resource: VersionedResource,
    geometry: PartitioningParameterSet,
    workspace: Path,
) -> None:
    """A second run expands the shared archive instead of repartitioning.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid.
    workspace : Path
        Directory the client writes into.
    """
    partition = cached(cache=cache)(_partition)
    first = partition(resource, geometry, RUN_ID, workspace)
    cache.promote(first.name, RUN_ID)

    result = partition(resource, geometry, "run-B", workspace)

    assert CALLS == ["partition"]
    assert len(sorted(result.glob("*.nc"))) == 4


def test_a_different_geometry_is_a_different_set(
    cache: ArtifactCache,
    resource: VersionedResource,
    geometry: PartitioningParameterSet,
    workspace: Path,
) -> None:
    """Splitting one resource two ways produces two artifacts.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid.
    workspace : Path
        Directory the client writes into.
    """
    partition = cached(cache=cache)(_partition)
    partition(resource, geometry, RUN_ID, workspace)

    partition(
        resource, PartitioningParameterSet(n_procs_x=4, n_procs_y=1), RUN_ID, workspace
    )

    assert CALLS == ["partition", "partition"]


def test_a_reused_work_directory_would_contaminate_a_set(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Only what the producer wrote for *this* call may enter the container.

    ``ingest_aggregate`` captures what it finds and declares a member count
    matching it, so surplus files left by an earlier call pass every
    completeness check and are then promoted and shared. Nothing in the cache
    can detect that: it cannot know which files were meant. Producing into a
    directory scoped to the call is the client's side of that contract, and
    this fails loudly if the fixture stops honouring it.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    partition = cached(cache=cache)(_partition)

    wide = partition(
        resource, PartitioningParameterSet(n_procs_x=4, n_procs_y=2), RUN_ID, workspace
    )
    narrow = partition(
        resource, PartitioningParameterSet(n_procs_x=2, n_procs_y=1), RUN_ID, workspace
    )

    assert len(sorted(wide.glob("*.nc"))) == 8
    assert len(sorted(narrow.glob("*.nc"))) == 2


def test_a_set_does_not_collide_with_the_single_file(
    cache: ArtifactCache,
    resource: VersionedResource,
    geometry: PartitioningParameterSet,
    workspace: Path,
) -> None:
    """The derived set occupies its own key space.

    A collision here would not waste an entry but corrupt one, since the two
    are different shapes on disk.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid.
    workspace : Path
        Directory the client writes into.
    """
    single = cached(cache=cache)(_fetch)(resource, RUN_ID, workspace)
    plural = cached(cache=cache)(_partition)(resource, geometry, RUN_ID, workspace)

    assert single.name != plural.name
    assert single.is_file()
    assert plural.is_dir()


def test_a_pre_partitioned_source_keeps_the_ordinary_strategy(
    cache: ArtifactCache,
    geometry: PartitioningParameterSet,
    workspace: Path,
) -> None:
    """A resource that arrives split is described by its geometry, not derived.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    geometry : PartitioningParameterSet
        Process grid.
    workspace : Path
        Directory the client writes into.
    """
    already = VersionedResource.model_validate(
        {
            "location": "https://example.org/d/split.nc",
            "hash": "abc",
            "partitioned": True,
        }
    )

    result = cached(cache=cache)(_partition)(already, geometry, RUN_ID, workspace)

    assert result.is_dir()
    assert not result.name.endswith(".set")


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_a_function_without_a_resource_cannot_be_cached(cache: ArtifactCache) -> None:
    """There is no key without a declared input.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    """

    def produce(run_id: str) -> Path:
        """Return a path, taking no resource.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        Path
            Unused.
        """
        return Path("unused")

    with pytest.raises(CachedCallError, match="exactly one Resource"):
        cached(cache=cache)(produce)(RUN_ID)


def test_two_resources_are_ambiguous(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Guessing which input identifies the output returns wrong artifacts.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """

    def combine(first: Resource, second: Resource, run_id: str) -> Path:
        """Return a path, taking two resources.

        Parameters
        ----------
        first : Resource
            One input.
        second : Resource
            Another input.
        run_id : str
            Run identifier.

        Returns
        -------
        Path
            Unused.
        """
        return Path("unused")

    with pytest.raises(CachedCallError, match="found 2"):
        cached(cache=cache)(combine)(resource, resource, RUN_ID)


def test_a_missing_run_id_is_refused(
    cache: ArtifactCache, resource: VersionedResource
) -> None:
    """The user tier is addressed by run, so there is nowhere to write.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    """

    def produce(resource: Resource) -> Path:
        """Return a path, taking no run identifier.

        Parameters
        ----------
        resource : Resource
            Declared input.

        Returns
        -------
        Path
            Unused.
        """
        return Path("unused")

    with pytest.raises(CachedCallError, match="addressed by run"):
        cached(cache=cache)(produce)(resource)


def test_producing_a_directory_without_a_geometry_is_refused(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The key already said this was a single file.

    Publishing a directory under it would put a set behind a name every reader
    treats as a file.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """

    def produce(resource: Resource, run_id: str) -> Path:
        """Return a directory despite declaring no geometry.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.

        Returns
        -------
        Path
            A directory.
        """
        target = workspace / "unexpected"
        target.mkdir(exist_ok=True)
        (target / "a.nc").write_bytes(b"a")
        return target

    with pytest.raises(CachedCallError, match="names a single file"):
        cached(cache=cache)(produce)(resource, RUN_ID)


def test_producing_a_file_for_a_set_is_refused(
    cache: ArtifactCache,
    resource: VersionedResource,
    geometry: PartitioningParameterSet,
    workspace: Path,
) -> None:
    """A geometry promised a set, so a lone file is a defect.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid.
    workspace : Path
        Directory the client writes into.
    """

    def produce(
        resource: Resource, geometry: PartitioningParameterSet, run_id: str
    ) -> Path:
        """Return one file despite declaring a geometry.

        Parameters
        ----------
        resource : Resource
            Declared input.
        geometry : PartitioningParameterSet
            Process grid.
        run_id : str
            Run identifier.

        Returns
        -------
        Path
            A single file.
        """
        target = workspace / "lonely.nc"
        target.write_bytes(b"alone")
        return target

    with pytest.raises(CachedCallError, match="names a set"):
        cached(cache=cache)(produce)(resource, geometry, RUN_ID)


def test_a_vanished_result_is_refused(
    cache: ArtifactCache, resource: VersionedResource
) -> None:
    """A path that does not exist is not a result.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    """

    def produce(resource: Resource, run_id: str) -> Path:
        """Return a path that was never written.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.

        Returns
        -------
        Path
            A path to nowhere.
        """
        return Path("/nonexistent/nowhere.nc")

    with pytest.raises(CachedCallError, match="does not exist"):
        cached(cache=cache)(produce)(resource, RUN_ID)


def test_exactly_one_cache_source_is_required() -> None:
    """Passing both or neither leaves it undefined which cache is used."""
    with pytest.raises(ValueError, match="exactly one"):
        cached()
    with pytest.raises(ValueError, match="exactly one"):
        cached(cache=None, cache_factory=None)


# ---------------------------------------------------------------------------
# Non-intrusiveness
# ---------------------------------------------------------------------------


def test_decoration_preserves_the_function(cache: ArtifactCache) -> None:
    """A decorated producer still looks and reads like the original.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    """
    decorated = cached(cache=cache)(_fetch)

    assert decorated.__name__ == _fetch.__name__
    assert decorated.__doc__ == _fetch.__doc__


def test_the_undecorated_function_is_unaffected(
    resource: VersionedResource, workspace: Path
) -> None:
    """The producer knows nothing about the cache and is testable alone.

    Parameters
    ----------
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    result = _fetch(resource, RUN_ID, workspace)

    assert result == workspace / "boundary.nc"
    assert result.is_file()


def test_a_hit_returns_the_cached_path_not_the_workspace_path(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The caller receives the cache's copy, which is what skipping work means.

    Worth stating explicitly: a caller that assumed the returned path was the
    one it passed in would be surprised.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    result = cached(cache=cache)(_fetch)(resource, RUN_ID, workspace)

    assert result != workspace / "boundary.nc"
    assert result.read_bytes() == (workspace / "boundary.nc").read_bytes()


def test_keyword_arguments_bind_the_same_way(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Calling by keyword must key identically to calling positionally.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    fetch = cached(cache=cache)(_fetch)

    positional = fetch(resource, RUN_ID, workspace)
    keyword = fetch(resource=resource, run_id=RUN_ID, workspace=workspace)

    assert CALLS == ["fetch"]
    assert positional == keyword


def test_the_shared_copy_is_verifiable(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Verification is on by default, so a promoted artifact checks out.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    result = cached(cache=cache)(_fetch)(resource, RUN_ID, workspace)
    cache.promote(result.name, RUN_ID)

    assert cache.verify(result.name) is True
    assert cache.locate(result.name, Tier.SHARED).exists


def test_a_key_error_surfaces_as_a_cached_call_error(
    cache: ArtifactCache, workspace: Path
) -> None:
    """A resource that cannot be keyed fails as a caching problem, not a key one.

    The caller decorated a function; the vocabulary of the error should match.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    workspace : Path
        Directory the client writes into.
    """
    unkeyable = Resource.model_validate(
        {"location": "https://example.org/d/x.nc", "partitioned": True}
    )

    with pytest.raises(CachedCallError, match="describing how it is split"):
        cached(cache=cache)(_fetch)(unkeyable, RUN_ID, workspace)


def test_two_geometries_are_ambiguous(
    cache: ArtifactCache,
    resource: VersionedResource,
    geometry: PartitioningParameterSet,
) -> None:
    """Two process grids leave it undefined which one shapes the output.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid.
    """

    def produce(
        resource: Resource,
        first: PartitioningParameterSet,
        second: PartitioningParameterSet,
        run_id: str,
    ) -> Path:
        """Return a path, taking two geometries.

        Parameters
        ----------
        resource : Resource
            Declared input.
        first : PartitioningParameterSet
            One grid.
        second : PartitioningParameterSet
            Another grid.
        run_id : str
            Run identifier.

        Returns
        -------
        Path
            Unused.
        """
        return Path("unused")

    with pytest.raises(CachedCallError, match="ambiguous"):
        cached(cache=cache)(produce)(resource, geometry, geometry, RUN_ID)


# ---------------------------------------------------------------------------
# File sets
# ---------------------------------------------------------------------------


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """Return a directory holding a mix of files the wildcard must filter.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Directory to discover.
    """
    root = tmp_path / "tree"
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_text("alpha")
    (root / "b.csv").write_text("beta")
    (root / "sub" / "c.txt").write_text("gamma")
    return root


def test_wildcard_selects_the_members(tree: Path) -> None:
    """Only what the pattern matches is described.

    Parameters
    ----------
    tree : Path
        Directory to discover.
    """
    assert fileset_for(tree, "*.txt").members == ("a.txt", "sub/c.txt")


def test_no_wildcard_takes_everything(tree: Path) -> None:
    """The default is every file beneath the directory.

    Parameters
    ----------
    tree : Path
        Directory to discover.
    """
    assert fileset_for(tree).members == ("a.txt", "b.csv", "sub/c.txt")


def test_members_keep_their_nesting(tree: Path) -> None:
    """Relative paths, not flattened names, so a tree survives the round trip.

    Parameters
    ----------
    tree : Path
        Directory to discover.
    """
    assert "sub/c.txt" in fileset_for(tree, "*.txt").members


def test_excluded_files_never_reach_the_cache(cache: ArtifactCache, tree: Path) -> None:
    """A file outside the set must not be swept in by proximity.

    Excluded explicitly through ``members=`` rather than by omission: the
    container is built from the set's own list, so anything else under the
    same root is left behind.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    tree : Path
        Directory to discover.
    """
    location = cache_fileset(cache, fileset_for(tree, "*.txt"), RUN_ID)

    stored = sorted(
        str(item.relative_to(location.path))
        for item in location.path.rglob("*")
        if item.is_file() and not item.name.startswith(".")
    )
    assert stored == ["a.txt", "sub/c.txt"]


def test_a_file_set_is_content_addressed(cache: ArtifactCache, tree: Path) -> None:
    """Editing a member is a different artifact.

    A file set has no declaration to key on — the caller has files already —
    so its identity is what they contain. Keying on names alone would let two
    unrelated directories serve each other's data.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    tree : Path
        Directory to discover.
    """
    before = fileset_key(fileset_for(tree, "*.txt"))
    (tree / "a.txt").write_text("changed")

    assert fileset_key(fileset_for(tree, "*.txt")) != before


def test_the_root_is_not_part_of_the_identity(tmp_path: Path, tree: Path) -> None:
    """The same files under two parents are one artifact.

    Otherwise the key would be machine-specific and useless in the shared
    tier.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    tree : Path
        Directory to discover.
    """
    elsewhere = tmp_path / "elsewhere"
    shutil.copytree(tree, elsewhere)

    assert fileset_identity(fileset_for(elsewhere, "*.txt")) == fileset_identity(
        fileset_for(tree, "*.txt")
    )


def test_the_wildcard_is_not_part_of_the_identity(tree: Path) -> None:
    """Two patterns selecting the same files produce the same artifact.

    Parameters
    ----------
    tree : Path
        Directory to discover.
    """
    assert fileset_identity(fileset_for(tree, "*.csv")) == fileset_identity(
        fileset_for(tree, "b.csv")
    )


def test_moving_a_member_changes_the_set(tmp_path: Path, tree: Path) -> None:
    """Paths are identifying, since a consumer globbing the container sees them.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    tree : Path
        Directory to discover.
    """
    before = fileset_for(tree, "*.txt").content_digest
    (tree / "sub" / "c.txt").rename(tree / "c.txt")

    assert fileset_for(tree, "*.txt").content_digest != before


def test_a_file_set_is_stored_as_a_set(cache: ArtifactCache, tree: Path) -> None:
    """The container is one artifact, not a file per member.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    tree : Path
        Directory to discover.
    """
    location = cache_fileset(cache, fileset_for(tree, "*.txt"), RUN_ID)
    record = cache.read_manifest(RUN_ID).artifacts[location.name]

    assert record.kind is ArtifactKind.SET
    assert location.name.endswith(".set")
    assert cache.verify(location.name, RUN_ID) is True


def test_caching_a_file_set_is_idempotent(cache: ArtifactCache, tree: Path) -> None:
    """Identical contents are the same artifact, so the second call is a hit.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    tree : Path
        Directory to discover.
    """
    first = cache_fileset(cache, fileset_for(tree, "*.txt"), RUN_ID)
    second = cache_fileset(cache, fileset_for(tree, "*.txt"), RUN_ID)

    assert first.path == second.path


def test_a_promoted_file_set_is_reused_across_runs(
    cache: ArtifactCache, tree: Path
) -> None:
    """Another run expands the shared archive rather than re-storing.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    tree : Path
        Directory to discover.
    """
    fileset = fileset_for(tree, "*.txt")
    cache_fileset(cache, fileset, RUN_ID, promote=True)

    shutil.rmtree(tree)
    location = cache_fileset(cache, fileset, "run-B")

    assert sorted(item.name for item in location.path.glob("*.txt")) == ["a.txt"]


def test_a_custom_name_sets_the_readable_stem(cache: ArtifactCache, tree: Path) -> None:
    """The stem is for humans reading a cache listing.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    tree : Path
        Directory to discover.
    """
    key = fileset_key(fileset_for(tree, "*.txt"), name="inputs")

    assert key.startswith("inputs-")


def test_an_empty_selection_is_refused(tree: Path) -> None:
    """Every empty set would key alike, so this is a mistake rather than a no-op.

    Parameters
    ----------
    tree : Path
        Directory to discover.
    """
    with pytest.raises(ValueError, match="no files matched"):
        fileset_for(tree, "*.nope")


def test_a_missing_directory_is_refused(tmp_path: Path) -> None:
    """Discovery cannot describe what is not there.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    with pytest.raises(FileNotFoundError):
        fileset_for(tmp_path / "absent")
