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
from cstar.orchestration.cache_keys import aggregate_key, resource_key
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


def _fetch(resource: Resource, run_id: str, destination: Path) -> None:
    """Write a file derived from ``resource`` at the path it was given.

    Parameters
    ----------
    resource : Resource
        Declared input.
    run_id : str
        Run identifier.
    destination : Path
        Path to write to.
    """
    CALLS.append("fetch")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(f"derived from {resource.location}".encode())


def _partition(
    resource: Resource,
    geometry: PartitioningParameterSet,
    run_id: str,
    destination: Path,
) -> None:
    """Write a directory of ranks.

    Parameters
    ----------
    resource : Resource
        Declared input.
    geometry : PartitioningParameterSet
        Process grid to split across.
    run_id : str
        Run identifier.
    destination : Path
        Directory to write into.
    """
    CALLS.append("partition")
    shutil.rmtree(destination, ignore_errors=True)
    destination.mkdir(parents=True)
    assert geometry.n_procs_x
    assert geometry.n_procs_y
    for rank in range(geometry.n_procs_x * geometry.n_procs_y):
        (destination / f"rank{rank:03d}.nc").write_bytes(f"rank {rank}".encode())


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

    result = fetch(resource, RUN_ID, workspace / "boundary.nc")

    assert CALLS == ["fetch"]
    assert result == workspace / "boundary.nc"
    assert result.is_file()
    assert cache.resolve(resource_key(resource), RUN_ID) is not None


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

    first = fetch(resource, RUN_ID, workspace / "boundary.nc")
    second = fetch(resource, RUN_ID, workspace / "boundary.nc")

    assert CALLS == ["fetch"]
    assert first == second == workspace / "boundary.nc"


def test_another_run_hits_the_shared_tier(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A second run reuses the first run's published artifact.

    The path handed back is the one run-B asked for, never a path into the
    cache: a caller given a cache path has nothing stopping it writing there,
    and one client editing a shared artifact in place corrupts it for every
    other run on the allocation.

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
    fetch(resource, RUN_ID, workspace / "boundary.nc")
    key = resource_key(resource)
    cache.promote(key, RUN_ID)

    result = fetch(resource, "run-B", workspace / "b" / "boundary.nc")

    assert CALLS == ["fetch"]
    assert result == workspace / "b" / "boundary.nc"
    assert result.read_bytes() == (workspace / "boundary.nc").read_bytes()
    assert cache.user_root not in result.parents
    assert cache.shared_root not in result.parents


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
    cached(cache=cache)(_fetch)(resource, RUN_ID, workspace / "boundary.nc")
    assert not cache.list_shared_artifacts()

    cached(cache=cache, promote=True)(_fetch)(
        resource, "run-B", workspace / "b" / "boundary.nc"
    )
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
    fetch(resource, RUN_ID, workspace / "boundary.nc")

    other = VersionedResource(location=str(resource.location), hash="different")
    fetch(other, RUN_ID, workspace / "other.nc")

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

    old(resource, RUN_ID, workspace / "old.nc")
    new(resource, RUN_ID, workspace / "new.nc")

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
    fetch(resource, RUN_ID, workspace / "boundary.nc")
    fetch(resource, RUN_ID, workspace / "boundary.nc")

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

    result = partition(resource, geometry, RUN_ID, workspace / "ranks")

    assert result.is_dir()
    assert len(sorted(result.glob("*.nc"))) == 4
    key = aggregate_key(resource, geometry)
    assert result == workspace / "ranks"
    assert cache.read_manifest(RUN_ID).artifacts[key].kind is ArtifactKind.SET


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
    partition(resource, geometry, RUN_ID, workspace / "ranks")
    cache.promote(aggregate_key(resource, geometry), RUN_ID)

    result = partition(resource, geometry, "run-B", workspace / "b-ranks")

    assert CALLS == ["partition"]
    assert result == workspace / "b-ranks"
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
    partition(resource, geometry, RUN_ID, workspace / "ranks")

    partition(
        resource,
        PartitioningParameterSet(n_procs_x=4, n_procs_y=1),
        RUN_ID,
        workspace / "alt-ranks",
    )

    assert CALLS == ["partition", "partition"]


def test_reusing_one_destination_for_two_sets_leaves_no_remnants(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A destination holding a larger set is replaced, not merged into.

    Copying a set over a directory that already holds one would otherwise
    leave the surplus members of the first behind, and a consumer globbing the
    directory would read them as part of the second — a set that passes every
    completeness check while containing files that were never in it.

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
    target = workspace / "ranks"

    wide = partition(
        resource, PartitioningParameterSet(n_procs_x=4, n_procs_y=2), RUN_ID, target
    )
    assert len(sorted(wide.glob("*.nc"))) == 8

    narrow = partition(
        resource, PartitioningParameterSet(n_procs_x=2, n_procs_y=1), RUN_ID, target
    )

    assert narrow == target
    assert sorted(item.name for item in narrow.glob("*.nc")) == [
        "rank000.nc",
        "rank001.nc",
    ]


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
    single = cached(cache=cache)(_fetch)(resource, RUN_ID, workspace / "boundary.nc")
    plural = cached(cache=cache)(_partition)(
        resource, geometry, RUN_ID, workspace / "ranks"
    )

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

    result = cached(cache=cache)(_partition)(
        already, geometry, RUN_ID, workspace / "ranks"
    )

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

    def produce(resource: Resource, run_id: str, destination: Path) -> None:
        """Write a directory despite declaring no geometry.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.
        destination : Path
            Path to write to.
        """
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "a.nc").write_bytes(b"a")

    with pytest.raises(CachedCallError, match="names a single file"):
        cached(cache=cache)(produce)(resource, RUN_ID, workspace / "unexpected")


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
        resource: Resource,
        geometry: PartitioningParameterSet,
        run_id: str,
        destination: Path,
    ) -> None:
        """Write one file despite declaring a geometry.

        Parameters
        ----------
        resource : Resource
            Declared input.
        geometry : PartitioningParameterSet
            Process grid.
        run_id : str
            Run identifier.
        destination : Path
            Path to write to.
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"alone")

    with pytest.raises(CachedCallError, match="names a set"):
        cached(cache=cache)(produce)(
            resource, geometry, RUN_ID, workspace / "lonely.nc"
        )


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

    def produce(resource: Resource, run_id: str, destination: Path) -> None:
        """Write nothing, leaving the destination empty.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.
        destination : Path
            Path it was told to write to, and does not.
        """
        return

    with pytest.raises(CachedCallError, match="does not exist"):
        cached(cache=cache)(produce)(resource, RUN_ID, Path("/nonexistent/nowhere.nc"))


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
    _fetch(resource, RUN_ID, workspace / "boundary.nc")

    assert (workspace / "boundary.nc").is_file()


def test_the_caller_never_receives_a_cache_path(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The returned path is always the one the caller asked for.

    Handing back a path into the cache invites a client to write through it,
    and one client editing an artifact in place corrupts it for everyone else
    reading that key.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """
    result = cached(cache=cache)(_fetch)(resource, RUN_ID, workspace / "boundary.nc")

    assert result == workspace / "boundary.nc"
    assert cache.user_root not in result.parents
    assert cache.shared_root not in result.parents


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

    positional = fetch(resource, RUN_ID, workspace / "boundary.nc")
    keyword = fetch(
        resource=resource, run_id=RUN_ID, destination=workspace / "boundary.nc"
    )

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
    cached(cache=cache)(_fetch)(resource, RUN_ID, workspace / "boundary.nc")
    key = resource_key(resource)
    cache.promote(key, RUN_ID)

    assert cache.verify(key) is True
    assert cache.locate(key, Tier.SHARED).exists


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


def test_editing_a_member_does_not_change_the_key(
    cache: ArtifactCache, tree: Path
) -> None:
    """The accepted weakness, pinned so it stays a decision rather than a bug.

    A file set is keyed on where its members live, not what they contain, so
    an in-place edit is invisible and the cache keeps serving what it stored.
    Catching it would mean reading every byte before any lookup could answer.
    Anything whose contents change under a stable path belongs behind a
    declaration, not here.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    tree : Path
        Directory to discover.
    """
    before = fileset_key(fileset_for(tree, "*.txt"))
    (tree / "a.txt").write_text("changed")

    assert fileset_key(fileset_for(tree, "*.txt")) == before


def test_adding_a_member_changes_the_key(tree: Path) -> None:
    """What *is* caught: the selection itself changing.

    Parameters
    ----------
    tree : Path
        Directory to discover.
    """
    before = fileset_key(fileset_for(tree, "*.txt"))
    (tree / "d.txt").write_text("delta")

    assert fileset_key(fileset_for(tree, "*.txt")) != before


def test_the_root_is_part_of_the_identity(tmp_path: Path, tree: Path) -> None:
    """The same filenames under two directories are two artifacts.

    Absolute paths are what make this sound on a shared filesystem: including
    the containing directory is what stops two unrelated directories that
    happen to share filenames from serving each other's data.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    tree : Path
        Directory to discover.
    """
    elsewhere = tmp_path / "elsewhere"
    shutil.copytree(tree, elsewhere)

    assert fileset_identity(fileset_for(elsewhere, "*.txt")) != fileset_identity(
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
    """Where a member sits is identifying, not just that it exists.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    tree : Path
        Directory to discover.
    """
    before = fileset_for(tree, "*.txt").path_digest
    (tree / "sub" / "c.txt").rename(tree / "c.txt")

    assert fileset_for(tree, "*.txt").path_digest != before


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


def test_discovery_never_reads_a_member(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keying on paths means keying costs a stat, not a pass over the data.

    This is the point of the trade. A set whose members are gigabytes is
    keyed, and therefore looked up, without any of them being opened; on a hit
    nothing is read at all. Pinned here because the guarantee is easy to lose
    to an innocuous-looking change, and losing it would be silent.

    Parameters
    ----------
    tree : Path
        Directory to discover.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to make any read fail loudly.
    """

    def refuse(self: Path, *args: object, **kwargs: object) -> object:
        """Fail rather than open a member.

        Parameters
        ----------
        self : Path
            Path being opened.
        *args : object
            Ignored.
        **kwargs : object
            Ignored.

        Returns
        -------
        object
            Never returns.
        """
        raise AssertionError(f"read the contents of {self}")

    monkeypatch.setattr(Path, "open", refuse)

    assert fileset_for(tree, "*.txt").members == ("a.txt", "sub/c.txt")


def test_repeating_a_hit_replaces_the_destination(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Asking twice is a no-op, not an error, even with the file already there.

    Re-running a step in one workspace is ordinary, so a destination that
    already holds the artifact is overwritten rather than refused.

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
    fetch(resource, RUN_ID, workspace / "boundary.nc")
    cache.promote(resource_key(resource), RUN_ID)

    target = workspace / "b" / "boundary.nc"
    once = fetch(resource, "run-B", target)
    target.write_bytes(b"clobbered by something else")
    twice = fetch(resource, "run-B", target)

    assert once == twice == target
    assert target.read_bytes() == (workspace / "boundary.nc").read_bytes()


# ---------------------------------------------------------------------------
# Destination contract
# ---------------------------------------------------------------------------


def test_a_producer_without_a_destination_cannot_be_cached(
    cache: ArtifactCache, resource: VersionedResource
) -> None:
    """On a hit the function is not called, so it cannot choose the path.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    """

    def produce(resource: Resource, run_id: str) -> None:
        """Take no destination.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.
        """
        return

    with pytest.raises(CachedCallError, match="argument naming the path"):
        cached(cache=cache)(produce)(resource, RUN_ID)


def test_the_destination_argument_can_be_renamed(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Producers that already name the parameter something else still work.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """

    def produce(resource: Resource, run_id: str, out: Path) -> None:
        """Write to a differently named destination parameter.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.
        out : Path
            Path to write to.
        """
        out.write_bytes(b"payload")

    target = workspace / "renamed.nc"
    result = cached(cache=cache, destination_argument="out")(produce)(
        resource, RUN_ID, target
    )

    assert result == target


def test_a_producer_writing_elsewhere_is_refused(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """Writing somewhere other than the destination breaks hit/miss symmetry.

    Detected by reading the destination rather than by trusting anything the
    producer says, which is the point of the producer returning nothing: the
    check does not depend on the producer being honest about what it did.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """

    def produce(resource: Resource, run_id: str, destination: Path) -> None:
        """Write somewhere other than where it was told.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.
        destination : Path
            Path it was told to write to, and ignores.
        """
        (workspace / "elsewhere.nc").write_bytes(b"payload")

    with pytest.raises(CachedCallError, match="does not exist"):
        cached(cache=cache)(produce)(resource, RUN_ID, workspace / "asked-for.nc")


def test_a_hit_creates_missing_parent_directories(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The destination's directory may not exist yet on the consuming run.

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
    fetch(resource, RUN_ID, workspace / "boundary.nc")
    cache.promote(resource_key(resource), RUN_ID)

    deep = workspace / "a" / "b" / "c" / "boundary.nc"
    result = fetch(resource, "run-B", deep)

    assert result == deep
    assert deep.is_file()


def test_a_set_delivered_over_a_file_is_refused(
    cache: ArtifactCache,
    resource: VersionedResource,
    geometry: PartitioningParameterSet,
    workspace: Path,
) -> None:
    """A shape mismatch at the destination is reported, not forced.

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
    partition(resource, geometry, RUN_ID, workspace / "ranks")
    cache.promote(aggregate_key(resource, geometry), RUN_ID)

    occupied = workspace / "occupied"
    occupied.write_bytes(b"a file is in the way")

    with pytest.raises(CachedCallError, match="needs a directory"):
        partition(resource, geometry, "run-B", occupied)


def test_a_file_delivered_over_a_directory_is_refused(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The inverse mismatch is reported too.

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
    fetch(resource, RUN_ID, workspace / "boundary.nc")
    cache.promote(resource_key(resource), RUN_ID)

    occupied = workspace / "occupied"
    occupied.mkdir()

    with pytest.raises(CachedCallError, match="single file"):
        fetch(resource, "run-B", occupied)


def test_the_cached_copy_survives_editing_the_delivered_file(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """The point of the whole change: the caller cannot reach the cache.

    A client that edits what it was handed must not damage the artifact every
    other run reads under that key.

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
    delivered = fetch(resource, RUN_ID, workspace / "boundary.nc")
    key = resource_key(resource)
    cache.promote(key, RUN_ID)

    delivered.write_bytes(b"a client scribbled here")

    assert cache.verify(key) is True
    assert cache.verify(key, RUN_ID, prefer_local=True) is True


def test_a_destination_that_is_the_cache_path_is_a_no_op(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A caller that hands back the cache's own path must not corrupt it.

    ``shutil.copy2`` raises on a self-copy, and any implementation that opened
    the destination for writing first would truncate the artifact before
    discovering it was the source.

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
    fetch(resource, RUN_ID, workspace / "boundary.nc")
    key = resource_key(resource)

    cached_path = cache.locate(key, Tier.USER, RUN_ID).path
    result = fetch(resource, RUN_ID, cached_path)

    assert result == cached_path
    assert cache.verify(key, RUN_ID, prefer_local=True) is True


def test_a_producer_may_return_anything_and_it_is_ignored(
    cache: ArtifactCache, resource: VersionedResource, workspace: Path
) -> None:
    """A producer with other callers keeps its own return type.

    The decorator promises not to look at it. That promise is what makes it
    safe to decorate a function that already exists and already returns
    something useful to somebody else, without writing a wrapper.

    On a hit the producer is not called at all, so any such value is
    unobtainable rather than merely discarded — which is why the caller is
    given the destination and never a producer's result.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    resource : VersionedResource
        Declared input.
    workspace : Path
        Directory the client writes into.
    """

    def produce(resource: Resource, run_id: str, destination: Path) -> dict[str, int]:
        """Write the artifact and return a summary its other callers want.

        Parameters
        ----------
        resource : Resource
            Declared input.
        run_id : str
            Run identifier.
        destination : Path
            Path to write to.

        Returns
        -------
        dict of str to int
            Ignored by the decorator.
        """
        destination.write_bytes(b"payload")
        return {"bytes_written": 7}

    target = workspace / "summarised.nc"
    result = cached(cache=cache)(produce)(resource, RUN_ID, target)

    assert result == target
    assert produce(resource, RUN_ID, target) == {"bytes_written": 7}
