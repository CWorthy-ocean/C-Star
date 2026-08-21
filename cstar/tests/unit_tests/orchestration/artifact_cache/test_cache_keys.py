"""Unit tests for :mod:`cstar.orchestration.cache_keys`.

Exercised against the real :class:`~cstar.orchestration.models.Resource` and
:class:`~cstar.orchestration.models.VersionedResource` models, using the
declarations that appear in the shipped ROMS/MARBL blueprint template.
"""

from dataclasses import dataclass
from pathlib import Path

import pytest

from cstar.applications.roms_marbl.cache import partition_identity, with_partitioning
from cstar.applications.roms_marbl.models import PartitioningParameterSet
from cstar.orchestration import cache_keys
from cstar.orchestration.cache_keys import (
    AGGREGATE_SUFFIX,
    DIGEST_LENGTH,
    KEY_SCHEME_VERSION,
    CacheKeyError,
    DynamicCacheKeyGenerator,
    aggregate_key,
    generator_for,
    hash_identity,
    identity_for,
    is_registered,
    location_identity,
    readable_parts,
    register_identity,
    resource_key,
)
from cstar.orchestration.models import DataResource, Resource, VersionedResource


@pytest.fixture
def versioned() -> VersionedResource:
    """Return a hashed resource as declared under ``forcing.boundary.data``.

    Returns
    -------
    VersionedResource
        Resource under test.
    """
    return VersionedResource(location="http://mockdoc.com/partitioning1.nc", hash="abc")


@pytest.fixture
def geometry() -> PartitioningParameterSet:
    """Return the process grid declared under ``partitioning`` in the template.

    Returns
    -------
    PartitioningParameterSet
        Partition geometry under test.
    """
    return PartitioningParameterSet(n_procs_x=16, n_procs_y=8)


@pytest.fixture
def plain() -> Resource:
    """Return an unhashed resource as declared under ``grid.data``.

    Returns
    -------
    Resource
        Resource under test.
    """
    return Resource(location="http://mockdoc.com/grid")


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("subject", "scheme"),
    [(VersionedResource, "hash"), (Resource, "location")],
)
def test_each_subject_resolves_to_its_scheme(subject: type, scheme: str) -> None:
    """Every derivation names its key space, so two can never collide.

    Parameters
    ----------
    subject : type
        Shape being keyed.
    scheme : str
        Key space it belongs to.
    """
    assert generator_for(subject).scheme == scheme


def test_schemes_produce_different_keys(versioned: VersionedResource) -> None:
    """The same resource keyed two ways yields two distinct artifacts.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    """
    by_hash = generator_for(VersionedResource)
    by_location = generator_for(Resource)

    location = str(versioned.location)

    assert by_hash.key_for(versioned, location) != by_location.key_for(
        versioned, location
    )


def test_an_unregistered_subject_is_refused() -> None:
    """Nothing can be keyed until somebody says how it is identified."""
    with pytest.raises(CacheKeyError, match="nothing registered"):
        generator_for(complex)


def test_a_value_passed_where_a_type_belongs_is_refused() -> None:
    """A value would register under something no lookup can reproduce."""
    with pytest.raises(CacheKeyError, match="must be a type"):
        generator_for(Resource(location="http://h/x.nc"))  # type: ignore[arg-type]


def test_re_registering_a_shape_is_refused() -> None:
    """Replacing an entry changes what every key derived through it means."""
    with pytest.raises(CacheKeyError, match="already registered"):
        register_identity(Resource, "other", location_identity)


def test_a_subclass_inherits_its_base_registration() -> None:
    """Resolution walks the MRO, so a new subclass keys like what it extends."""

    class Mirrored(Resource):
        """A resource subclass registering nothing of its own."""

    assert generator_for(Mirrored).scheme == "location"


# ---------------------------------------------------------------------------
# Key shape
# ---------------------------------------------------------------------------


def test_key_is_filesystem_safe(versioned: VersionedResource) -> None:
    """A key is used directly as an artifact name, so it must be a bare filename."""
    key = resource_key(versioned)
    assert "/" not in key
    assert key not in {".", ".."}
    assert key


def test_key_keeps_the_source_name_and_extension(
    versioned: VersionedResource,
) -> None:
    """A human listing the cache should be able to tell what they are seeing."""
    key = resource_key(versioned)
    assert key.startswith("partitioning1-")
    assert key.endswith(".nc")


def test_key_embeds_a_truncated_digest(versioned: VersionedResource) -> None:
    """The digest carries the identity; its length is part of the format."""
    key = resource_key(versioned)
    digest = key.removeprefix("partitioning1-").removesuffix(".nc")
    assert len(digest) == DIGEST_LENGTH
    assert all(character in "0123456789abcdef" for character in digest)


def test_extensionless_location_yields_a_bare_key(plain: Resource) -> None:
    """A location with no suffix still produces a usable name."""
    key = resource_key(plain)
    assert key.startswith("grid-")
    assert "." not in key.removeprefix("grid-")


def test_key_is_deterministic(versioned: VersionedResource) -> None:
    """Two calls on equal inputs agree, or lookups could never hit."""
    assert resource_key(versioned) == resource_key(versioned)


# ---------------------------------------------------------------------------
# Hash strategy
# ---------------------------------------------------------------------------


def test_hash_key_ignores_which_mirror_serves_the_file() -> None:
    """Identical content behind two hosts should share one cached artifact."""
    left = VersionedResource(location="http://mirror-a.example/x.nc", hash="abc")
    right = VersionedResource(location="http://mirror-b.example/x.nc", hash="abc")

    assert resource_key(left) == resource_key(right)


def test_hash_key_changes_with_the_hash() -> None:
    """New upstream data must not be served from the old cache entry."""
    before = VersionedResource(location="http://h/x.nc", hash="abc")
    after = VersionedResource(location="http://h/x.nc", hash="def")

    assert resource_key(before) != resource_key(after)


def test_hash_key_separates_partitioned_data(
    geometry: PartitioningParameterSet,
) -> None:
    """A pre-partitioned resource is physically different data."""
    whole = VersionedResource(location="http://h/x.nc", hash="abc")
    split = VersionedResource.model_validate(
        {"location": "http://h/x.nc", "hash": "abc", "partitioned": True}
    )

    assert resource_key(whole) != resource_key(split, companion=geometry)


def test_hash_key_distinguishes_renamed_content() -> None:
    """The filename is part of the key, so a rename caches separately.

    A deliberate trade: legible cache listings in exchange for occasionally
    storing one payload twice.
    """
    left = VersionedResource(location="http://h/x.nc", hash="abc")
    right = VersionedResource(location="http://h/y.nc", hash="abc")

    assert resource_key(left) != resource_key(right)


def test_hash_strategy_refuses_an_unhashed_resource(plain: Resource) -> None:
    """Without a hash this strategy cannot identify the content, so it says so."""
    with pytest.raises(CacheKeyError, match="declares no hash"):
        hash_identity(plain)


# ---------------------------------------------------------------------------
# Location strategy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "equivalent",
    [
        "http://mockdoc.com/a/x.nc",
        "http://MockDoc.com/a/x.nc",
        "HTTP://mockdoc.com/a/x.nc",
        "http://mockdoc.com:80/a/x.nc",
        "http://mockdoc.com/a/x.nc#section",
        "http://mockdoc.com/a/x.nc/",
    ],
)
def test_location_normalisation_collapses_equivalent_urls(equivalent: str) -> None:
    """Trivially different spellings of one URL must not split the cache."""
    canonical = Resource(location="http://mockdoc.com/a/x.nc")

    assert resource_key(Resource(location=equivalent)) == resource_key(canonical)


def test_location_keeps_the_query_string() -> None:
    """A query often selects which content is served, so it is significant."""
    assert resource_key(Resource(location="http://h/x.nc?version=1")) != resource_key(
        Resource(location="http://h/x.nc?version=2")
    )


def test_location_key_separates_partitioned_data(
    geometry: PartitioningParameterSet,
) -> None:
    """Partitioning applies to unhashed resources too."""
    whole = Resource(location="http://mockdoc.com/grid")
    split = Resource.model_validate(
        {"location": "http://mockdoc.com/grid", "partitioned": True}
    )

    assert resource_key(whole) != resource_key(split, companion=geometry)


def test_location_strategy_accepts_a_hashed_resource(
    versioned: VersionedResource,
) -> None:
    """Keying on location is always possible; it is merely weaker."""
    assert resource_key(versioned)


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


def test_context_changes_the_key(versioned: VersionedResource) -> None:
    """Inputs outside the resource can still be folded into identity."""
    assert resource_key(versioned) != resource_key(
        versioned, context={"code_version": "2.1.0"}
    )


def test_context_ordering_does_not_matter(versioned: VersionedResource) -> None:
    """Equal context mappings key alike regardless of insertion order."""
    assert resource_key(versioned, context={"a": "1", "b": "2"}) == resource_key(
        versioned, context={"b": "2", "a": "1"}
    )


def test_empty_context_matches_no_context(versioned: VersionedResource) -> None:
    """Passing nothing and passing an empty mapping mean the same thing."""
    assert resource_key(versioned) == resource_key(versioned, context={})


# ---------------------------------------------------------------------------
# Selection and versioning
# ---------------------------------------------------------------------------


def test_generator_for_prefers_the_hash(versioned: VersionedResource) -> None:
    """The strongest available identity wins."""
    assert generator_for(type(versioned)).scheme == "hash"


def test_generator_for_falls_back_to_location(plain: Resource) -> None:
    """An unhashed resource can still be keyed."""
    assert generator_for(type(plain)).scheme == "location"


def test_scheme_version_participates_in_the_digest(
    versioned: VersionedResource, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bumping the version is how every key is invalidated at once."""
    before = resource_key(versioned)
    monkeypatch.setattr(
        "cstar.orchestration.cache_keys.KEY_SCHEME_VERSION", KEY_SCHEME_VERSION + 1
    )

    assert resource_key(versioned) != before


def test_blueprint_resources_key_distinctly() -> None:
    """The resources in the shipped template must not collide with one another."""
    declared = [
        VersionedResource(location="http://mockdoc.com/partitioning1.nc", hash="abc"),
        VersionedResource(location="http://mockdoc.com/partitioning2.nc", hash="pqr"),
        VersionedResource(location="http://mockdoc.com/partitioning3.nc", hash="xyz"),
        VersionedResource(location="http://mockdoc.com/partitioning.nc", hash="abc123"),
        Resource(location="http://mockdoc.com/grid"),
        Resource.model_validate(
            {"location": "http://mockdoc.com/grid", "partitioned": True}
        ),
    ]
    geometry = PartitioningParameterSet(n_procs_x=16, n_procs_y=8)

    keys = [resource_key(resource, companion=geometry) for resource in declared]
    assert len(set(keys)) == len(keys)


# ---------------------------------------------------------------------------
# Location normalisation edge cases
# ---------------------------------------------------------------------------


def test_non_default_port_is_significant() -> None:
    """Only the scheme's default port is dropped; others identify a service."""
    assert resource_key(Resource(location="http://h:8080/x.nc")) != resource_key(
        Resource(location="http://h/x.nc")
    )


def test_credentials_are_part_of_the_location() -> None:
    """Two accounts may be served different content behind one host."""
    assert resource_key(Resource(location="http://alice@h/x.nc")) != resource_key(
        Resource(location="http://bob@h/x.nc")
    )


def test_password_participates_in_the_location() -> None:
    """A credential pair is kept whole rather than partly discarded."""
    assert resource_key(Resource(location="http://alice:one@h/x.nc")) != resource_key(
        Resource(location="http://alice:two@h/x.nc")
    )


def test_local_paths_pass_through_normalisation(tmp_path: object) -> None:
    """A filesystem path is not a URL, so it is keyed as written.

    Such a key is machine-specific and therefore unsuitable for the shared
    tier, which is why a hash should be declared for anything that matters.
    """
    assert resource_key(Resource(location="  /scratch/data/x.nc  ")) == (
        resource_key(Resource(location="/scratch/data/x.nc"))
    )


def test_root_path_survives_normalisation() -> None:
    """Stripping a trailing slash must not empty the path entirely."""
    assert resource_key(Resource(location="http://h/"))


def test_empty_location_is_rejected() -> None:
    """A resource with no location cannot be keyed on one."""
    with pytest.raises(CacheKeyError, match="declares no location"):
        location_identity(Resource.model_construct(location=""))


# ---------------------------------------------------------------------------
# Partition geometry
# ---------------------------------------------------------------------------


@pytest.fixture
def split() -> VersionedResource:
    """Return a resource declared pre-partitioned.

    Returns
    -------
    VersionedResource
        Resource under test.
    """
    return VersionedResource.model_validate(
        {"location": "http://h/x.nc", "hash": "abc", "partitioned": True}
    )


def test_partitioned_resource_requires_its_parameter_set(
    split: VersionedResource,
) -> None:
    """The flag says a resource is split, not how, so it cannot key alone.

    Silently keying on the flag would give two runs using different process
    grids one key for different data — the defect this raise exists to prevent.
    """
    with pytest.raises(CacheKeyError, match="declared partitioned"):
        resource_key(split)


def test_partition_geometry_changes_the_key(split: VersionedResource) -> None:
    """Different process grids produce physically different data."""
    assert resource_key(
        split, companion=PartitioningParameterSet(n_procs_x=16, n_procs_y=8)
    ) != resource_key(
        split, companion=PartitioningParameterSet(n_procs_x=8, n_procs_y=4)
    )


def test_matching_geometry_keys_alike(
    split: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Two runs splitting one resource identically must share a cache entry."""
    assert resource_key(split, companion=geometry) == resource_key(
        split, companion=PartitioningParameterSet(n_procs_x=16, n_procs_y=8)
    )


def test_axes_are_not_interchangeable(split: VersionedResource) -> None:
    """A 16x8 split is not an 8x16 split."""
    assert resource_key(
        split, companion=PartitioningParameterSet(n_procs_x=16, n_procs_y=8)
    ) != resource_key(
        split, companion=PartitioningParameterSet(n_procs_x=8, n_procs_y=16)
    )


def test_geometry_is_ignored_for_an_unpartitioned_resource(
    versioned: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """A caller looping over a blueprint may pass it for every resource.

    Geometry cannot affect the content of data that was never split, so
    supplying it is harmless rather than an error.
    """
    assert resource_key(versioned, companion=geometry) == resource_key(versioned)


def test_parameter_set_governance_does_not_affect_the_key(
    split: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Documentation and lock state describe governance, not the data.

    Folding them in would split the cache on a documentation edit, which cannot
    change a single byte of output.
    """
    annotated = PartitioningParameterSet.model_validate(
        {
            "n_procs_x": 16,
            "n_procs_y": 8,
            "documentation": "http://docs.example/partitioning",
        }
    )

    assert resource_key(split, companion=annotated) == resource_key(
        split, companion=geometry
    )


def test_location_strategy_also_requires_geometry() -> None:
    """The requirement is on the resource, not on the keying strategy."""
    unhashed_split = Resource.model_validate(
        {"location": "http://mockdoc.com/grid", "partitioned": True}
    )

    with pytest.raises(CacheKeyError, match="declared partitioned"):
        resource_key(unhashed_split)


def test_parameter_set_hash_identifies_the_geometry(
    split: VersionedResource,
) -> None:
    """A declared hash identifies the whole parameter set, so it is used alone.

    It covers any dynamically added parameters too, which enumerating the
    known fields would miss.
    """
    hashed = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-abc"}
    )
    other = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-def"}
    )

    assert resource_key(split, companion=hashed) != resource_key(split, companion=other)


def test_parameter_set_hash_supersedes_the_parameters(
    split: VersionedResource,
) -> None:
    """A declared hash wins over the fields it summarises.

    Consequence worth knowing: the hash is trusted rather than recomputed, so a
    hash left stale after the geometry changes will key two different splits
    alike. Blueprints that do not maintain it should leave it unset.
    """
    stale = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-abc"}
    )
    edited = PartitioningParameterSet.model_validate(
        {"n_procs_x": 8, "n_procs_y": 4, "hash": "params-abc"}
    )

    assert resource_key(split, companion=stale) == resource_key(split, companion=edited)


def test_absent_hash_falls_back_to_the_parameters(
    split: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Without a hash the parameters themselves carry the identity."""
    assert resource_key(split, companion=geometry) != resource_key(
        split, companion=PartitioningParameterSet(n_procs_x=8, n_procs_y=4)
    )


def test_adding_a_hash_changes_the_key(
    split: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Two derivations of one geometry are not interchangeable.

    Adding a hash to an existing blueprint therefore invalidates artifacts
    cached under the parameter-derived key. That is a one-time cost of adopting
    hashes, not a correctness problem: the miss is safe, a false hit would not
    be.
    """
    hashed = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-abc"}
    )

    assert resource_key(split, companion=hashed) != resource_key(
        split, companion=geometry
    )


# ---------------------------------------------------------------------------
# Derived keys
# ---------------------------------------------------------------------------


def test_expansion_occupies_a_separate_key_space(
    versioned: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """A partition and the file it came from must never share a shared name.

    They are different shapes on disk — an archive and a file — so a collision
    is not a wasted cache entry but a corrupted one.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    geometry : PartitioningParameterSet
        Partition geometry under test.
    """
    single = resource_key(versioned)
    aggregate = aggregate_key(versioned, geometry)

    assert single != aggregate


def test_expansion_key_is_suffixed_as_a_set(
    versioned: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """An aggregate is not a NetCDF file and must not claim to be one.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    geometry : PartitioningParameterSet
        Partition geometry under test.
    """
    key = aggregate_key(versioned, geometry)

    assert key.endswith(AGGREGATE_SUFFIX)
    assert key.startswith("partitioning1-")


def test_expansion_key_tracks_the_geometry(
    versioned: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Splitting one resource two ways yields two artifacts.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    geometry : PartitioningParameterSet
        Partition geometry under test.
    """
    assert aggregate_key(versioned, geometry) != aggregate_key(
        versioned, PartitioningParameterSet(n_procs_x=8, n_procs_y=16)
    )


def test_expansion_requires_a_geometry(versioned: VersionedResource) -> None:
    """The geometry is what is being produced, so it can never be optional.

    This is the difference from an ordinary key, where a parameter set
    describes a resource that arrives already split and is ignored for one
    that does not.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    """
    with pytest.raises(TypeError):
        aggregate_key(versioned)  # type: ignore[call-arg]


def test_expansion_refuses_an_already_partitioned_source(
    split: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Repartitioning needs both geometries, and this key carries one.

    Refused rather than keyed on half its inputs, which would give two
    different repartitions the same name.

    Parameters
    ----------
    split : VersionedResource
        Resource declared pre-partitioned.
    geometry : PartitioningParameterSet
        Partition geometry under test.
    """
    with pytest.raises(CacheKeyError, match="already partitioned"):
        aggregate_key(split, geometry)


def test_expansion_delegates_source_identity(
    plain: Resource, geometry: PartitioningParameterSet
) -> None:
    """A source with no hash is keyed on its location rather than rejected.

    Parameters
    ----------
    plain : Resource
        Unhashed resource under test.
    geometry : PartitioningParameterSet
        Partition geometry under test.
    """
    key = aggregate_key(plain, geometry)

    assert key.endswith(AGGREGATE_SUFFIX)


def test_expansion_inherits_the_resource_key_space(
    versioned: VersionedResource,
    plain: Resource,
    geometry: PartitioningParameterSet,
) -> None:
    """A hashed source and an unhashed one expand into distinct key spaces.

    The aggregate scheme is derived from the resource's own, so the weaker
    identity does not quietly become the stronger one by passing through a
    derivation.

    Parameters
    ----------
    versioned : VersionedResource
        Hashed resource under test.
    plain : Resource
        Unhashed resource under test.
    geometry : PartitioningParameterSet
        Partition geometry under test.
    """
    assert aggregate_key(versioned, geometry) != aggregate_key(plain, geometry)


def test_expansion_key_is_stable(
    versioned: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """The same declaration keys the same way on every call.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    geometry : PartitioningParameterSet
        Partition geometry under test.
    """
    assert resource_key(versioned, companion=geometry) == resource_key(
        versioned, companion=geometry
    )


# ---------------------------------------------------------------------------
# Input closure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("identity_fn", "expected"),
    [
        (hash_identity, {"resource.hash"}),
        (location_identity, {"resource.location"}),
    ],
)
def test_identity_is_a_closed_set_of_fields(
    identity_fn: object,
    expected: set[str],
    versioned: VersionedResource,
) -> None:
    """Only these fields may reach a key, and this fails if that widens.

    Preprocessing an input takes hours to days, so a field that leaks into the
    key silently invalidates that work whenever it is edited — and the failure
    is invisible, because a stale key looks exactly like a cold cache. Nobody
    reports it; they wait again.

    Pinning the field set is what makes that a failing test rather than a slow
    month. Adding a field to :class:`~cstar.orchestration.models.Resource` will
    break this, which is the point: whether it belongs in the key is a decision
    somebody should have to make on purpose.

    Parameters
    ----------
    generator : CacheKeyGenerator
        Strategy under test.
    expected : set of str
        Fields permitted to identify a resource under that strategy.
    versioned : VersionedResource
        Resource under test.
    """
    assert set(identity_fn(versioned)) == expected  # type: ignore[operator]


def test_identity_fields_are_safe_to_merge(versioned: VersionedResource) -> None:
    """Each identity function qualifies its own fields, so composing is safe.

    ``hash_identity`` and ``partition_identity`` both describe something called
    a hash, meaning different things. If either left the name bare, merging
    them would let the geometry overwrite the content digest and put two
    different artifacts under one key. Qualifying in the function rather than
    at the merge site means a composer written elsewhere is safe too.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    """
    geometry = PartitioningParameterSet.model_validate(
        {"n_procs_x": 4, "n_procs_y": 2, "hash": "geom-abc"}
    )

    assert set(hash_identity(versioned)) & set(partition_identity(geometry)) == set()

    merged = with_partitioning(hash_identity)((versioned, geometry))
    assert merged["resource.hash"] == hash_identity(versioned)["resource.hash"]
    assert merged["partition.hash"] == "geom-abc"


def test_derived_identity_adds_nothing_of_its_own(
    versioned: VersionedResource,
) -> None:
    """A derivation is keyed by its source plus its scheme, not by new fields.

    The derivation itself lives in ``scheme`` and the geometry, both folded in
    by ``key_for``, so identity stays exactly the delegate's.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    """
    paired = with_partitioning(hash_identity)
    fields = paired((versioned, PartitioningParameterSet(n_procs_x=4, n_procs_y=2)))

    geometry = PartitioningParameterSet(n_procs_x=4, n_procs_y=2)

    assert fields["resource.hash"] == hash_identity(versioned)["resource.hash"]
    assert {field for field in fields if not field.startswith("resource.")} == set(
        partition_identity(geometry)
    )


def test_expansion_uses_a_declared_geometry_hash(
    versioned: VersionedResource,
) -> None:
    """A parameter set that declares a hash is identified by it.

    The hash covers the whole set, including parameters added dynamically that
    the model does not enumerate, so it supersedes the individual fields.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    """
    hashed = PartitioningParameterSet.model_validate(
        {"n_procs_x": 4, "n_procs_y": 2, "hash": "geom-abc"}
    )

    assert aggregate_key(versioned, hashed) != aggregate_key(
        versioned, PartitioningParameterSet(n_procs_x=4, n_procs_y=2)
    )


# ---------------------------------------------------------------------------
# Type-agnostic keys
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Grid:
    """A value with no relationship to the blueprint models.

    Attributes
    ----------
    nx : int
        Cells in x.
    ny : int
        Cells in y.
    crs : str
        Coordinate reference system.
    """

    nx: int
    ny: int
    crs: str = "EPSG:4326"


def _grid_identity(grid: _Grid) -> dict[str, str]:
    """Return the fields distinguishing one grid from another.

    Parameters
    ----------
    grid : _Grid
        Value being keyed.

    Returns
    -------
    dict of str to str
        Identifying fields.
    """
    return {"nx": str(grid.nx), "ny": str(grid.ny), "crs": grid.crs}


@pytest.fixture
def grid_keys() -> DynamicCacheKeyGenerator[_Grid]:
    """Return a generator keyed on grids.

    Returns
    -------
    DynamicCacheKeyGenerator
        Generator under test.
    """
    return DynamicCacheKeyGenerator("grid", _grid_identity)


def test_any_type_can_be_keyed(grid_keys: DynamicCacheKeyGenerator[_Grid]) -> None:
    """A value unrelated to the blueprint models keys without adapting it.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    key = grid_keys.key_for(_Grid(16, 8), Path("/data/domain.nc"))

    assert key.startswith("domain-")
    assert key.endswith(".nc")
    assert len(key) == len("domain-") + DIGEST_LENGTH + len(".nc")


def test_identity_fields_change_the_key(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """Whatever the identity function reports is what distinguishes artifacts.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    assert grid_keys.key_for(_Grid(16, 8), "/d/x.nc") != grid_keys.key_for(
        _Grid(8, 16), "/d/x.nc"
    )


def test_the_directory_does_not_participate(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """One artifact keyed from two workspaces must agree.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    assert grid_keys.key_for(_Grid(16, 8), "/scratch/a/x.nc") == grid_keys.key_for(
        _Grid(16, 8), Path("/home/b/x.nc")
    )


def test_the_filename_does_participate(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """The stem is folded in, so the key stays a pure function of its inputs.

    The cost is duplication: one value under two names caches twice. That is
    the same trade the resource strategies make, for the same reason — legible
    cache listings.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    assert grid_keys.key_for(_Grid(16, 8), "/d/x.nc") != grid_keys.key_for(
        _Grid(16, 8), "/d/y.nc"
    )


def test_schemes_separate_the_key_space() -> None:
    """Two identity functions over one type must not collide."""
    first = DynamicCacheKeyGenerator("grid", _grid_identity)
    second = DynamicCacheKeyGenerator("grid-coarsened", _grid_identity)

    assert first.key_for(_Grid(16, 8), "/d/x.nc") != second.key_for(
        _Grid(16, 8), "/d/x.nc"
    )


def test_dynamic_scheme_version_participates(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bumping the scheme version invalidates every key at once.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to move the version.
    """
    before = grid_keys.key_for(_Grid(16, 8), "/d/x.nc")
    monkeypatch.setattr(cache_keys, "KEY_SCHEME_VERSION", KEY_SCHEME_VERSION + 1)

    assert grid_keys.key_for(_Grid(16, 8), "/d/x.nc") != before


def test_dynamic_context_changes_the_key(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """Inputs outside the value still have to reach the key.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    assert grid_keys.key_for(
        _Grid(16, 8), "/d/x.nc", context={"solver": "v1"}
    ) != grid_keys.key_for(_Grid(16, 8), "/d/x.nc", context={"solver": "v2"})


def test_dynamic_empty_context_matches_no_context(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """An empty mapping is the absence of context, not a distinct one.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    assert grid_keys.key_for(_Grid(16, 8), "/d/x.nc", context={}) == grid_keys.key_for(
        _Grid(16, 8), "/d/x.nc"
    )


def test_suffix_can_be_overridden(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """A set is not a file of the source's type and must not claim to be.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    key = grid_keys.key_for(_Grid(16, 8), "/d/x.nc", suffix=AGGREGATE_SUFFIX)

    assert key.endswith(AGGREGATE_SUFFIX)
    assert key != grid_keys.key_for(_Grid(16, 8), "/d/x.nc")


def test_partitioning_is_expressible_through_the_identity_function() -> None:
    """What the base class hard-codes, a caller supplies.

    This is the point of the design: geometry is not privileged, it is just
    another field somebody decided identifies their artifact.
    """

    def identity(pair: tuple[_Grid, PartitioningParameterSet]) -> dict[str, str]:
        """Fold a process grid into a grid's identity.

        Parameters
        ----------
        pair : tuple
            Grid and the geometry it is split across.

        Returns
        -------
        dict of str to str
            Identifying fields.
        """
        grid, geometry = pair
        return {
            **_grid_identity(grid),
            "procs": f"{geometry.n_procs_x}x{geometry.n_procs_y}",
        }

    generator = DynamicCacheKeyGenerator("partitioned-grid", identity)
    grid = _Grid(16, 8)

    assert generator.key_for(
        (grid, PartitioningParameterSet(n_procs_x=4, n_procs_y=2)), "/d/x.nc"
    ) != generator.key_for(
        (grid, PartitioningParameterSet(n_procs_x=2, n_procs_y=4)), "/d/x.nc"
    )


def test_an_empty_identity_is_refused() -> None:
    """A key with no identity is the filename, which anything can share."""
    generator: DynamicCacheKeyGenerator[_Grid] = DynamicCacheKeyGenerator(
        "empty", lambda _: {}
    )

    with pytest.raises(CacheKeyError, match="returned nothing"):
        generator.key_for(_Grid(16, 8), "/d/x.nc")


def test_non_string_identity_values_are_refused() -> None:
    """Normalisation belongs to whoever understands the type."""
    generator: DynamicCacheKeyGenerator[_Grid] = DynamicCacheKeyGenerator(
        "loose",
        lambda grid: {"nx": grid.nx},  # type: ignore[dict-item]
    )

    with pytest.raises(CacheKeyError, match="must supply str to str"):
        generator.key_for(_Grid(16, 8), "/d/x.nc")


@pytest.mark.parametrize("scheme", ["", "has space", "has/slash"])
def test_an_unusable_scheme_is_refused(scheme: str) -> None:
    """The scheme reaches the key space and must be a safe tag.

    Parameters
    ----------
    scheme : str
        Candidate scheme.
    """
    with pytest.raises(CacheKeyError, match="filesystem-safe"):
        DynamicCacheKeyGenerator(scheme, _grid_identity)


def test_dynamic_repr_names_the_scheme(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """A generator is identified in debugging output by its key space.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    assert repr(grid_keys) == "DynamicCacheKeyGenerator(scheme='grid')"


def test_an_extensionless_path_yields_a_bare_key(
    grid_keys: DynamicCacheKeyGenerator[_Grid],
) -> None:
    """Not every artifact has an extension to carry.

    Parameters
    ----------
    grid_keys : DynamicCacheKeyGenerator
        Generator under test.
    """
    assert not Path(grid_keys.key_for(_Grid(16, 8), "/d/domain")).suffix


def test_readable_parts_falls_back_when_nothing_survives() -> None:
    """A location with no usable name still produces a legible key."""
    assert readable_parts("/") == ("artifact", "")
    assert readable_parts("https://example.org/") == ("artifact", "")


def test_context_values_must_be_strings(versioned: VersionedResource) -> None:
    """An unformatted value renders by ``repr``, which is not always stable.

    A set's ``repr`` depends on ``PYTHONHASHSEED``, so a key built from one
    differs between processes and the cache never hits — silently, because an
    unreachable cache looks exactly like a cold one.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    """
    with pytest.raises(CacheKeyError, match="context must supply str to str"):
        resource_key(versioned, context={"features": {"alpha", "beta"}})  # type: ignore[dict-item]


def test_context_is_not_identity(versioned: VersionedResource) -> None:
    """Context and identity occupy separate payload slots, so neither shadows.

    A caller may use a field name in context that an identity function already
    uses without the two interfering.

    Parameters
    ----------
    versioned : VersionedResource
        Resource under test.
    """
    shadowed = resource_key(versioned, context={"resource.hash": "not-the-hash"})

    assert shadowed != resource_key(versioned)
    assert shadowed != resource_key(
        VersionedResource(location=str(versioned.location), hash="not-the-hash")
    )


def test_is_registered_reports_unknown_shapes() -> None:
    """Callers discover pairings by asking, rather than by naming a type.

    This is what lets :mod:`cstar.orchestration.caching` find the value that
    pairs with a resource without importing the application that defines it.
    """
    assert is_registered(VersionedResource)
    assert is_registered((VersionedResource, PartitioningParameterSet))
    assert not is_registered(complex)
    assert not is_registered((VersionedResource, complex))


# ---------------------------------------------------------------------------
# Declarative registration
# ---------------------------------------------------------------------------


def test_decorator_registers_a_plain_identity() -> None:
    """Registration sits on the function it registers."""

    class Widget:
        """A type registered by decoration."""

    @identity_for(Widget, "widget")
    def widget_identity(widget: Widget) -> dict[str, str]:
        """Identify a widget.

        Parameters
        ----------
        widget : Widget
            Value being keyed.

        Returns
        -------
        dict of str to str
            Identifying fields.
        """
        return {"widget.kind": "plain"}

    assert generator_for(Widget).scheme == "widget"
    assert widget_identity(Widget()) == {"widget.kind": "plain"}


def test_decorator_returns_the_function_unchanged() -> None:
    """A registered function stays directly callable and testable."""

    class Gadget:
        """A type registered by decoration."""

    def gadget_identity(gadget: Gadget) -> dict[str, str]:
        """Identify a gadget.

        Parameters
        ----------
        gadget : Gadget
            Value being keyed.

        Returns
        -------
        dict of str to str
            Identifying fields.
        """
        return {"gadget.kind": "plain"}

    assert identity_for(Gadget, "gadget")(gadget_identity) is gadget_identity


def test_decorator_composes_through_a_base() -> None:
    """A factory registers once per base it composes with, by stacking.

    Returning the function unchanged is what makes stacking work: each
    application sees the original factory rather than a wrapper.
    """

    class Facet:
        """A companion type registered by decoration."""

    @identity_for((VersionedResource, Facet), "hash", base=hash_identity)
    @identity_for((Resource, Facet), "location", base=location_identity)
    def with_facet(base: object) -> object:
        """Compose a resource identity with a facet.

        Parameters
        ----------
        base : object
            Identity function for the resource alone.

        Returns
        -------
        object
            Identity function over a pair.
        """

        def identity(subject: tuple[DataResource, Facet]) -> dict[str, str]:
            """Return the pair's identity.

            Parameters
            ----------
            subject : tuple
                Resource and facet.

            Returns
            -------
            dict of str to str
                Identifying fields.
            """
            resource, _ = subject
            return {**base(resource), "facet.tag": "x"}  # type: ignore[operator]

        return identity

    assert generator_for((VersionedResource, Facet)).scheme == "hash"
    assert generator_for((Resource, Facet)).scheme == "location"
    assert callable(with_facet(hash_identity))


def test_a_factory_that_returns_a_non_callable_is_refused() -> None:
    """Passing a base declares the function a factory, so it must behave as one."""

    class Broken:
        """A type whose registration is malformed."""

    with pytest.raises(CacheKeyError, match="rather than an identity function"):

        @identity_for(Broken, "broken", base=hash_identity)
        def not_a_factory(base: object) -> str:
            """Return something that is not an identity function.

            Parameters
            ----------
            base : object
                Ignored.

            Returns
            -------
            str
                Not a callable.
            """
            return "oops"
