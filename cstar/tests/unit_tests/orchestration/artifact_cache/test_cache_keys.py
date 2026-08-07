"""Unit tests for :mod:`cstar.orchestration.cache_keys`.

Exercised against the real :class:`~cstar.orchestration.models.Resource` and
:class:`~cstar.orchestration.models.VersionedResource` models, using the
declarations that appear in the shipped ROMS/MARBL blueprint template.
"""

import pytest

from cstar.applications.roms_marbl.models import PartitioningParameterSet
from cstar.orchestration.cache_keys import (
    DIGEST_LENGTH,
    KEY_SCHEME_VERSION,
    CacheKeyError,
    CacheKeyGenerator,
    HashKeyGenerator,
    LocationKeyGenerator,
    generator_for,
)
from cstar.orchestration.models import Resource, VersionedResource


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


def test_base_class_is_abstract() -> None:
    """The strategy interface cannot be instantiated directly."""
    with pytest.raises(TypeError):
        CacheKeyGenerator()  # type: ignore[abstract]


@pytest.mark.parametrize(
    ("generator", "scheme"),
    [(HashKeyGenerator(), "hash"), (LocationKeyGenerator(), "location")],
)
def test_each_generator_tags_its_scheme(
    generator: CacheKeyGenerator, scheme: str
) -> None:
    """Every strategy names its derivation, so two can never collide."""
    assert generator.scheme == scheme


def test_schemes_produce_different_keys(versioned: VersionedResource) -> None:
    """The same resource keyed two ways yields two distinct artifacts."""
    assert HashKeyGenerator().key_for(versioned) != LocationKeyGenerator().key_for(
        versioned
    )


def test_repr_names_the_strategy() -> None:
    """The representation is useful when logging which scheme produced a key."""
    assert repr(HashKeyGenerator()) == "HashKeyGenerator()"


# ---------------------------------------------------------------------------
# Key shape
# ---------------------------------------------------------------------------


def test_key_is_filesystem_safe(versioned: VersionedResource) -> None:
    """A key is used directly as an artifact name, so it must be a bare filename."""
    key = HashKeyGenerator().key_for(versioned)
    assert "/" not in key
    assert key not in {".", ".."}
    assert key


def test_key_keeps_the_source_name_and_extension(
    versioned: VersionedResource,
) -> None:
    """A human listing the cache should be able to tell what they are seeing."""
    key = HashKeyGenerator().key_for(versioned)
    assert key.startswith("partitioning1-")
    assert key.endswith(".nc")


def test_key_embeds_a_truncated_digest(versioned: VersionedResource) -> None:
    """The digest carries the identity; its length is part of the format."""
    key = HashKeyGenerator().key_for(versioned)
    digest = key.removeprefix("partitioning1-").removesuffix(".nc")
    assert len(digest) == DIGEST_LENGTH
    assert all(character in "0123456789abcdef" for character in digest)


def test_extensionless_location_yields_a_bare_key(plain: Resource) -> None:
    """A location with no suffix still produces a usable name."""
    key = LocationKeyGenerator().key_for(plain)
    assert key.startswith("grid-")
    assert "." not in key.removeprefix("grid-")


def test_key_is_deterministic(versioned: VersionedResource) -> None:
    """Two calls on equal inputs agree, or lookups could never hit."""
    assert HashKeyGenerator().key_for(versioned) == HashKeyGenerator().key_for(
        versioned
    )


# ---------------------------------------------------------------------------
# Hash strategy
# ---------------------------------------------------------------------------


def test_hash_key_ignores_which_mirror_serves_the_file() -> None:
    """Identical content behind two hosts should share one cached artifact."""
    generator = HashKeyGenerator()
    left = VersionedResource(location="http://mirror-a.example/x.nc", hash="abc")
    right = VersionedResource(location="http://mirror-b.example/x.nc", hash="abc")

    assert generator.key_for(left) == generator.key_for(right)


def test_hash_key_changes_with_the_hash() -> None:
    """New upstream data must not be served from the old cache entry."""
    generator = HashKeyGenerator()
    before = VersionedResource(location="http://h/x.nc", hash="abc")
    after = VersionedResource(location="http://h/x.nc", hash="def")

    assert generator.key_for(before) != generator.key_for(after)


def test_hash_key_separates_partitioned_data(
    geometry: PartitioningParameterSet,
) -> None:
    """A pre-partitioned resource is physically different data."""
    generator = HashKeyGenerator()
    whole = VersionedResource(location="http://h/x.nc", hash="abc")
    split = VersionedResource.model_validate(
        {"location": "http://h/x.nc", "hash": "abc", "partitioned": True}
    )

    assert generator.key_for(whole) != generator.key_for(split, partitioning=geometry)


def test_hash_key_distinguishes_renamed_content() -> None:
    """The filename is part of the key, so a rename caches separately.

    A deliberate trade: legible cache listings in exchange for occasionally
    storing one payload twice.
    """
    generator = HashKeyGenerator()
    left = VersionedResource(location="http://h/x.nc", hash="abc")
    right = VersionedResource(location="http://h/y.nc", hash="abc")

    assert generator.key_for(left) != generator.key_for(right)


def test_hash_strategy_refuses_an_unhashed_resource(plain: Resource) -> None:
    """Without a hash this strategy cannot identify the content, so it says so."""
    with pytest.raises(CacheKeyError, match="declares no hash"):
        HashKeyGenerator().key_for(plain)


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
    generator = LocationKeyGenerator()
    canonical = Resource(location="http://mockdoc.com/a/x.nc")

    assert generator.key_for(Resource(location=equivalent)) == generator.key_for(
        canonical
    )


def test_location_keeps_the_query_string() -> None:
    """A query often selects which content is served, so it is significant."""
    generator = LocationKeyGenerator()

    assert generator.key_for(
        Resource(location="http://h/x.nc?version=1")
    ) != generator.key_for(Resource(location="http://h/x.nc?version=2"))


def test_location_key_separates_partitioned_data(
    geometry: PartitioningParameterSet,
) -> None:
    """Partitioning applies to unhashed resources too."""
    generator = LocationKeyGenerator()
    whole = Resource(location="http://mockdoc.com/grid")
    split = Resource.model_validate(
        {"location": "http://mockdoc.com/grid", "partitioned": True}
    )

    assert generator.key_for(whole) != generator.key_for(split, partitioning=geometry)


def test_location_strategy_accepts_a_hashed_resource(
    versioned: VersionedResource,
) -> None:
    """Keying on location is always possible; it is merely weaker."""
    assert LocationKeyGenerator().key_for(versioned)


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


def test_context_changes_the_key(versioned: VersionedResource) -> None:
    """Inputs outside the resource can still be folded into identity."""
    generator = HashKeyGenerator()

    assert generator.key_for(versioned) != generator.key_for(
        versioned, context={"code_version": "2.1.0"}
    )


def test_context_ordering_does_not_matter(versioned: VersionedResource) -> None:
    """Equal context mappings key alike regardless of insertion order."""
    generator = HashKeyGenerator()

    assert generator.key_for(versioned, context={"a": 1, "b": 2}) == generator.key_for(
        versioned, context={"b": 2, "a": 1}
    )


def test_empty_context_matches_no_context(versioned: VersionedResource) -> None:
    """Passing nothing and passing an empty mapping mean the same thing."""
    generator = HashKeyGenerator()

    assert generator.key_for(versioned) == generator.key_for(versioned, context={})


# ---------------------------------------------------------------------------
# Selection and versioning
# ---------------------------------------------------------------------------


def test_generator_for_prefers_the_hash(versioned: VersionedResource) -> None:
    """The strongest available identity wins."""
    assert isinstance(generator_for(versioned), HashKeyGenerator)


def test_generator_for_falls_back_to_location(plain: Resource) -> None:
    """An unhashed resource can still be keyed."""
    assert isinstance(generator_for(plain), LocationKeyGenerator)


def test_scheme_version_participates_in_the_digest(
    versioned: VersionedResource, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bumping the version is how every key is invalidated at once."""
    before = HashKeyGenerator().key_for(versioned)
    monkeypatch.setattr(
        "cstar.orchestration.cache_keys.KEY_SCHEME_VERSION", KEY_SCHEME_VERSION + 1
    )

    assert HashKeyGenerator().key_for(versioned) != before


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

    keys = [
        generator_for(resource).key_for(resource, partitioning=geometry)
        for resource in declared
    ]
    assert len(set(keys)) == len(keys)


# ---------------------------------------------------------------------------
# Location normalisation edge cases
# ---------------------------------------------------------------------------


def test_non_default_port_is_significant() -> None:
    """Only the scheme's default port is dropped; others identify a service."""
    generator = LocationKeyGenerator()

    assert generator.key_for(
        Resource(location="http://h:8080/x.nc")
    ) != generator.key_for(Resource(location="http://h/x.nc"))


def test_credentials_are_part_of_the_location() -> None:
    """Two accounts may be served different content behind one host."""
    generator = LocationKeyGenerator()

    assert generator.key_for(
        Resource(location="http://alice@h/x.nc")
    ) != generator.key_for(Resource(location="http://bob@h/x.nc"))


def test_password_participates_in_the_location() -> None:
    """A credential pair is kept whole rather than partly discarded."""
    generator = LocationKeyGenerator()

    assert generator.key_for(
        Resource(location="http://alice:one@h/x.nc")
    ) != generator.key_for(Resource(location="http://alice:two@h/x.nc"))


def test_local_paths_pass_through_normalisation(tmp_path: object) -> None:
    """A filesystem path is not a URL, so it is keyed as written.

    Such a key is machine-specific and therefore unsuitable for the shared
    tier, which is why a hash should be declared for anything that matters.
    """
    generator = LocationKeyGenerator()

    assert generator.key_for(Resource(location="  /scratch/data/x.nc  ")) == (
        generator.key_for(Resource(location="/scratch/data/x.nc"))
    )


def test_root_path_survives_normalisation() -> None:
    """Stripping a trailing slash must not empty the path entirely."""
    assert LocationKeyGenerator().key_for(Resource(location="http://h/"))


def test_empty_location_is_rejected() -> None:
    """A resource with no location cannot be keyed on one."""
    with pytest.raises(CacheKeyError, match="declares no location"):
        LocationKeyGenerator().identity(Resource.model_construct(location=""))


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
        HashKeyGenerator().key_for(split)


def test_partition_geometry_changes_the_key(split: VersionedResource) -> None:
    """Different process grids produce physically different data."""
    generator = HashKeyGenerator()

    assert generator.key_for(
        split, partitioning=PartitioningParameterSet(n_procs_x=16, n_procs_y=8)
    ) != generator.key_for(
        split, partitioning=PartitioningParameterSet(n_procs_x=8, n_procs_y=4)
    )


def test_matching_geometry_keys_alike(
    split: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Two runs splitting one resource identically must share a cache entry."""
    generator = HashKeyGenerator()

    assert generator.key_for(split, partitioning=geometry) == generator.key_for(
        split, partitioning=PartitioningParameterSet(n_procs_x=16, n_procs_y=8)
    )


def test_axes_are_not_interchangeable(split: VersionedResource) -> None:
    """A 16x8 split is not an 8x16 split."""
    generator = HashKeyGenerator()

    assert generator.key_for(
        split, partitioning=PartitioningParameterSet(n_procs_x=16, n_procs_y=8)
    ) != generator.key_for(
        split, partitioning=PartitioningParameterSet(n_procs_x=8, n_procs_y=16)
    )


def test_geometry_is_ignored_for_an_unpartitioned_resource(
    versioned: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """A caller looping over a blueprint may pass it for every resource.

    Geometry cannot affect the content of data that was never split, so
    supplying it is harmless rather than an error.
    """
    generator = HashKeyGenerator()

    assert generator.key_for(versioned, partitioning=geometry) == generator.key_for(
        versioned
    )


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

    assert HashKeyGenerator().key_for(
        split, partitioning=annotated
    ) == HashKeyGenerator().key_for(split, partitioning=geometry)


def test_location_strategy_also_requires_geometry() -> None:
    """The requirement is on the resource, not on the keying strategy."""
    unhashed_split = Resource.model_validate(
        {"location": "http://mockdoc.com/grid", "partitioned": True}
    )

    with pytest.raises(CacheKeyError, match="declared partitioned"):
        LocationKeyGenerator().key_for(unhashed_split)


def test_parameter_set_hash_identifies_the_geometry(
    split: VersionedResource,
) -> None:
    """A declared hash identifies the whole parameter set, so it is used alone.

    It covers any dynamically added parameters too, which enumerating the
    known fields would miss.
    """
    generator = HashKeyGenerator()
    hashed = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-abc"}
    )
    other = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-def"}
    )

    assert generator.key_for(split, partitioning=hashed) != generator.key_for(
        split, partitioning=other
    )


def test_parameter_set_hash_supersedes_the_parameters(
    split: VersionedResource,
) -> None:
    """A declared hash wins over the fields it summarises.

    Consequence worth knowing: the hash is trusted rather than recomputed, so a
    hash left stale after the geometry changes will key two different splits
    alike. Blueprints that do not maintain it should leave it unset.
    """
    generator = HashKeyGenerator()
    stale = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-abc"}
    )
    edited = PartitioningParameterSet.model_validate(
        {"n_procs_x": 8, "n_procs_y": 4, "hash": "params-abc"}
    )

    assert generator.key_for(split, partitioning=stale) == generator.key_for(
        split, partitioning=edited
    )


def test_absent_hash_falls_back_to_the_parameters(
    split: VersionedResource, geometry: PartitioningParameterSet
) -> None:
    """Without a hash the parameters themselves carry the identity."""
    generator = HashKeyGenerator()

    assert generator.key_for(split, partitioning=geometry) != generator.key_for(
        split, partitioning=PartitioningParameterSet(n_procs_x=8, n_procs_y=4)
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
    generator = HashKeyGenerator()
    hashed = PartitioningParameterSet.model_validate(
        {"n_procs_x": 16, "n_procs_y": 8, "hash": "params-abc"}
    )

    assert generator.key_for(split, partitioning=hashed) != generator.key_for(
        split, partitioning=geometry
    )
