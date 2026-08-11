"""Cache-key identity for ROMS/MARBL types.

:mod:`cstar.orchestration.cache_keys` knows how to key a blueprint resource and
nothing about how ROMS splits one across a process grid — deliberately, since
orchestration should not depend on an application. This module supplies that
half: it says how a
:class:`~cstar.applications.roms_marbl.models.PartitioningParameterSet`
identifies itself, and registers the pairings that let a resource be keyed
together with the geometry it is split across.

Importing this module is what makes those pairings available. Import it from
the application entry point, or wherever ROMS/MARBL work is set up, before the
first key is derived.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cstar.applications.roms_marbl.models import PartitioningParameterSet
from cstar.orchestration.cache_keys import (
    hash_identity,
    location_identity,
    register_identity,
)
from cstar.orchestration.models import Resource, VersionedResource

if TYPE_CHECKING:
    from cstar.orchestration.cache_keys import IdentityFunction
    from cstar.orchestration.models import DataResource

__all__ = ["PARAMETER_METADATA", "partition_identity", "with_partitioning"]

PARAMETER_METADATA: frozenset[str] = frozenset({"documentation", "locked"})
"""Parameter-set fields excluded from a key.

These describe governance rather than the data a parameter set produces:
``documentation`` is a provenance URL and ``locked`` is a mutability flag.
Folding either in would split the cache on edits that cannot change a single
byte of output.

``hash`` is excluded from this set because it is not metadata but an identity;
see :func:`partition_identity`.
"""


def partition_identity(partitioning: PartitioningParameterSet) -> dict[str, str]:
    """Identify the geometry a resource is split across.

    Parameters
    ----------
    partitioning : PartitioningParameterSet
        Geometry the resource is split across.

    Returns
    -------
    dict of str to str
        The parameter set's ``hash`` when it declares one, since that hash
        identifies the whole set including any dynamically added parameters.
        Otherwise the substantive parameters themselves. Fields are namespaced
        under ``partition.``: a parameter set's ``hash`` is not a resource's
        ``hash``, and merging the two flat would let the geometry silently
        overwrite the content digest — two different artifacts under one key.
        Qualifying here rather than at the merge site means every composer is
        safe, not just the one shipped below.

    Warnings
    --------
    A declared ``hash`` is trusted, not verified. Nothing here recomputes it,
    so a hash left stale after an edit to the parameters will key two different
    geometries alike — the one way this function can produce a false cache hit.
    Blueprints that do not maintain the hash should leave it unset, which falls
    back to the parameters themselves.
    """
    declared = getattr(partitioning, "hash", None)
    if declared:
        return {"partition.hash": str(declared)}
    return {
        f"partition.{field}": str(value)
        for field, value in partitioning.model_dump(mode="json").items()
        if field not in PARAMETER_METADATA
    }


def with_partitioning(base: IdentityFunction) -> IdentityFunction:
    """Compose a resource identity with the geometry it is split across.

    Parameters
    ----------
    base : IdentityFunction
        Identity function for the resource alone.

    Returns
    -------
    IdentityFunction
        Identity function over a ``(resource, partitioning)`` pair.

    Notes
    -----
    A plain merge is safe because each identity function qualifies its own
    field names; see :func:`partition_identity`.
    """

    def identity(
        subject: tuple[DataResource, PartitioningParameterSet],
    ) -> dict[str, str]:
        """Return the resource's identity with the geometry folded in.

        Parameters
        ----------
        subject : tuple
            Resource and the geometry it is split across.

        Returns
        -------
        dict of str to str
            Identifying fields.
        """
        resource, partitioning = subject
        return {**base(resource), **partition_identity(partitioning)}

    return identity


register_identity(
    (VersionedResource, PartitioningParameterSet),
    "hash",
    with_partitioning(hash_identity),
)
register_identity(
    (Resource, PartitioningParameterSet),
    "location",
    with_partitioning(location_identity),
)
