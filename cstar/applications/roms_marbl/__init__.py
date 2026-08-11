"""ROMS/MARBL application models and cache-key identity.

Importing this package registers how its types are identified for cache keys,
so a caller that uses a
:class:`~cstar.applications.roms_marbl.models.PartitioningParameterSet` can key
on it without knowing registration was a step. The alternative — requiring an
explicit import at every entry point — fails by producing an unkeyable type
somewhere far from the omission.
"""

from cstar.applications.roms_marbl import cache as _cache  # noqa: F401
