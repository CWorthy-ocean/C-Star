Caching API
###########

The Caching API is used to store, re-use, and share assets used or created
during by a :term:`Blueprint` in an orchestrated :term:`workplan`.

.. seealso::

   Use the caching-api within :doc:`custom_applications` 

Caching
-------

Reuses expensive preprocessing between runs and between users. 

.. seealso::

   For details on how keys are derived and what each mechanism guarantees, see :doc:`caching` 

.. autosummary::
   :toctree: generated/

   cstar.orchestration.artifact_cache.ArtifactCache
   cstar.orchestration.artifact_cache.Location
   cstar.orchestration.artifact_cache.ArtifactRecord
   cstar.orchestration.artifact_cache.ArtifactKind
   cstar.orchestration.artifact_cache.Manifest
   cstar.orchestration.artifact_cache.OnConflict
   cstar.orchestration.artifact_cache.SetManifest
   cstar.orchestration.artifact_cache.Tier
   cstar.orchestration.caching.cached
   cstar.orchestration.caching.FileSet
   cstar.orchestration.caching.fileset_for
   cstar.orchestration.caching.cache_fileset
   cstar.orchestration.cache_keys.resource_key
   cstar.orchestration.cache_keys.aggregate_key
   cstar.orchestration.cache_keys.identity_for
   cstar.orchestration.cache_keys.register_identity
   cstar.orchestration.cache_keys.generator_for
   cstar.orchestration.cache_keys.DynamicCacheKeyGenerator
   cstar.orchestration.fingerprinting.Fingerprinter
   cstar.orchestration.fingerprinting.FullFingerprinter
   cstar.orchestration.fingerprinting.NullFingerprinter
   cstar.orchestration.fingerprinting.QuickFingerprinter
