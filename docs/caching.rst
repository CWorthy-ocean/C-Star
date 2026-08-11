.. _caching:

Artifact caching
################

C-Star preprocessing steps — pulling a slice from a multi-terabyte dataset,
deriving variables, splitting a field across a process grid — take hours to
days. When a simulation fails on a bad configuration, none of that upstream
work needs repeating. The caching subsystem exists to make sure it isn't.


.. seealso::

    :doc:`custom_applications`


What the cache guarantees
-------------------------

Three properties shape every decision below.

**Names determine content.** The shared tier is addressed by artifact name
alone, so two runs that would produce interchangeable results must generate the
same name, and two runs that would not must generate different ones. Everything
in *Cache keys* follows from this.

**Lookups are cheap.** Asking "has anyone already produced this?" costs a
``stat``, whether the answer is yes or no. A lookup that had to read the data
first would defeat its own purpose.

**A miss is safe; a false hit is not.** Where the two trade off, the design
takes the miss. A miss costs time; a false hit silently substitutes the wrong
data into a scientific result.

Two tiers
---------

====== =============================== =====================
Tier   Path                            Holds
====== =============================== =====================
User   ``<user_root>/<run_id>/<name>`` One run's workspace
Shared ``<shared_root>/<name>``        The published library
====== =============================== =====================

The addressing is deliberately asymmetric. A user-tier path carries the run
that produced the artifact; a shared path carries only the name.

That asymmetry is the point. A shared artifact is one that several runs — and
several users — should reuse, and a run identifier in the path would make each
run's copy distinct, which is exactly the reuse the tier exists to provide. The
cost is that the shared tier has no room for two artifacts of one name, which
is what makes the *names determine content* rule load-bearing rather than
merely tidy.

:meth:`~cstar.orchestration.artifact_cache.ArtifactCache.resolve` checks the shared
tier first and falls back to the run's own workspace, so a published artifact is
preferred over a local rebuild.

Promotion
~~~~~~~~~

Promotion copies an artifact from a run's workspace into the shared tier. It is
always an explicit call — never a side effect of producing something — because
publishing to a space everyone reads is a decision about what is worth keeping,
and a producer is the wrong place to make it. The :meth:`@cached<cstar.orchestration.caching.cached>`
decorator's ``promote`` parameter defaults to off for the same reason.

When the shared name is already taken, ``OnConflict`` decides:

-  ``ERROR`` (default) — refuse. The name is claimed by different content.
-  ``SKIP`` — keep what is published, record the divergence, carry on.
-  ``OVERWRITE`` — replace.

``SKIP`` is not "ignore the problem". Divergence under one name has two
indistinguishable causes: the computation was not bit-reproducible, or the key
omits an input that actually matters. The second is a defect that stays hidden
for months, so occurrences are counted and logged rather than discarded.

``SKIP`` is the right default for long-running work. A format that stamps a
creation timestamp into its header makes byte-difference routine for identical
data, and failing a week-long run over that is the wrong trade.

Cache keys
----------

Keys are **input-addressed**: computed from the declaration of an input, before
that input is fetched or processed. That is what makes them usable as a lookup
— a run can ask "has anyone already produced this?" without first doing the
work. Content-addressing would answer the same question only after paying for
it.

The mechanism
~~~~~~~~~~~~~

:class:`~cstar.orchestration.cache_keys.DynamicCacheKeyGenerator` assembles every key the same way: a scheme naming
the derivation, a scheme version, a readable stem, and a truncated SHA-256 over
a sorted payload. What identifies a particular subject is supplied as a
function, so nothing about any one type is baked into the machinery.

::

   <stem>-<16 hex chars><suffix>
   bgc-boundary-2010-cb48017839fceec2.nc

The stem is for humans reading a cache listing; the digest is the identity. The
filename is folded into the digest as well as prefixed, so a key stays a pure
function of its inputs — at the cost of caching the same content twice under
two names, which is a deliberate trade for legible listings.

Identity functions
~~~~~~~~~~~~~~~~~~

An identity function returns the fields that distinguish one result from
another, and nothing that varies between equivalent runs. Two rules apply.

**Values are strings.** Normalisation — rounding, case, ordering — is decided
by the code that understands the type, not by ``json.dumps``. This is not
stylistic: ``json.dumps(default=str)`` renders an unknown object by ``repr``,
and a ``set``'s ``repr`` depends on ``PYTHONHASHSEED``, so a key built from one
differs between processes. The cache then never hits, and nothing reports it,
because an unreachable cache is indistinguishable from a cold one.

**Field names are unique to what they identify.** Both a resource and a
partition geometry can carry something called a hash, meaning different things.
Namespacing (``resource.hash``, ``partition.hash``, ``fileset.paths``) means
composing two identities cannot silently drop a field.

The registry
~~~~~~~~~~~~

:func:`~cstar.orchestration.cache_keys.register_identity` — or the
:func:`~cstar.orchestration.cache_keys.identity_for` decorator — associates a
*subject shape* with a scheme and an identity function. A shape is one type, or
a tuple of types when a key is composed from several values taken together.

Lookup walks each type's method resolution order, so a subclass keys like its
base unless it registers something of its own.

The registry is what keeps ``cache_keys`` free of the types it keys. ROMS/MARBL
partition geometry is registered within a `ROMS-MARBL` :ref:`custom application<custom_applications>`
in ``cstar.applications.roms_marbl.cache``, not in orchestration; importing that package
performs the registration, so using a ROMS type implies its keys exist. Anything outside
this package becomes keyable the same way, without orchestration learning it exists.

Choosing a key
~~~~~~~~~~~~~~

Take the first that fits.

+--------------+--------------+--------------+--------------+--------------+
|              | Mechanism    | Addressed by | Cost to      | Catches      |
|              |              |              | check        |              |
+==============+==============+==============+==============+==============+
| 1            | ``@cached``, | Declaration  | stat         | Any declared |
|              | ``re         |              |              | input        |
|              | source_key`` |              |              | changing     |
+--------------+--------------+--------------+--------------+--------------+
| 2            | ``@id        | Declaration  | stat         | Whatever the |
|              | entity_for`` |              |              | function     |
|              | on your own  |              |              | reports      |
|              | type         |              |              |              |
+--------------+--------------+--------------+--------------+--------------+
| 3            | ``f          | Member paths | stat         | Files added, |
|              | ileset_for`` |              |              | removed,     |
|              | +            |              |              | moved        |
|              | ``cac        |              |              |              |
|              | he_fileset`` |              |              |              |
+--------------+--------------+--------------+--------------+--------------+
| 4            | ``ingest``   | Nothing      | stat         | Divergent    |
|              | with a       |              |              | content, at  |
|              | chosen name  |              |              | promotion    |
+--------------+--------------+--------------+--------------+--------------+

Rungs 3 and 4 are weaker on purpose, and the weakness is documented where it is
used rather than hidden.

A :class:`~cstar.orchestration.caching.FileSet` describes files that already exist
and have no declaration to key on. It is keyed on its members' **absolute paths**,
which is sound on a shared filesystem where a path names the same bytes for every
user, and costs a directory walk rather than a pass over the data. It cannot see an
in-place edit: same paths, same key, stale contents. Anything revised under a stable
path belongs on rung 1 or 2.

Rung 4 is the escape hatch, and is less exposed than it sounds — names are
validated as single path components, writes reach the user tier only, and
promotion refuses a name already holding different bytes. What no mechanism can
supply is a record of *what produced it*, so nothing catches two configurations
that share a name today and diverge after a code change.

One rule cuts across all four: a key must be a pure function of what determines
the output, and of nothing else. Too much in a key wastes a recompute; too
little returns the wrong file. **Omitting is the dangerous direction.** And
because a stale key looks exactly like a cold cache, key scope never surfaces
on its own — the work simply runs again, silently, and nobody files a bug.

Set artifacts
-------------

A partition produces 128 files that are only ever used together. Treating them
as one artifact keeps shared cardinality at one entry rather than 128, and
makes the collection the unit of reuse it actually is.

**Archive in the shared tier, expanded directory in the user tier.** The client
always sees a directory.

Measured on a 128-member, 25 MB set:

==================== ====== =============
Operation            Wall   Metadata ops
==================== ====== =============
Copy 128 files       125 ms 256
Copy one archive     113 ms 2
Pack + copy + expand 387 ms 2 (copy step)
==================== ====== =============

End to end the archive is slower, because packing and expanding are real work.
The number that matters on a parallel filesystem is the copy step in isolation:
per-file ``open`` is what saturates a metadata server, and 2 operations versus
256 is a difference that grows with cache size and concurrent readers. The
shared tier also keeps every guarantee it has for files — atomic
``os.replace``, a single fingerprint, unchanged deletion — because a shared
aggregate *is* one file.

Container format
~~~~~~~~~~~~~~~~

Uncompressed tar. NetCDF is usually compressed internally, so compressing again
buys little and costs CPU on every pack and expand.

Entry metadata is **normalised**: ``mtime``, ``uid``, ``gid``, ``uname`` and
``gname`` zeroed, entries added in sorted order, PAX format. Without this, two
runs producing byte-identical members still produce different archives, because
tar records each member's mtime.

=============================================== =============
Archive of recreated but byte-identical members Digests match
=============================================== =============
Plain tar                                       No
Normalised tar                                  Yes
=============================================== =============

That makes the archive a content identity rather than a transport wrapper: two
containers can be compared by digest, and a rebuild is recognised as the same
artifact rather than a conflicting one.

The set manifest
~~~~~~~~~~~~~~~~

The :class:`~cstar.orchestration.artifact_cache.SetManifest` is written inside the
container as ``.cstar-set.json``, dot-prefixed so ``glob("*.nc")`` never sees it.
Members are relative POSIX paths, so a container may nest.

It does three jobs:

1. **Completeness.** A partition job killed at 100 of 128 files leaves a
   directory that looks plausible. A single file's "did you write something
   non-empty" check has no analogue for a set; the declared member count does.
2. **Atomicity gap.** A set cannot be published as atomically as a file. The
   manifest is what lets a reader tell a complete container from a half-written
   one.
3. **Cheap comparison.** A digest over the ordered member paths and digests
   lets two containers be compared without reading either in full.

Committing a container
~~~~~~~~~~~~~~~~~~~~~~

``os.replace`` cannot overwrite a non-empty directory, so a container is built
aside and swapped in:

1. build ``<name>.<pid>.tmp/``
2. verify member count and digests against what was written
3. rename any existing ``<name>/`` to ``<name>.<pid>.old/``
4. rename tmp into place
5. remove the old

Steps 3–4 are the only window. Readers holding open descriptors are unaffected,
and a reader resolving mid-swap sees either the old container or the new one —
never a partial one, because the tmp is fully built and verified first.

Retrieval
~~~~~~~~~

:meth:`~cstar.orchestration.artifact_cache.ArtifactCache.resolve` stays pure and
cheap: it stats and returns a location, which for a shared hit is the *archive*.
Callers wanting files use :meth:`~cstar.orchestration.artifact_cache.ArtifactCache.materialize`,
which expands the archive into the run's workspace when only the archive is present
and returns a directory either way.

Keeping expansion out of ``resolve`` matters — ``resolve`` runs on every
lookup, including misses, and must not acquire the right to write.

``materialize`` always returns a user-tier directory, so its own tier cannot
tell a caller whether the answer came from shared. Code that needs to know
should check before calling.

Fingerprinting
--------------

A :class:`~cstar.orchestration.fingerprinting.Fingerprinter` reduces a file to a digest
for performing comparisons. The mode that produced a digest is persisted alongside it, so
a later reader can tell whether two values are comparable.

-  :class:`~cstar.orchestration.fingerprinting.FullFingerprinter` — SHA-256 over every byte. **The default.**
-  :class:`~cstar.orchestration.fingerprinting.NullFingerprinter` — no digest. An explicit opt-out.
-  :class:`~cstar.orchestration.fingerprinting.QuickFingerprinter` — size plus leading and trailing blocks. Retained for
   fast tests, not for production use.

The default is full hashing because an artifact here may represent days of
compute, which makes minutes of hashing cheap next to reusing a corrupt one.
``NullFingerprinter`` does not merely skip work: without digests 
:meth:`~cstar.orchestration.fingerprinting.Fingerprinter.verify` returns ``None`` rather
than a verdict, and promotion cannot recognise a re-derivation as equivalent. Switching
verification off has to be typed out.

Writes
------

:meth:`~cstar.orchestration.artifact_cache.ArtifactCache.stage` is the primitive: it
yields a temporary path, and publishes under the real name only once the body returns
without raising. A job killed mid-write leaves no artifact a later run could mistake
for a complete one.

:meth:`~cstar.orchestration.artifact_cache.ArtifactCache.ingest` copies an externally
produced file; :meth:`~cstar.orchestration.artifact_cache.ArtifactCache.ingest_aggregate`
does the same for a directory, and its ``members`` parameter restricts what is taken —
so a directory holding ``a.txt`` and ``b.csv``, described with ``*.txt``,
yields a container holding ``a.txt`` alone.

**Committing does not overwrite by default.** Two steps in one run that happen
to choose the same name would otherwise replace each other silently, with no
error and no record that anything was displaced. Replacement stays available to
callers that mean it.

The ``@cached`` decorator
-------------------------

.. code:: python

   @cached(cache_factory=get_cache)
   def fetch_boundary(resource: VersionedResource, run_id: str) -> Path:
       path = workspace / "boundary.nc"
       download(resource.location, path)
       return path

The producer stays a plain function returning the path it wrote to, with no
knowledge of the cache, and is still directly callable in a test. The decorator
binds the call, finds the inputs among the arguments by type, derives the key
before the function runs, and calls the function only on a miss.

Two behaviours worth knowing. On a hit the caller receives the *cached* path,
not the path the function would have written to — that is what skipping the
work means. And a shared hit is copied into the run's workspace before its path
is returned, because a caller handed a path can write to it, and one client
editing a shared artifact in place would corrupt it for every other run on the
allocation.

Artifact shape follows the arguments: a registered companion among them means
the result is a directory of files, and without one it is a single file. A
mismatch between that and what the function returns is refused rather than
published under a name every reader would misread.

The decorator is deliberately small. It covers one resource in, one path out. A
step consuming several resources, or one whose output is not determined by its
declared inputs alone, should call the cache directly rather than be forced
through a decorator that cannot express it.

Bypass
------

``CSTAR_ARTIFACT_CACHE_BYPASS`` makes lookups report a miss regardless of what
is on disk, so client code recreates its inputs. Writes still happen and
overwrite, so the cache repopulates rather than being disabled — the semantics
are *refresh*, not *off*.

Reference tracking
------------------

Shared artifacts carry a reference log: a timestamped lease per consuming run,
recorded on the read path and debounced so a hot artifact does not generate a
write per read.

Leases rather than reference counts, and reporting rather than deletion.
Liveness here is *inferred* from voluntary registration, so a quiet artifact
may still have readers that never registered.
:meth:`~cstar.orchestration.artifact_cache.ArtifactCache.gc_candidates` reports
that nothing has touched recently; deletion stays a human decision.

What is not covered
-------------------

-  **Partial reads.** Reading one member without expanding its container. Tar
   is seekable enough for this, so it is addable without changing the format.
-  **Retention policy.** :meth:`~cstar.orchestration.artifact_cache.ArtifactCache.delete_user`
   is the mechanism for dropping a single artifact from a run; deciding *which*
   to keep is a caller concern.
-  **Contraction keys.** A key for joining a set back into one file. For a pure
   structural round trip this is degenerate — the join reconstructs the source,
   so the key names something that already exists. The useful case is joining
   simulation *output*, which is keyed from the simulation's own inputs and is
   not an aggregate-key concern.
