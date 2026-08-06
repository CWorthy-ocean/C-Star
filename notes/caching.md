# C-Star Caching System Requirements for Implementation Planning

## Goal

Add a generic caching system for expensive sub-step operations in C-Star so that file outputs can be reused instead of regenerated.

The system should support:

- a **personal, ephemeral cache** by default
- a **group, durable cache** for approved/published results
- **automatic reuse** of cached outputs when appropriate
- **developer-facing hooks** to opt functions/methods into caching
- **user-facing management tools** for inspect / promote / clear flows

This is still a **prototype-phase feature**. The priority is to cover new ground quickly and validate the architecture before hardening.

## Key Motivation

A first real use case is **Forge**, which already has a weak form of caching based only on output filenames. That is insufficient because filenames do not encode all meaningful inputs, so cached outputs can be wrong. This new system should replace that per-file ad hoc behavior with a more principled caching mechanism.

There was also explicit interest in trying a fairly complete AI-assisted first pass of the feature, then iterating on the result if needed.

---

## Core Functional Requirements

### 1. Scope and granularity

- Caching must work at the **sub-step level**, i.e. functions/methods, not only whole workplan steps.
- It should be a **generic capability** that developers can apply to many operations that generate files.
- Primary concern is caching **file-producing operations**.

### 2. Outputs

- Must support **1..N output files**.
- Must support returning enough information for downstream code to continue working normally.
- It will likely also need to support restoring or caching the **Python return value** when that simplifies control flow.
  - Example: if a function normally returns a path-like object, file collection, or some lightweight object, cached execution should still return something equivalent.
  - However, do **not** cache expensive Python objects unnecessarily if reconstructing them is easy.
- A useful design pattern discussed was a **cache handle** analogous to existing Slurm/local handles:
  - should expose paths to cached outputs
  - should expose metadata like creation time and cache provenance

### 3. Cache key behavior

- Cache key should **default to function arguments**.
- Developer must be able to:
  - add additional key components
  - exclude some function arguments from the key
- Cache keys must encode **all inputs that affect outputs**.
- Cache keys must be:
  - deterministic
  - computable without running the operation
  - not dependent on unstable runtime-only values like PIDs
- We need to be able to identify cache entries **after the fact**, without rerunning the function or regenerating the key from scratch.
  - This implies persisted cache records / metadata, not just recomputing key names ad hoc.

### 4. Cache storage layers

#### Personal cache
- Default cache target is **user-specific ephemeral storage**
- On HPC this should be **SCRATCH**
- Expected to be per-user

#### Group cache
- User can later **promote/publish/preserve** a personal cache entry into a **group-shared durable cache**
- On HPC this would likely be **PROJECT** or equivalent durable shared storage
- Group cache contains results that are considered approved / reusable by others

### 5. Cache lookup behavior

When a cached function runs:

1. If `--no-cache` is provided:
   - ignore all caches
   - generate fresh outputs
   - write them to the normal output location

2. Otherwise, when cache is active:
   - if **group cache** entry exists for the key, use that
   - else if **personal cache** entry exists, use that
   - else generate outputs, write them to **personal cache**, and record the cache entry

This was the intended priority in the meeting:
- **group cache first**
- then personal cache
- then regenerate

### 6. Output placement and symlink behavior

Preferred architecture from the meeting:

- write real files into the **cache storage**
- create **symlinks** into the user-facing output/FSM directory

This direction was preferred because the reverse arrangement is fragile:
- if real files live in the user output dir and cache points to them, users may delete the output dir and break the cache

Known tradeoff of the preferred design:
- if users copy the output directory, they may only copy symlinks rather than real data

This should be treated as an explicit design tradeoff, and docs/tools may be needed to help with it.

### 7. Promote / publish flow

- User must be able to **promote** a cache entry from personal cache to group cache
- Promote operation should:
  - move or copy the data into the group cache
  - create/update the group cache record for the associated key
- Promote must work **after a run completes**
- Therefore cache metadata must persist enough information to