# Development notes (historical)

This directory holds engineering process artifacts — planning documents, audits, and
inventories written while the resolver/wizard/forge decomposition was being carried out.
They are kept as historical records of *why* the architecture looks the way it does, and
are **not** maintained as current documentation.

For the current architecture, module map, and end-to-end call chain, see
[`docs/developer-guide.md`](../developer-guide.md).

Contents:

- `architecture-decomposition-plan.md` — plan for splitting the old monolith into
  resolver / wizard / forge application (completed).
- `executor-portability-plan.md` — plan for making the executor host-agnostic (completed;
  a few known gaps are flagged inline and in the developer guide).
- `forge-blueprint-inventory.md` — the original inventory of configuration
  sources-of-truth that motivated the `ForgeBlueprint` design (superseded).
- `forge-blueprint-parameter-audit.md` — parameter-by-parameter audit of the blueprint
  schema at a point in time (schema has since changed; see the developer guide).
- `roms-tools-options-integration.md` — engineering log of the roms-tools options
  passthrough work (historical record by design).
