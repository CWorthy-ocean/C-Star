"""Host contract for the forge application.

``HostPaths`` is the plain-data handle the forge application receives at run time to
know *where it is executing*: the per-run artifact root (``working_dir``), the shared
source-data download cache, and the machine identity. It is **host-independent contract,
not resolution** — the app never detects the machine itself; a provider builds a
``HostPaths`` and injects it at the entry point (``process_forge_blueprint(..., host=...)``).

- Everything the executor PRODUCES (input netCDFs, namelist, cppdefs, the emitted
  roms_marbl blueprint, build dirs) lands under ``working_dir``.
- Everything it READS that isn't in the ForgeBlueprint is a *cache* location:
  ``source_data_cache`` (big reference downloads, shared across runs).

Providers: in this repo, Forge's disposable ``cstar_forge.config.resolve_host`` /
``cstar_forge.run`` build it (``working_dir`` seeded from the spec's default, rebased
onto host scratch on HPC systems, overridable per host). C-Star builds an equivalent on
relocation. This module travels with the app; the resolver does not. Kept
dependency-light (stdlib only).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HostPaths:
    """Where the forge application runs."""

    working_dir: Path  # per-run artifact root — all produced outputs live under here
    source_data_cache: Path  # shared download cache for source datasets
    system: str

    def summary(self, casename: str | None = None) -> str:
        """A human-readable one-block summary of the resolved host."""
        lines = [f"Host: {self.system}"]
        lines.append(f"  working_dir       -> {self.working_dir}")
        lines.append(f"  source_data_cache -> {self.source_data_cache}")
        if casename is not None:
            lines.append(f"  casename          -> {casename}")
        return "\n".join(lines)
