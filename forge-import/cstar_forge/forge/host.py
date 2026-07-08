"""Host contract for the forge application.

``HostPaths`` is the plain-data handle the forge application receives at run time to
know *where it is executing* — the data directories, the machine tag, and the machine
config. It is **host-independent contract, not resolution**: the app never detects the
machine itself; a provider builds a ``HostPaths`` and injects it at the entry point
(``process_spec_config(..., host=...)``).

- In this repo, Forge's disposable resolver (``cstar_forge.config.resolve_host``) builds
  it from auto-detected ``cstar_forge.config``.
- When the app relocates into C-Star, C-Star builds an equivalent ``HostPaths`` from its
  own host resolution. This module travels with the app; the *resolver* does not.

Kept dependency-light (stdlib only) so it stays relocatable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class HostPaths:
    """Where the forge application runs: data dirs + machine identity.

    ``machine_config`` is intentionally opaque (duck-typed: ``.account`` /
    ``.pes_per_node`` / ``.queues`` / cluster info) so this contract need not import the
    Forge/C-Star machine-config type.
    """

    source_data: Path
    input_data: Path
    scratch: Path
    catalog: Path
    system: str
    system_id: str
    machine_config: Any = None

    def summary(
        self, casename: str | None = None, run_output_dir: str | None = None
    ) -> str:
        """A human-readable one-block summary of the resolved host."""
        lines = [f"Host: {self.system}"]
        mc = self.machine_config
        if mc is not None and getattr(mc, "account", None):
            lines.append(
                f"  account: {mc.account}  pes/node: {getattr(mc, 'pes_per_node', None)}"
            )
        for k in ("source_data", "input_data", "scratch", "catalog"):
            lines.append(f"  {k:11s} -> {getattr(self, k)}")
        if casename is not None:
            lines.append(f"  casename     -> {casename}")
        if run_output_dir is not None:
            lines.append(f"  run_output   -> {run_output_dir}")
        return "\n".join(lines)
