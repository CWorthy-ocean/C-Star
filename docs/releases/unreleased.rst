.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- The ROMS-MARBL output file models and stream names (``RestartFile``, ``BoundaryFile``, ``CarbonateSensitivityFile``, ``OUTPUT_STREAMS``, ``OUTPUT_ROOT_NAME`` and related names) must now be imported from ``cstar.applications.roms_marbl.outputs``. (`#788 <https://github.com/CWorthy-ocean/C-Star/pull/788>`_)

  - Their old homes, ``roms_marbl.transforms``, ``roms_marbl.app`` and ``roms_marbl.file_system``, no longer re-export them.

New features
~~~~~~~~~~~~


- Re-entering a run refuses a step that reached ``Done`` under a different definition, naming the steps and the exact ``--clobber <step>`` to pass, or ``--run-id <other-id>`` to start a new run. (`#783 <https://github.com/CWorthy-ocean/C-Star/pull/783>`_)

  - A step's definition covers its blueprint file's content, ``blueprint_overrides``, ``directives``, dependencies and application; runtime ``--var`` values, ``compute_overrides`` and ``workflow_overrides`` are not part of it.
  - A step whose producer changed through ``from_step`` is named too; a step new to the workplan, a step that failed, or one recorded by an earlier release without a definition hash, is not checked.
  - A run manifest that cannot be read also refuses the re-entry; one written by a newer release keeps the "upgrade cstar-ocean" guidance.

- Step records list each step's products: kind, path relative to the step, size in bytes and metadata (ROMS stream, timestamp and output format; the roms-tools sidecar of each forge input; the role of each configuration file). (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)

  - roms_marbl records joined outputs (``restart``, ``history``, ``average``, other streams as ``output``), the generated namelist, the staged ``cppdefs.opt`` and ``marbl_in`` (``config``), the compiled executable (``build``) and the log.
  - A ROMS stream another step consumes is a kind of its own, so the consumer can find it by kind: ``gas_exchange`` (``_cdrgas``, read by ``carbonate-sensitivity-from``), ``upscaling`` (``_uscl``, read by the upscaler), ``cdr_output`` (``_cdr``); the boundary extraction a nested run reads is ``boundary_forcing`` with role ``child``.
  - forge records the published blueprint, the generated inputs by kind (grid, initial conditions, boundary, surface, tidal, river and CDR forcing), its three rendered configuration files and the log; nest_ic, upscaler and plotter record their one output each.

- ``cstar run check`` verifies the recorded products against the run directory: a missing or resized product fails the check. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)

  - ``--verify`` also compares content where a product records a sha256 (nothing records one yet, and the report says so).
  - A log is reported only when it shrank: a step's own log is still written after its record is taken.
  - A done step without a step record and a latest attempt that has not finished are reported for information only.
  - For a run-id the manifest is refreshed first, as ``cstar run show`` does; ``--no-refresh`` reads it as last recorded.
  - A directory holding ``cstar-run.yaml`` but no ``tasks/`` (a standalone ``cstar blueprint run``) is accepted as a target.

- Custom applications declare their products by overriding ``artifacts()`` on their ``ApplicationDefinition`` with glob patterns per ``ArtifactKind``; a runner may add metadata to a product while it runs. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)

Bug Fixes
~~~~~~~~~


- Two steps staging the same codebase at once could fail or log errors (a failed ``git pull``, a removed cache, "Directory not empty", or a ``cp -av`` exiting 1) because the shared clone cache had no lock. (`#787 <https://github.com/CWorthy-ocean/C-Star/pull/787>`_)

  - A step waits up to 30 minutes for another step to finish with the cache, logging that it is waiting, then clones the repository into its own directory instead.
  - On a file system without file-locking support, staging logs a warning and uses the cache without the lock, as before.

Improvements
~~~~~~~~~~~~


- ``ArtifactKind.namelist`` is now ``config``: every file the model read as configuration, told apart by ``metadata.role`` (``cppdefs``, ``namelist``, ``marbl_in``); ``build`` means a compiled executable. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- One module, ``roms_marbl.outputs``, now holds how a ROMS-MARBL step names its output files and the models that parse those names. (`#788 <https://github.com/CWorthy-ocean/C-Star/pull/788>`_)

Miscellaneous
~~~~~~~~~~~~~

- The "Re-running a workplan" docs state the rule and its two escape hatches. (`#783 <https://github.com/CWorthy-ocean/C-Star/pull/783>`_)
- Docs: a "Declaring Products" section with the kinds table in the custom-applications guide, the products and product checks in the workplans guide, the Product term; the published run schemas' ``kind`` examples list ``config``. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- The hello_world e2e asserts empty products and no product findings; the Tier 2 forge-to-ROMS e2e asserts both records list their products, ``show`` totals are non-zero and ``check`` finds no product problem. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
