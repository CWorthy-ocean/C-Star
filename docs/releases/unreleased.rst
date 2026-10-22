.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- The ROMS-MARBL output file models and stream names (``RestartFile``, ``BoundaryFile``, ``CarbonateSensitivityFile``, ``OUTPUT_STREAMS``, ``OUTPUT_ROOT_NAME`` and related names) must now be imported from ``cstar.applications.roms_marbl.outputs``. (`#788 <https://github.com/CWorthy-ocean/C-Star/pull/788>`_)

  - Their old homes, ``roms_marbl.transforms``, ``roms_marbl.app`` and ``roms_marbl.file_system``, no longer re-export them.

- New attempts no longer write ``<stem>_original.bak``, ``<stem>.vars`` or their rotated ``.1``, ``.2`` copies in the run directory; read the attempt's ``attempts/<time>/`` copies instead. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)

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
- New ``cdr_metrics`` application writes the CDR efficiency and CO₂ uptake time series (``output/cdr_metrics.nc``) and a plot of them (``output/cdr_metrics.png``) from ROMS-MARBL ``_cdr`` output. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)

  - ``sources`` lists any number of directories or files, such as the segments of a ``continue-from`` chain; their records are stitched together and sorted by time.
  - A gap or overlap between consecutive averaging windows (a missing or repeated segment) fails the step and names the records.
  - Paths are given as ``location:`` entries, so relative paths resolve against the blueprint file and workplan placeholders such as ``{{output_dir: <step>}}`` work.

- ``domains`` maps a domain name to its grid and sources, ordered child to parent; one domain gives single-domain metrics. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Nested (upscaled) experiments: mark each parent domain with ``source_role: transfer``, and the domains are combined with roms-tools' ``UpscaledCDR``. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)

  - The CO₂ uptake of all domains is summed; the efficiency is normalised by the release domain's cumulative source only, so the transferred alkalinity is not counted twice.
  - ``cdr_metrics.nc`` then also holds each domain's uptake series and the ``closure_residual`` and ``<parent>_DIC_difference_inside_<child>`` diagnostics, and ``output/cdr_domains.png`` maps the domains.
  - Parents must be MARBL runs writing ``_cdr`` output; a CDR-lite parent (which writes ``_cdrtrc``) cannot be analysed.

- ``_cdr`` output lacking a variable the metrics need is reported with the ROMS setting that writes it; a release domain whose sources have the wrong sign fails with a report of where and how much. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- The metrics file records its provenance: domains and roles, grid files, number of ``_cdr`` files and the roms-tools version. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Each attempt that reads the workplan file carries its inputs into ``attempts/<YYYYMMDDTHHMMSS>/`` in the run directory: ``workplan.yaml`` (or ``workplan.json``), ``workplan.vars``, ``workplan_transformed.yaml`` and ``blueprints/<step>/<file>``. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)

  - The run manifest records each attempt's directory and a sha256 digest of each carried file, and points each step at its carried blueprint (``carried_blueprint``).
  - Each step record names the blueprint file the step ran (``blueprint_file``), such as its directive-applied override file.
  - A re-entry with ``--run-id`` alone or ``--resume`` replays the recorded plan and carries nothing.

- ``cstar run check`` compares carried inputs with their recorded digests: a changed file fails the check, and a missing file or deleted older attempt is listed for information. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- ``cstar run show`` lists the inputs directory of each attempt. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- ``cdr_metrics`` steps list their metrics series and figures in their step record, so ``cstar run show`` lists them and ``cstar run check`` reports either file if it goes missing. (`#793 <https://github.com/CWorthy-ocean/C-Star/pull/793>`_)

  - The series is an ``analysis`` product with role ``cdr_metrics``; the figure has role ``metrics``, and the map of nested domains has role ``domains``.
  - ``analysis`` is a new product kind for a dataset a step computes from other steps' output; the role says which analysis.

- Each step record says what ran the step (``execution``): the scheduler (``slurm`` or ``local``), the host and the process id. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)

  - Inside a SLURM job it also records the job id and the granted nodes, tasks, CPUs on the step's node, partition and account, read from the step's own environment.
  - A standalone ``cstar blueprint run`` inside an ``salloc`` records that allocation.

- The run manifest records each step's latest submission (``launcher``, ``job_id``, ``submitted_at``), written when the step is submitted. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)

  - ``launcher`` says whether ``job_id`` is a SLURM job id or the process id of a local step, also for a job that was killed before its step could write a record.
  - A step an attempt clobbers or resumes loses its earlier submission.

- ``cstar run show`` has a Jobs table with each step's job, how long it waited from submission to start, and its wall time. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)

  - The wait includes time a step was held for the steps it depends on; ``sacct`` can separate the two by job id.

- A run can be named by ``run://<uuid>`` or by its directory as well as by its run-id, in ``runs:`` aliases and in ``cstar run show``, ``check``, ``log``, ``path`` and ``cd``. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)

  - A run-id not tracked on this machine is looked up in the run directories of that name in your data home and the project runs area; two different runs under one run-id are an error that lists both.
  - A tracked run-id is always the tracked run; when the project runs area holds another run with that run-id, a warning names it and how to choose it.
  - When a tracked run's directory is gone, as after a scratch purge, a copy of the same run in the project runs area is used.

- A workplan can depend on a run that is not tracked on this machine, such as a copy or a promoted run, using the steps its step records say completed. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)

  - Scheduling pins each referenced run's run-id, uuid and start (and directory, when named by one) in the transformed workplan, and the run manifest records the uuid.

- ``cstar run ls --shared`` lists the project runs area and ``cstar run ls --all`` adds every run found in the run directories, with a ``where`` column. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)

  - The workplan builder's Refresh runs lists the same runs, named by ``run://<uuid>``.




Bug Fixes
~~~~~~~~~


- Two steps staging the same codebase at once could fail or log errors (a failed ``git pull``, a removed cache, "Directory not empty", or a ``cp -av`` exiting 1) because the shared clone cache had no lock. (`#787 <https://github.com/CWorthy-ocean/C-Star/pull/787>`_)

  - A step waits up to 30 minutes for another step to finish with the cache, logging that it is waiting, then clones the repository into its own directory instead.
  - On a file system without file-locking support, staging logs a warning and uses the cache without the lock, as before.

- ``cstar workplan run --resume`` no longer fails when it cannot update the run manifest, for example on a file system without ``flock``; it warns and resumes. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)

Improvements
~~~~~~~~~~~~


- ``ArtifactKind.namelist`` is now ``config``: every file the model read as configuration, told apart by ``metadata.role`` (``cppdefs``, ``namelist``, ``marbl_in``); ``build`` means a compiled executable. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- One module, ``roms_marbl.outputs``, now holds how a ROMS-MARBL step names its output files and the models that parse those names. (`#788 <https://github.com/CWorthy-ocean/C-Star/pull/788>`_)
- ``--resume <workplan>`` and re-runs under a derived run ID compare with the latest attempt's carried workplan; older attempt directories may be deleted by hand. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)

  - When that copy is gone too, ``--resume <workplan>`` warns and resumes the recorded plan, and a derived re-run is refused until ``--run-id`` is given.
  - An unreadable run manifest is reported as such instead of being treated as nothing recorded.

- An attempt refused while it prepares leaves the run as the previous attempt left it: the plan in the run directory is replaced only once the inputs are carried. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- A step reads the files of the step it depends on from that step's record by product kind, falling back to searching its ``output`` directory by name when the record lists none of that kind. (`#790 <https://github.com/CWorthy-ocean/C-Star/pull/790>`_)

  - ``continue-from`` reads the producer's restarts, or, from a step that recorded none, initial-conditions files named like restarts (what ``nest_ic`` writes).
  - ``nest-from`` reads the child boundary extraction, ``carbonate-sensitivity-from`` the gas-exchange sensitivities, the upscaler the ``_uscl`` files, and a deferred blueprint the emitted blueprint.
  - A recorded file that a step reads but that is missing on disk now fails the step with a pointer to ``cstar run check``, instead of the step silently using the files that remain.

- ``cdr_metrics`` reads the ``_cdr`` files a producing step recorded, instead of searching its ``output`` directory, falling back to the search for older runs and plain directories. (`#793 <https://github.com/CWorthy-ocean/C-Star/pull/793>`_)

  - A recorded ``_cdr`` file missing from disk now fails the step, naming the domain and pointing at ``cstar run check``, instead of the step silently using the files that remain.

- ``continue-from``, ``nest-from`` and ``carbonate-sensitivity-from`` read a step's recorded files only from its ``output`` directory, the same directory they search when the record lists none. (`#795 <https://github.com/CWorthy-ocean/C-Star/pull/795>`_)
- The upscaler resolves a linked ``uscl_file_location`` first, so it reads the record of the step the link points to and refuses a recorded ``_uscl`` file missing from disk. (`#795 <https://github.com/CWorthy-ocean/C-Star/pull/795>`_)
- One shared helper now reads a step's recorded files of a kind in one directory, replacing three copies of the same lookup. (`#795 <https://github.com/CWorthy-ocean/C-Star/pull/795>`_)
- Keeping a run's records never stops or stalls the run: a command that waits more than a minute for the manifest lock skips that update with a warning. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)
- YAML files are read with libyaml when PyYAML has it, about five times faster on a large run manifest. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)
- ``cstar run status``, ``gather`` and ``check --repair`` explain that they act on a run tracked on this machine and point at ``cstar run show`` or the run's directory for any other. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)



Miscellaneous
~~~~~~~~~~~~~

- The "Re-running a workplan" docs state the rule and its two escape hatches. (`#783 <https://github.com/CWorthy-ocean/C-Star/pull/783>`_)
- Docs: a "Declaring Products" section with the kinds table in the custom-applications guide, the products and product checks in the workplans guide, the Product term; the published run schemas' ``kind`` examples list ``config``. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- The hello_world e2e asserts empty products and no product findings; the Tier 2 forge-to-ROMS e2e asserts both records list their products, ``show`` totals are non-zero and ``check`` finds no product problem. (`#786 <https://github.com/CWorthy-ocean/C-Star/pull/786>`_)
- Workplans guide: new "Computing CDR metrics from segmented runs" section, including a note that flux-based metrics currently differ across a restart while DIC-based metrics match. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Published blueprint schema ``cdr_metrics`` 1.1.0. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Tier 2 e2e: the MARBL forge step gains an ALK release; two new ROMS steps form a ``continue-from`` chain; two ``cdr_metrics`` steps compute metrics from the whole run and from the segments. (`#773 <https://github.com/CWorthy-ocean/C-Star/pull/773>`_)
- Published the run manifest and step record schemas v1.1.0, and documented carried inputs in the workplan guide and terminology. (`#789 <https://github.com/CWorthy-ocean/C-Star/pull/789>`_)
- The directives guide gains a "Finding a step's files" section, and the deferred-blueprint note describes how the blueprint is found without ``filename``. (`#790 <https://github.com/CWorthy-ocean/C-Star/pull/790>`_)
- The wizard notebook (``cstar forge copy-notebook``) now links to the C-Star wizard documentation instead of the archived cstar-forge docs site. (`#792 <https://github.com/CWorthy-ocean/C-Star/pull/792>`_)
- The workplans guide describes how ``cdr_metrics`` finds its sources and records its products, and the custom-applications guide lists the ``analysis`` kind. (`#793 <https://github.com/CWorthy-ocean/C-Star/pull/793>`_)
- New tutorial "Automating your simple model with workplans": the end-to-end run as a two-step workplan with a deferred blueprint, with real ``cstar run status``, ``show`` and ``ls`` output, plus ``log``, ``path`` and ``cd``. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- New tutorial "Spin-up, continuation and a nested grid": a six-step workplan using ``blueprint_overrides`` (a shorter spin-up time step), ``continue-from``, an inline ``nest_ic`` step and a two-source ``nest-from``, with a roms-tools plot of the nested grids and a screenshot of the wizard's nesting controls. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- Ready-made forge blueprints for the wio-toy outer grid and a 2000 km child grid nested in it, with tides and rivers off so no TPXO download is needed. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- The end-to-end tutorial is retitled "Using Blueprints to Set Up and Run an Ocean Model" and points to the workplan page as its next step. (`#791 <https://github.com/CWorthy-ocean/C-Star/pull/791>`_)
- Published the run manifest and step record schemas v1.2.0, and documented execution facts in the workplan guide and terminology. (`#794 <https://github.com/CWorthy-ocean/C-Star/pull/794>`_)
- Published the workplan schema v1.1.0 and the run manifest schema v1.3.0, and documented run handles, extending a finished run, the project runs area and ``ls --shared``/``--all``. (`#796 <https://github.com/CWorthy-ocean/C-Star/pull/796>`_)
