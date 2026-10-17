.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- A workplan ``name`` must be slug-stable (``north-atlantic-2012``, not ``North Atlantic 2012``); a non-slug name is refused with the slug to use, which is the run-id that name already produced, so existing runs stay reachable. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

  - A slug is ASCII: lowercase letters, digits, ``_`` and single ``-``; ``slugify`` now reduces accented letters to their base letter (``Étude Ü`` gives ``etude-u``) instead of keeping them, which changes the derived id only for names that contained non-ASCII characters.
  - The bundled workplan templates and the docs examples are renamed accordingly, and the wizard's name field says so.
  - The workplan schema version stays 1.0.0; this is the only narrowing of what a valid workplan file may contain.
  - The transformed workplan saved in the run directory keeps the workplan ``name`` instead of being renamed to ``<name> (transformed)``; files written by earlier releases still load under the run-id they produced, and ``cstar workplan ls`` shows the name without the old suffix.

- Re-running a different workplan file under a run-id derived from the same name is refused unless its content matches the run's recorded original; pass ``--run-id`` to choose another id or to re-enter deliberately. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

  - Editing the recorded file in place (adding steps as a run grows) never trips the guard, and one ``--run-id <id>`` re-entry re-records the new file and original, so later runs need no flag.

- ``cstar workplan status <unknown-id>`` exits with status 2 instead of printing a message and exiting 0. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan run --resume`` no longer re-exports the ``CSTAR_DATA_HOME`` and ``CSTAR_STATE_HOME`` captured when the run started; a reloaded run resolves against the current ones, and one warning names any other captured ``CSTAR_*`` setting that differs from the shell. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- A relative ``location:`` in a blueprint now resolves against the directory of the blueprint file, not the current working directory. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)

  - A location that exists only relative to the working directory is refused, with both candidate paths named.
  - A blueprint fetched from a URL keeps the old meaning: its relative locations resolve against the directory ``cstar`` is run from.

- ``ROMSInputDataset`` no longer accepts a ``linker``; the unused ``DatasetLinker`` class is removed. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)
- ucla-roms refs that aren't release tags (e.g. ``main``, other branches) now select the 0.11.0 namelist schema, which always writes ``use_constant_fe`` and ``constant_fe_value``; a ucla-roms build without #391 stops on them. (`#774 <https://github.com/CWorthy-ocean/C-Star/pull/774>`_)

  - Pin a release tag (e.g. ``0.10.1``) to keep the 0.9.0 schema.

- Forge→roms_marbl workplans that pin ucla-roms by a commit hash at or after 0.9.0 now fail when the roms_marbl step reads Forge's namelist. (`#774 <https://github.com/CWorthy-ocean/C-Star/pull/774>`_)

  - Forge cannot resolve a hash to a release and uses the newest schema, while the roms_marbl step resolves it to the nearest tag; pin a release tag instead until this is fixed.

- C-Star now requires roms-tools 5.2.1 or later; update existing environments before installing this version. (`#782 <https://github.com/CWorthy-ocean/C-Star/pull/782>`_)

  - Boundary BGC with ``density`` or ``density_mld`` interpolation (e.g. the bundled ``glorys-era5-glodap-woa`` forcing) is now written on the physics time axis in grouped files instead of one ``_clim`` file per source, about 36 BGC boundary files per simulated year.
  - A ``_clim`` boundary file left over from an earlier run still counts as already present on a rerun without clobber; remove it or rerun with clobber.
  - ESPER-derived O2 is floored at 2.0 mmol/m³ instead of 0, with or without salinity conditioning.

- The bundled ``glorys-era5-esper`` forcing spec turns salinity conditioning on. Re-resolving it changes IC and boundary ALK, DIC, NO3, PO4, SiO3 and O2 wherever the model salinity is below 34 PSU (measured on the 12 km Pacific 2010 IC and January/July boundaries: silicate blow-ups and DIC-above-alkalinity removed, nitrate zeros down to the in-range rate; above 34 PSU unchanged), requires the CWorthy PyESPER fork at tag ``v1.1.1+cworthy.1`` or later (installed from a clone; not on PyPI or conda), and downloads a ~25 MB WOA23 file. (`#764 <https://github.com/CWorthy-ocean/C-Star/pull/764>`_)



New features
~~~~~~~~~~~~


- ``cstar workplan run --resume <id>`` now explains that ``--resume`` takes no value and shows the ``--run-id <id> --resume`` form instead of "workplan not found". (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan run <run>/<stem>_transformed.yaml`` re-enters that run exactly as ``--run-id <id>`` does, instead of failing validation; the run-id comes from the run directory holding the file, so a run started under an explicit ``--run-id`` is found too. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)

  - A copy of the file anywhere else is refused with the ``--run-id`` way in, and ``--pre-run`` with a transformed workplan is refused with a message asking for the authored one.

- Orphaned step directories and sentinels left by renamed or removed steps are reported on re-entry, listed with their paths and left in place. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- A run directory loads from wherever it sits: the transformed workplan records its run root (``root``), and every reader rebases the paths under that root onto the directory the file is read from; files written by earlier releases carry no root and load exactly as before. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)

  - Moving one run between data homes on the same machine works end to end: move ``workplan_runs/<id>`` from the old ``CSTAR_DATA_HOME`` to the new one and point ``CSTAR_DATA_HOME`` at it; ``status``, ``path``, ``gather``, ``continue-from`` and ``--resume`` all resolve there.
  - A run record follows a run only when its recorded directory is gone and the current data home holds ``workplan_runs/<id>``; a run copied to an arbitrary path, or to another machine, has valid internal paths but is not reachable by run-id until ``attach`` (Phase 2).

- New ``cstar run`` command group for inspecting and managing existing runs: ``status``, ``log``, ``gather``, ``path``, ``cd``, ``ls`` and ``check``. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)

  - The ``cstar workplan <verb>`` spellings keep working unchanged and print no notice; which spelling becomes canonical is deliberately left open.
  - ``cstar run cd`` needs the shell function regenerated with ``cstar env shell-init --install``; ``cstar workplan cd`` keeps working either way.

- ``cstar run check <run-id|path>`` reports absolute paths inside a run's reference files, absolute or dangling symlinks, links pointing outside the run, missing step directories, and orphaned step directories or sentinels. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)

  - ``--repair`` rewrites in-run symlinks as relative links and rebuilds the gathered output; ``--all`` also scans launch artifacts that are regenerated on every launch.
  - Exit code 0 when clean, 1 when findings remain, 2 for bad arguments.

- Forge and roms_marbl blueprints can hold MARBL's dissolved iron constant on ucla-roms >= 0.11.0 with ``use_constant_fe`` and ``constant_fe_value`` (mmol/m³, default 3.0e-3). (`#774 <https://github.com/CWorthy-ocean/C-Star/pull/774>`_)

  - In Forge settings they live under ``marbl_bgc``, and in roms_marbl ``namelist_overrides`` under ``marbl_biogeochemistry_settings``, spelled in lowercase.
  - Forge rejects ``use_constant_fe: true`` on a ucla-roms release older than 0.11.0, or in a build without MARBL, before generating any inputs.
  - Namelists and stored blueprints without the keys still validate, using the ucla-roms defaults (option off).

- New bundled ModelSpec ``roms-marbl-0.11-default`` (ucla-roms 0.11.0) is now the wizard's default model. (`#774 <https://github.com/CWorthy-ocean/C-Star/pull/774>`_)
- The wizard's Advanced settings show "Hold iron constant" and "Constant iron concentration"; the concentration field appears only while the option is on. (`#774 <https://github.com/CWorthy-ocean/C-Star/pull/774>`_)
- Forge accepts the ``upscaled`` CDR mode with ``bgc_mode: cdr_lite`` on ucla-roms >= 0.11.0, so a parent without MARBL can receive a child's upscaled CDR-lite fluxes. (`#777 <https://github.com/CWorthy-ocean/C-Star/pull/777>`_)

  - State the parent's CDR-lite tracer counts with ``run_time_overrides={"param": {"nt_cdr_oae": ..., "nt_cdr_dor": ...}}``, matching the child's tracers; Forge rejects the blueprint without them.
  - Forge rejects the combination on ucla-roms releases before 0.11.0, and in physics-only builds on every release.

- The upscaler writes the child's CDR-lite tracer boundary fluxes into the parent's CDR forcing after ALK and DIC, with a ``cdr_trc_name`` variable naming each column. (`#777 <https://github.com/CWorthy-ocean/C-Star/pull/777>`_)

  - Output from children without CDR-lite tracers is unchanged, and ucla-roms releases before 0.11.0 still read the first two columns as ALK and DIC.
  - A CDR-lite tracer missing from one of the child's open boundaries is left out, with a warning.

- Every workplan run keeps a run manifest, ``cstar-run.yaml``, in its run directory: a unique id minted once per run directory, one attempt per scheduling, one entry per step, and what the run depends on outside itself. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)

  - Each attempt records its start time, the system it ran on, the ``cstar-ocean`` and ``roms-tools`` versions, the steps it clobbered or resumed, whether it was a pre-run, and when it finished.
  - Each step entry records a definition hash of what the step computes, the blueprint it ran, its status when last observed, and its step record once it has one.
  - The dependencies cover the runs referenced under ``runs``, source-data locations, codebases with their commit or branch, and the catalog entries and producer blueprints named in the blueprints' provenance.

- Every step writes a step record, ``cstar-step.yaml``, in its working directory when it finishes: outcome, start and finish times, system, package versions and errors; a standalone ``cstar blueprint run`` writes both files in its working directory. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- ``cstar run show <run-id|directory>`` prints a run's manifest; ``--format table|yaml|json`` picks the output and ``--no-refresh`` shows it as last recorded. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)

  - A directory holding a manifest is read without tracking, so a run copied from another machine can be inspected with ``cstar run show /path/to/copy``.
  - ``cstar run status``, ``cstar run show <run-id>`` and ``cstar workplan run --resume`` refresh the manifest with the current step statuses and records, and close the latest attempt once every step is terminal; ``--no-refresh`` on ``status`` reads without writing.

- ``cstar blueprint run`` accepts ``--step <name>`` to record which workplan step a run executes; workplan steps pass it automatically. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- New bundled ForcingSpec ``glorys-era5-physics`` for ``bgc_mode`` none or cdr_lite. It combines GLORYS, ERA5, WOA SSS restoring, TPXO tides and DAI rivers, with no BGC. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)
- When the BGC mode changes, the wizard swaps an untouched default ForcingSpec for the matching one: ``glorys-era5-physics`` without MARBL, ``glorys-era5-unified`` with it. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)
- The wizard has a new Output card between Run and Advanced, holding the OutputSpec picker and every output stream. (`#780 <https://github.com/CWorthy-ocean/C-Star/pull/780>`_)

  - Main streams: physical state, surface flux, BGC and MARBL output, CDR, CDR-lite and CDR gas-exchange output. Child-grid extraction appears on parent grids and upscaled output on child grids.
  - Under a collapsed "More output streams": diagnostics, temperature/salinity, forcing, depth-slice, random, particle, pressure-flux and runtime-diagnostics output.

- ``SourceSpec.esper_salinity_conditioning`` (default off, ESPER-only), threaded through the resolver, engine, input data, the wizard (checkbox beside the ESPER method/equation) and the forge source docs. (`#764 <https://github.com/CWorthy-ocean/C-Star/pull/764>`_)
- ``WOA_SALINITY`` dataset handler: stages ``woa23_decav_s00_01.nc`` into the shared ``WOA`` folder (the same file the ``WOA_BGC`` handler stages as its annual salinity member). The resolver notes it whenever an ESPER source has the switch on. (`#764 <https://github.com/CWorthy-ocean/C-Star/pull/764>`_)





Bug Fixes
~~~~~~~~~


- Re-running a workplan overwrote the previous attempt's ``_original.bak``, ``_transformed.yaml`` and ``.vars`` in place; they are now rotated per attempt (``.1``, ``.2``, ...). (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- A ``.vars`` file from an earlier attempt survived a later attempt that passed no variables. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- ``cstar workplan check`` accepted an authored workplan with an invalid name by treating it as an already transformed one. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Attaching large user-provided netCDF files in the wizard, or verifying them at forge staging, no longer reads each file whole into memory (a login-node hang or kernel kill with tens of ``_cdrgas`` files). (`#769 <https://github.com/CWorthy-ocean/C-Star/pull/769>`_)
- Resuming a run from a shell that no longer exports the original ``CSTAR_DATA_HOME`` recorded an output directory under the wrong data home; the record now takes the root from the transformed workplan it was given. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- A transient HTTP error (e.g. GitHub rate limiting) while inspecting a remote input dataset no longer gets it rejected as "a text file (e.g. a roms-tools YAML)"; C-Star now reports the HTTP error. (`#770 <https://github.com/CWorthy-ocean/C-Star/pull/770>`_)
- Fetching a remote workplan or blueprint no longer times out after 1 second; timeouts and connection failures are reported as "Unable to retrieve remote file" like other retrieval failures. (`#770 <https://github.com/CWorthy-ocean/C-Star/pull/770>`_)
- A Forge settings key that matched a setting only up to case, or by its ROMS namelist name, was silently ignored, so the run used the default instead of the value written. (`#775 <https://github.com/CWorthy-ocean/C-Star/pull/775>`_)

  - Such a key in a ModelSpec, OutputSpec, ForcingSpec, override or stored Forge blueprint now fails validation, e.g. ``LLm (did you mean llm?)`` or ``akv_bak (did you mean akv?)``.
  - All offending keys are reported in one error, before any input data is generated.
  - Version-gated settings get the same check on every ucla-roms release: a case variant of ``use_constant_fe``, ``nt_cdr_oae`` or a newer section's name is rejected even where that release lacks the setting.

- On ucla-roms >= 0.11.0, the upscaled CDR mode wrote ``cdr_nz_chd = 50``, which aborts at init unless the child grid has exactly 50 levels; Forge now writes 0, which tells ROMS to read the level count from the forcing file. (`#777 <https://github.com/CWorthy-ocean/C-Star/pull/777>`_)
- Blueprint paths and step names containing spaces no longer break the launched step command: both launchers now shell-quote every token of the command they render. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- Turning on CDR output or upscaled output in the wizard without MARBL no longer saves a blueprint that only fails at ``cstar forge run``. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)

  - When the BGC mode is none or cdr_lite, the wizard hides settings that need MARBL: BGC/MARBL output, CDR and CDR gas-exchange output, upscaled output, NHY/NOX forcing, and the online carbonate sensitivity.
  - Edits to hidden settings are kept and come back when MARBL is selected again.

- The wizard's "Compute dt (CFL)" button works while other settings are still incomplete, e.g. before a cdr_lite run has its CDR forcing. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)
- Forge → roms_marbl runs pinned to a ucla-roms commit hash no longer fail when roms_marbl reads the namelist: Forge now writes the namelist for the release the commit descends from. (`#781 <https://github.com/CWorthy-ocean/C-Star/pull/781>`_)

  - A hash Forge cannot resolve (offline, private remote, commit on no fetched ref) warns and keeps the newest namelist version, as before.
  - A hash ahead of its release gets that release's namelist in both steps; to use unreleased namelist keys, pin a branch instead.
  - Upscaled CDR forcing pinned by hash gets the ``cdr_nz_chd`` of that release: 0 (level count from the file) from 0.11.0, the seeded value before.

- Plotting an attached cdr_lite or passive-tracer CDR-forcing netCDF in the wizard no longer fails with "Unknown tracer name(s) … for tracer_set='marbl'". (`#778 <https://github.com/CWorthy-ocean/C-Star/pull/778>`_)
- The wizard's tracer flux plot now shows each cdr_lite or passive release's own tracer instead of failing on a missing ALK tracer. (`#778 <https://github.com/CWorthy-ocean/C-Star/pull/778>`_)

  - In a forcing that mixes MARBL and cdr_lite or passive releases, a MARBL release's ALK flux plot now shows only that release.

- Simple-mode CDR plots under ``bgc_mode: cdr_lite`` now show the cdr_lite release that will actually be generated. (`#778 <https://github.com/CWorthy-ocean/C-Star/pull/778>`_)
- Forge boundary and initial-condition steps with ESPER or density-interpolated BGC no longer grow memory without bound while saving, and density boundaries follow each year's physics instead of one sampled day (roms-tools 5.2.0). (`#782 <https://github.com/CWorthy-ocean/C-Star/pull/782>`_)
- The ESPER preflight validates the source block as roms-tools will receive it (after the staged path is substituted), not the raw blueprint switch. (`#764 <https://github.com/CWorthy-ocean/C-Star/pull/764>`_)



Improvements
~~~~~~~~~~~~


- The ``--run-id`` help text says the id defaults to the workplan name. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Looking up whether a mistyped path is a run-id no longer creates lock files or reads run records. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- A user-provided file's ``content_hash`` is now ``nc-sample-v1:<sha256>``: the netCDF structure, variables up to 1 MiB in full, and up to 64 evenly spaced rows (always the first and last record, at most 2 MiB) of each larger variable, independent of format, chunking and compression. (`#769 <https://github.com/CWorthy-ocean/C-Star/pull/769>`_)

  - A blueprint recorded with the earlier full-content digest, or with a scheme this C-Star does not know, is accepted with one warning that the file was not verified; re-saving the blueprint refreshes it.
  - The wizard's attached-file status and the staging mismatch warning show the digest's hex rather than its scheme tag.

- ``LiveWorkplan.load``, ``try_load`` and ``load_all`` replace ad-hoc deserialization at every reader of a transformed workplan; ``rebase_paths``, ``read_all`` and ``read_document`` are shared helpers. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- Following a moved run tolerates directories that cannot be inspected and runs off the event loop when listing runs. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- Forge writes the emitted ROMS-MARBL blueprint with locations relative to its own directory (``../input_data/...``, ``../builds/...``), so a forge run can be moved or copied as a unit. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)
- The generated ROMS namelist names inputs relative to ``work/`` (``../input/input_datasets/...``), keeping names short for ROMS's 99-character filename buffers; a source outside the run keeps its absolute path. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)
- Symlinks C-Star creates (staged inputs, ``work/roms``, the tracking ``latest`` pointer, the legacy source-data link) are relative when both ends sit in the same tree and absolute otherwise. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)
- A migrated blueprint copy written to another directory keeps naming the same files. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)
- Upscaled CDR runs now set ``cdr_relocate_to_wet_pts``: a child-boundary profile whose nearest parent point is land moves to the nearest wet point instead of aborting, and runs that already worked keep the same release points. (`#777 <https://github.com/CWorthy-ocean/C-Star/pull/777>`_)
- A workplan step whose local blueprint file is missing is refused when the workplan is prepared, naming the step and the path, instead of failing when the step launches. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- Every blueprint now has a content hash (metadata such as its name or working directory excluded), which the manifest and the forge provenance use to identify blueprints. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- Sentinels, the run manifest, step records and shell rc files are written through one atomic writer that never leaves a half-written file behind and no longer toggles the process umask. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- ``upscale_output.do_upscale`` without MARBL is now rejected before data staging, as ``cdr_output.do_cdr_output`` already was. ucla-roms compiles neither stream without MARBL. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)

  - Stored blueprints are checked for these switches, and for CDR gas-exchange output without MARBL, before generation starts.

- Under ``bgc_mode: cdr_lite`` the wizard shows the CDR-lite tracer output as always on, so its output period and records per file are visible and editable. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)
- The wizard's CDR-lite settings now have readable labels and hints instead of raw namelist names. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)
- The CDR output streams moved from the Advanced card's CDR pane to the Output card, and the Advanced card no longer has an output pane. (`#780 <https://github.com/CWorthy-ocean/C-Star/pull/780>`_)
- The nesting options in the Grid card now set the child-grid extraction period and the extraction settings that come from the child grid; they are no longer edited separately under Advanced. (`#780 <https://github.com/CWorthy-ocean/C-Star/pull/780>`_)
- If a BGC-mode, model or nesting change leaves the blueprint invalid, the wizard hides the old settings until the blueprint is valid again. One example is switching to cdr_lite before adding CDR forcing. (`#780 <https://github.com/CWorthy-ocean/C-Star/pull/780>`_)
- A blueprint whose ESPER source has the switch on but no ``WOA_SALINITY`` in ``datasets`` is rejected at load time, naming the one key to add. (`#764 <https://github.com/CWorthy-ocean/C-Star/pull/764>`_)
- The boundary forcing step prints, before constructing ``BoundaryForcing`` with BGC sources, that the whole run's physics T/S are read first and that no boundary file appears until that finishes (hours for a multi-year high-resolution run); roms-tools logs the same at INFO on the root logger, which C-Star's logging keeps off the terminal. (`#764 <https://github.com/CWorthy-ocean/C-Star/pull/764>`_)


Miscellaneous
~~~~~~~~~~~~~

- Docs: a "Re-running a workplan" section with the reuse rules (done steps reused unless clobbered, failed steps re-run unless resumed, in-progress steps adopted when the launcher matches, the collision rule, rotation, orphans), and the terminology entry for run ID. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Unit tests serve the bundled workplan templates for their GitHub URLs instead of fetching them from ``main``. (`#767 <https://github.com/CWorthy-ocean/C-Star/pull/767>`_)
- Forge blueprint reference and the ``UserProvidedFile`` docstring describe the new digest. (`#769 <https://github.com/CWorthy-ocean/C-Star/pull/769>`_)
- Docs: the workplans page describes the recorded root, which moves work by run-id today, and which wait for ``attach``. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- New end-to-end test ``test_workplan_relocation.py`` (hello_world steps, no ROMS build) that copies a whole data home, repoints ``CSTAR_DATA_HOME`` and exercises ``status``, ``path``, ``gather``, ``continue-from`` and ``--resume`` from the copy. (`#768 <https://github.com/CWorthy-ocean/C-Star/pull/768>`_)
- Unit tests no longer reach the network except for pooch-cached dataset downloads, and the unit CI workflow caches those downloads. (`#770 <https://github.com/CWorthy-ocean/C-Star/pull/770>`_)
- Docs: run-level commands documented under ``cstar run``, the anchoring rule on the HPC and blueprint pages, and the run-check functions on the orchestration API page. (`#766 <https://github.com/CWorthy-ocean/C-Star/pull/766>`_)
- The integration suite and the weekly extended matrix now pin ucla-roms 0.11.0; the integration namelist goldens gain the two iron keys. (`#774 <https://github.com/CWorthy-ocean/C-Star/pull/774>`_)
- The developer and ModelSpec docs describe the 0.11.0 schema tier and the new default. (`#774 <https://github.com/CWorthy-ocean/C-Star/pull/774>`_)
- The Forge spec docs now say that settings keys use Forge's lowercase names, not the ROMS namelist spelling. (`#775 <https://github.com/CWorthy-ocean/C-Star/pull/775>`_)
- The Forge internals developer guide describes the ucla-roms 0.11.0 upscaled-CDR rules. (`#777 <https://github.com/CWorthy-ocean/C-Star/pull/777>`_)
- Published schemas for the two new documents under ``docs/schemas/run/``, generated by the developer-mode ``cstar run schemas``. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- Documentation: "Showing a Run" and "The run manifest" in the workplans guide, glossary entries for run manifest, step record, attempt and product, a note on standalone runs in the blueprints guide, and the catalog design notes updated to the manifest that now exists. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- A hello_world end-to-end module covers the manifest after a run, the status refresh, a re-entry, a copied run and a standalone blueprint run. (`#772 <https://github.com/CWorthy-ocean/C-Star/pull/772>`_)
- Documented the physics-only ForcingSpec in the Forge specs guide. (`#779 <https://github.com/CWorthy-ocean/C-Star/pull/779>`_)
- The Forge internals developer guide describes how commit-hash pins choose a namelist version. (`#781 <https://github.com/CWorthy-ocean/C-Star/pull/781>`_)
- Documented the Output card in the wizard guide, with a new Output-card screenshot and refreshed overview and Advanced screenshots. (`#780 <https://github.com/CWorthy-ocean/C-Star/pull/780>`_)
