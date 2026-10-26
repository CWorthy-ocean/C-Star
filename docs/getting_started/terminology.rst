Terminology and Concepts
========================

Applications and blueprints
---------------------------

.. glossary::

    Application
      Something C-Star can run: an ocean model, a data-processing tool, or an
      analysis. Each application declares the blueprint it accepts and knows
      how to turn one into a result. C-Star ships two applications for ocean
      modeling:

      * ``ucla_roms`` runs a UCLA-ROMS simulation: with MARBL biogeochemistry,
        without biogeochemistry, or with CDR-LiTE.
      * ``forge`` generates a new UCLA-ROMS domain: it produces the input
        files and settings for a simulation and writes the UCLA-ROMS
        blueprint that runs it.

      Smaller applications (``nest_ic``, ``upscaler``, ``cdr_metrics``,
      ``plotter``, ``hello_world``) support nesting and CDR workflows and
      serve as examples for :doc:`writing your own <../developers/custom_applications>`.
      See :doc:`../applications/index`.

    Blueprint
      A YAML file holding everything an application needs to produce its
      result. What it contains depends on the application: a UCLA-ROMS
      blueprint names the model code, input files, partitioning and runtime
      settings of one simulation; a forge blueprint describes the domain to
      build. Every blueprint shares a small core (``name``, ``description``,
      ``application``, ``working_dir``) from
      :class:`~cstar.orchestration.models.Blueprint`, and every application's
      blueprint is a subclass of it.

      Blueprints are complete and self-contained. The same blueprint produces
      the same result on any supported machine, and can be checked
      (``cstar blueprint check``) and run (``cstar blueprint run``) from the
      command line. See :doc:`../applications/blueprints`.

    UCLA-ROMS blueprint
      The blueprint of the ``ucla_roms`` application: a runnable
      description of one simulation. Forge writes one at the end of domain
      generation; you can also write one by hand for input files you already
      have. See :doc:`../applications/ucla_roms/blueprint`.

    Forge
      C-Star's domain-generation application. From a forge blueprint it
      downloads the source datasets, builds the grid and every ROMS input
      file (initial conditions, surface and boundary forcing, tides, rivers,
      optional carbon dioxide removal forcing), renders the model's
      compile-time and run-time settings, and emits the UCLA-ROMS blueprint
      that runs the result. Forge is driven by ``cstar blueprint run`` like
      any other application, with ``cstar forge run`` exposing its extra
      options. See :doc:`../applications/forge/index`.

    Forge blueprint
      The blueprint of the ``forge`` application: the single input to domain
      generation. It records the model configuration, grid, forcing
      selections, run window, output settings and resolved model settings
      for one domain, with the values written out in full rather than
      referenced, so it keeps working even if the catalog entries it was
      built from change. Usually written by the wizard. See
      :doc:`../applications/forge/blueprint`.

    Working directory
      Where an application writes its results. A blueprint's ``working_dir``
      names it; omitted, it is
      ``CSTAR_DATA_HOME/blueprint_runs/<application>/<name>``. A workplan
      lays out one directory per step under the run's directory,
      ``CSTAR_DATA_HOME/workplan_runs/<run id>/tasks/<step>``. See
      :doc:`../deployments/hpc`.

The catalog and the wizard
--------------------------

.. glossary::

    Spec
      A reusable, named piece of a domain description stored in the catalog.
      Forge composes a forge blueprint from four kinds of spec, and an
      optional fifth:

      * a **ModelSpec** pins the model code (ROMS, MARBL, optionally PIO),
        its render templates, and the model-level default settings;
      * a **DomainSpec** defines a grid: extent, resolution, vertical levels,
        topography source, open boundaries and processor layout;
      * a **ForcingSpec** selects the datasets for initial conditions and
        surface, boundary, tidal and river forcing;
      * an **OutputSpec** chooses which fields the model writes and how
        often;
      * a **CdrSpec** describes optional carbon dioxide removal forcing.

      The wizard presents specs as dropdowns, and can save your own edited
      versions under new names. See :doc:`../applications/forge/specs`.

    Catalog
      Where specs and saved blueprints live. A catalog is a stack of plain
      directory trees: the small catalog bundled with C-Star at the bottom,
      optionally a shared group catalog, and your own writable catalog on top
      (``~/cstar/catalog`` by default, or the first entry of
      ``CSTAR_CATALOG``). The wizard reads all layers and writes to yours.
      See :doc:`../catalog_wizard/catalog`.

    Wizard
      C-Star's point-and-click interface, served as a web page by
      ``cstar wizard`` or opened as a Jupyter notebook. Its Blueprint page
      builds a forge blueprint section by section (model, domain, forcing,
      run setup, output and advanced settings), shows the resolved YAML for
      review, and saves or downloads it. Its Workplan page composes steps
      into a workplan, checks it as you edit, and submits it. See
      :doc:`../catalog_wizard/launching`.

Workplans and their steps
-------------------------

.. glossary::

    Workplan
      A YAML file listing one or more **steps** to execute as a unit. Each
      step names an application and a blueprint, and may depend on other
      steps:

      * a step does not start until every step it depends on has completed
        successfully;
      * steps with no unmet dependencies may run at the same time.

      Steps can override values in their blueprint, pass information to one
      another through directives, and consume a blueprint that an earlier
      step generates. See :doc:`../workplans/index`.

    Step
      One unit of work in a workplan: an application, a blueprint, optional
      dependencies, overrides and directives. Each step gets its own
      directory for inputs, logs and output under the run's working
      directory.

    Directive
      A per-step instruction applied on the compute node just before the
      application starts, using information that only exists at run time.
      ``continue-from`` takes a restart file written by an earlier step as
      initial conditions; ``nest-from`` takes a parent run's output as the
      boundary forcing of a nested child; ``carbonate-sensitivity-from`` takes
      the carbonate sensitivities that an earlier UCLA-ROMS run with MARBL wrote as
      surface forcing for a CDR-LiTE run. See :doc:`../workplans/directives`.

    Orchestrator
      The process that executes a workplan. It builds a graph of the steps
      from their dependencies, submits each step as work becomes available,
      and monitors their status. On an HPC system it runs on a login node and
      submits steps to the job scheduler through a **launcher**; on a laptop
      it runs the steps as local processes. It is ephemeral and can be
      stopped and restarted without loss of state.

    Worker
      The process that executes a single blueprint on a compute resource. It
      reads the blueprint and runs the application exactly as the blueprint
      specifies. Each workplan step, and each ``cstar blueprint run``, is a
      worker.

    Run ID
      The identifier of a run. By default it is the workplan's ``name``, which
      must already be a slug (ASCII lowercase letters, digits, ``-`` and ``_``), so
      the same file always runs under the same ID; ``cstar workplan submit --run-id
      <id>`` overrides it. For ``cstar blueprint submit`` it is the blueprint's
      ``name`` made into a slug. It names the run's directory
      (``CSTAR_DATA_HOME/workplan_runs/<id>``), lets you check status (``cstar run status``) or reattach to a run
      in progress, and lets you re-enter a run to retry failed steps or
      resume them in place. A new run ID starts the workplan from scratch.
      Two different workplans with the same ``name`` share a run ID, so
      running the second one without ``--run-id`` is refused.

      Every run also has a uuid, the ``id`` in its run manifest. It is minted
      once and kept by every copy of the run: two runs can share a run ID, but
      never a uuid. ``run://<uuid>`` names the run by it, in your data home or
      the project runs area where its directory is kept, or wherever an
      attached run's directory is. See
      :ref:`workplan_external_runs`.

    Pre-run
      Performing every stage before the model launch -- staging inputs,
      cloning and compiling the codebases, partitioning inputs, and
      generating and validating the namelist -- and then exiting without
      launching the model (``--pre-run``). It leaves the working directory
      in place, so a later run of the same blueprint or step attaches to it
      and launches the model. Only applications that declare themselves
      pre-runnable support it. See :doc:`../runs/pre_run_resume`.

What a run leaves behind
------------------------

.. glossary::

    Run manifest
      ``cstar-run.yaml`` in a run's directory: the run's identity (its run ID
      and uuid), one **attempt** for each time it was scheduled, one entry per
      step with its latest submission, status and **step record**, and what the
      run depends on outside itself. The scheduling command writes it;
      ``cstar run status`` and ``cstar run show`` refresh it for a run
      scheduled on this machine; an attached run is read as it is. See
      :doc:`../runs/moving/manifest`.

    Step record
      ``cstar-step.yaml`` in a step's working directory, written by the step
      process when it shuts down: its outcome, start and finish times, system,
      package versions, what ran it (the scheduler, host and granted
      allocation), **products** and errors. The run manifest folds each
      step's record in. See :doc:`../runs/moving/manifest`.

    Attempt
      One scheduling of a workplan into a run: ``cstar workplan submit``, or a
      re-entry with ``--run-id``, ``--resume`` or ``--clobber``. The run
      manifest lists them oldest first; an attempt's ``finished_at`` is set
      once every step is terminal. An attempt that reads the workplan file
      carries its inputs -- the workplan as given, its variables, the plan it
      prepared and each step's blueprint -- into ``attempts/<start time>/`` in
      the run directory. See :doc:`../runs/moving/manifest`.

    Product
      A file a step left, listed in its step record with a kind (see the
      kinds table in :doc:`../developers/custom_applications`), its path, its size in bytes,
      a content digest once one is recorded, and metadata. See
      :doc:`../runs/moving/manifest`.

    Attach
      Registering a run directory that arrived from elsewhere in this
      machine's tracking with ``cstar run attach``. It writes a tracking
      record and never writes into the run: an attached run is inspected and
      referenced, not relaunched. It is read-only, which refuses the commands
      that write into its directory, when it is in the project runs area or
      belongs to another user, and owned otherwise. See
      :doc:`../runs/moving/attach`.

    Promote
      Placing a run in the project runs area (``cstar/runs`` under
      ``CSTAR_PROJECT_HOME``) with ``cstar run promote``: a copy of what its
      manifest names (records, logs and products; the large build and
      per-rank files on request), a link to the run where it is, or a move
      of the whole directory, after which the run is tracked as attached. The copy keeps the run's
      uuid. See :doc:`../runs/moving/promote`.
