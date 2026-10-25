.. _ucla-roms-index:

UCLA-ROMS
=========

The ``ucla_roms`` application runs a `UCLA-ROMS
<https://github.com/CWorthy-ocean/ucla-roms>`__ simulation. It can run the
physics alone, the physics coupled to the `MARBL
<https://marbl-ecosys.github.io>`__ biogeochemistry model, or either with
CDR-LiTE, ucla-roms' lightweight set of carbon dioxide removal tracers. One
blueprint describes one simulation, and running it does everything from
fetching the model code to joining the output files.

.. toctree::
   :maxdepth: 1

   What it does <self>
   What's in the blueprint <blueprint>
   Example <example>

What it does
------------

Given a :doc:`UCLA-ROMS blueprint <blueprint>`, the application works through
the following stages. Each writes into the blueprint's working directory, so
when a stage fails you can look at what it left behind.

1. **Stage the inputs.** The model code (ucla-roms, and MARBL and ParallelIO
   when the blueprint asks for them), the compile-time and run-time files,
   and the input datasets are fetched from wherever the blueprint says they
   are: a git repository, a URL, or a local path. Only the datasets whose
   dates overlap the run window are fetched. Everything lands under
   ``input/`` in the working directory.
2. **Compile the model.** ucla-roms is compiled against the blueprint's
   compile-time files (``cppdefs.opt`` and friends), with MARBL and
   ParallelIO built first when they are in use. On a supported HPC system the
   build uses the site's compiler and MPI modules; on a laptop it uses the
   toolchain from the conda environment. The executable is kept with the
   compile-time code.
3. **Partition the inputs.** ROMS reads one file per MPI rank unless it is
   built with ParallelIO. Without ParallelIO, every input file is split into
   tiles according to the blueprint's ``partitioning`` layout; files that are
   already partitioned are used as they are. With ParallelIO
   (``use_pio: true``), the inputs are left whole and only checked for a
   compatible netCDF format.
4. **Fill in the namelist.** The run-time file the blueprint names is a
   template. C-Star keeps its scientific settings and fills in everything that
   depends on this run: the input file paths, the output location, the time
   step, the start and end dates. ``namelist_overrides`` in the blueprint are
   applied last, so a value you set there always wins. The generated
   ``namelist.nml`` is validated before the model starts.
5. **Pre-check.** C-Star checks that the pieces agree with one another before
   anything is launched: that every dataset was partitioned to the layout the
   namelist expects, that the model reference date of the inputs matches the
   namelist, and, for a CDR-LiTE build that reads carbonate sensitivities from
   files, that those files are present and complete.
6. **Run the model.** On a system with a job scheduler the model is submitted
   as a job, with the account, queue and walltime from your configuration or
   the workplan; otherwise it runs in place under ``mpirun``. The model's own
   log goes to ``logs/``.
7. **Post-run.** Without ParallelIO, ROMS writes one output file per rank into
   ``temp_output/``; C-Star joins them into whole files in ``output/`` and
   removes the pieces. With ParallelIO, ROMS writes the whole files directly
   and C-Star only checks that they are there. The step record
   (``cstar-step.yaml``) is written last, listing the outputs by stream so a
   later step, or ``cstar run check``, can find them.

The working directory then looks like this:

.. code-block:: text

   <working directory>/
     input/
       codebases/           the cloned ucla-roms (and MARBL, ParallelIO) sources
       compile_time_code/   cppdefs.opt and the other .opt files, and the compiled roms executable
       runtime_code/        marbl_in and the other run-time files
       input_datasets/      the input netCDF files, partitioned when ROMS needs them to be
     work/                  the generated namelist and other files C-Star uses to launch the run
     logs/                  the model's log
     output/                the simulation's output, one file per stream and time
     cstar-step.yaml        what this run did and what it left behind

Two options change where the stages stop. ``--pre-run`` performs stages 1
to 5 and exits, so start-up problems surface on a login node instead of
after a wait in the queue; a later run attaches to the prepared directory
and launches the model. ``--resume`` adopts the working directory of an
interrupted run, stages included, and continues from its last usable
restart. Both are described in :doc:`../../runs/pre_run_resume`.

Running it
----------

A UCLA-ROMS blueprint runs like any other:

.. code-block:: console

   cstar blueprint run my_simulation.yaml

On a cluster this can be started from a login node: the code is fetched and
compiled there and the model itself goes to the scheduler. Joining the
output afterwards is the one stage that is heavy on the login node, so set
``CSTAR_NPROCS_POST`` to a small number there, or run the simulation as a
step of a :doc:`workplan <../../workplans/index>` so that every stage runs on
a compute node. In a workplan a UCLA-ROMS step can also take its initial
conditions, boundary forcing or carbonate sensitivities from an earlier step
through :doc:`directives <../../workplans/directives>`.

Most UCLA-ROMS blueprints are written by :doc:`Forge <../forge/index>` at the
end of domain generation. :doc:`blueprint` describes what is in one, and
:doc:`example` walks through a ready-made toy domain you can run on a laptop.
