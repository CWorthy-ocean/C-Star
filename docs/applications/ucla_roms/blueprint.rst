.. _blueprints-ucla-roms:

What's in the blueprint
=======================

A UCLA-ROMS blueprint describes one simulation: the model code to build, the
input files to read, how to split the domain across processors, and the run
window. Forge writes one at the end of domain generation; you can also write
one by hand for input files you already have, for example files made with
`ROMS-Tools <https://roms-tools.readthedocs.io>`__ directly.

.. note::

   Blueprints written as ``application: roms_marbl``, the application's earlier
   name, still load and run unchanged.

Fields
------

Besides the :ref:`core fields <blueprints>` every blueprint has, a UCLA-ROMS
blueprint has these:

``valid_start_date``, ``valid_end_date``
   The period the blueprint's inputs cover. A run window outside it is
   refused, so a blueprint cannot be run past the end of its forcing by
   mistake.
``code``
   The model code and the files it is built and run with. ``roms``, and
   optionally ``marbl`` and ``pio``, each name a git repository by
   ``location`` and pin it with a ``branch`` or ``commit``; a fork or a local
   clone works too. ``compile_time`` and ``run_time`` point at the
   compile-time files (``cppdefs.opt``, ``param.opt`` and the other ``.opt``
   files, and the ``Makefile``) and the run-time files (the namelist and the
   MARBL settings): a repository, a directory, or a URL, with a ``filter``
   listing the files to take.
``grid``, ``initial_conditions``
   The grid file and the initial conditions file.
``forcing``
   The forcing files, one list per kind: ``surface``, ``boundary``,
   ``tidal``, ``river`` and, for a CDR-LiTE build that does not compute them
   online, ``carbonate_sensitivity`` (the ``ddic_dco2`` and ``ddic_dalk``
   files that an earlier MARBL run wrote as its ``_cdrgas`` output).
``cdr_forcing``
   An optional carbon dioxide removal forcing file.
``nesting_info``
   Optional: how this grid relates to a parent or child grid.
``partitioning``
   The processor layout, ``n_procs_x`` by ``n_procs_y``, and whether to build
   with ParallelIO (``use_pio``). The number of MPI ranks is the product.
``runtime_params``
   The run window: ``start_date`` and ``end_date``. Other run-time settings
   come from the namelist, so this section stays small.
``namelist_overrides``
   Values to set in the namelist, grouped as the namelist groups them. They
   are applied last, over everything C-Star derives, so a value here always
   wins. The time step, for example, is ``time_stepping.dt``.

Each dataset entry under ``grid``, ``initial_conditions``, ``forcing`` and
``cdr_forcing`` is a ``location`` with an optional ``partitioned`` flag. A
file can be anywhere: a local path, a URL, or a path inside a repository.
A relative ``location`` resolves against the directory of the blueprint
file, not the directory you run from, so a blueprint can sit next to its
data and be moved with it.

:class:`cstar.applications.ucla_roms.models.UclaRomsBlueprint` is the model
behind the file, and the API reference describes each field in full.

.. rubric:: UclaRomsBlueprint fields

.. autosummary::

  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.valid_start_date
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.valid_end_date
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.code
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.initial_conditions
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.grid
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.forcing
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.partitioning
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.runtime_params
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.cdr_forcing
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.nesting_info
  ~cstar.applications.ucla_roms.models.UclaRomsBlueprint.namelist_overrides

Partitioned and unpartitioned inputs
------------------------------------

Without ParallelIO, ROMS reads one tile of each input file per MPI rank.
C-Star partitions the inputs for you: point each ``location`` at the whole
file and C-Star splits it according to ``partitioning`` before the run.

For large domains, partitioning can take a while, and you may already have
partitioned files. In that case point ``location`` at the first tile
(``<name>.000.nc``) and set ``partitioned: true``; C-Star finds the other
tiles beside it. The tiles must match ``n_procs_x`` and ``n_procs_y``, and
C-Star checks that they do.

With ParallelIO (``use_pio: true`` under ``partitioning``), ROMS reads and
writes whole files, so nothing is partitioned. Input files must then be in a
netCDF format ParallelIO can read (classic or CDF-5, not NETCDF4); Forge
writes them that way, and C-Star refuses a file that is not.

Example
-------

A blueprint for a two-node run on Anvil, with inputs that were partitioned
ahead of time. Notice that:

- the ROMS code is pinned to a branch; a ``commit`` or a fork would work the
  same way, and a tag such as ``0.12.0`` can be given as the ``commit``;
- the compile-time and run-time files come from local directories, under
  ``code.compile_time`` and ``code.run_time``, each with a ``filter`` naming
  the files to take;
- every dataset is partitioned already, with ``partitioned: true``, so
  ``n_procs_x`` and ``n_procs_y`` must match the tiles;
- the time step is set through ``namelist_overrides``.

.. code-block:: yaml

    name: 2node_1wk_example
    description: this is mainly to test infra like containers and workplans. it should run on 256 processors (2 nodes)
    application: ucla_roms
    schema_version: 3.3.0
    working_dir: /anvil/scratch/x-seilerman/2node_1wk_job1/
    state: draft
    valid_start_date: 2000-01-15 0:00:00
    valid_end_date: 2000-01-23 0:00:00
    code:
      roms:
        location: https://github.com/CWorthy-ocean/ucla-roms.git
        branch: main
      marbl:
        location: https://github.com/marbl-ecosys/MARBL.git
        branch: marbl0.45.0

      run_time:
        location: /anvil/scratch/x-seilerman/2node_test_domain
        branch: "na"
        filter:
          files:
          - test_domain_1wk.in
          - marbl_in
          - marbl_tracer_output_list
          - marbl_diagnostic_output_list
      compile_time:
        location: /anvil/scratch/x-seilerman/2node_test_domain/compile
        branch: "na"
        filter:
          files:
          - bgc.opt
          - bulk_frc.opt
          - cppdefs.opt
          - diagnostics.opt
          - ocean_vars.opt
          - param.opt
          - tracers.opt
          - Makefile

    grid:
      data:
        - location: /anvil/scratch/x-seilerman/2node_test_domain/input_files/partitioned_files/grid_64x64x5.000.nc
          partitioned: true
    initial_conditions:
      data:
        - location: /anvil/scratch/x-seilerman/2node_test_domain/input_files/partitioned_files/init_condis_bgc.000.nc
          partitioned: true

    forcing:
      tidal:
        data:
          - location: /anvil/scratch/x-seilerman/2node_test_domain/input_files/partitioned_files/tides_Jan1_2000.000.nc
            partitioned: true
      surface:
        data:
          - location: /anvil/scratch/x-seilerman/2node_test_domain/input_files/partitioned_files/surf_phys_filepath_200001.000.nc
            partitioned: true
          - location: /anvil/scratch/x-seilerman/2node_test_domain/input_files/partitioned_files/surf_frc_bgc_clim.000.nc
            partitioned: true
      boundary:
        data:
          - location: /anvil/scratch/x-seilerman/2node_test_domain/input_files/partitioned_files/boundary_force_phys_jan15_feb21_200001.000.nc
            partitioned: true
          - location: /anvil/scratch/x-seilerman/2node_test_domain/input_files/partitioned_files/boundary_force_bgc_jan15_feb21_clim.000.nc
            partitioned: true

    partitioning:
      n_procs_x: 16
      n_procs_y: 16

    namelist_overrides:
      time_stepping:
        dt: 900

    runtime_params:
      start_date: "2000-01-15 00:00:00"
      end_date: "2000-01-22 00:00:00"

The :doc:`example page <example>` walks through a smaller blueprint, a toy
domain whose inputs are fetched from a public repository, that runs on a
laptop in a few minutes.
