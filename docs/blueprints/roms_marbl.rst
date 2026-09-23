.. _blueprints-roms-marbl:

ROMS-MARBL blueprint
=====================

Attributes
----------

:class:`cstar.applications.roms_marbl.models.RomsMarblBlueprint` contains all information
necessary to execute a coupled simulation using **UCLA-ROMS** with biogeochemistry
handled by **MARBL**. It adds the following attributes:

.. rubric:: RomsMarblBlueprint Attributes

.. autosummary::

  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.valid_start_date
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.valid_end_date
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.code
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.initial_conditions
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.grid
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.forcing
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.partitioning
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.runtime_params
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.cdr_forcing
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.nesting_info
  ~cstar.applications.roms_marbl.models.RomsMarblBlueprint.namelist_overrides

Explore the API reference of :class:`cstar.applications.roms_marbl.models.RomsMarblBlueprint`
for more detail on each item.


Example
-------

This example YAML demonstrates a configured ``RomsMarblBlueprint``. Notice that:

- ROMS code can be built from a fork, branch, or even a git commit hash, by specifying :attr:`branch:` or :attr:`commit:`
- Remote or local resources can be used to build and execute a simulation, under :attr:`compile_time:`
- C-Star handles both partitioned and unpartitioned data
- Runtime and compile-time behaviors can be customized in the ``.opt`` and ``.in`` files
- ``use_pio`` (whether to use the ParallelIO library for model input/output) is set under ``partitioning:``
- ``namelist_overrides`` maps a ROMS namelist group to key/value overrides. These are applied last, over C-Star's own derived runtime namelist settings, so user-supplied values win. For example, the model time step is set via ``namelist_overrides.time_stepping.dt`` (or directly in the namelist file itself).

.. code-block:: yaml

    name: 2node_1wk_example
    description: this is mainly to test infra like containers and workplans. it should run on 256 processors (2 nodes)
    application: roms_marbl
    schema_version: 3.0.0
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
