Blueprints
==========

Example
-------

.. code:: yaml

    name: 2node_1wk_example
    description: this is mainly to test infra like containers and workplans. it should run on 256 processors (2 nodes)
    application: roms_marbl
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

    model_params:
      time_step: 900

    runtime_params:
      start_date: "2000-01-15 00:00:00"
      end_date: "2000-01-22 00:00:00"
      output_dir: /anvil/scratch/x-seilerman/2node_1wk_job1/

Schema details
--------------

TODO

Checking validity
-----------------


CLI
^^^

.. code::

    cstar blueprint check my_blueprint.yaml

In Python
^^^^^^^^^

.. code:: python

    from pathlib import Path
    from cstar.cli.blueprint.check import check as check_blueprint

    check_blueprint(Path("/path/to/my/blueprint.yaml"))


Execution
---------


.. attention::
    You may need want to examine the `configuration options <configuration.rst>`_ available via environment variables before running.

.. warning::
    If you run a ROMS-MARBL blueprint directly on a HPC login node, you should *strongly* consider setting ``CSTAR_NPROCS_POST`` to a small number (~2), otherwise the joining post-process will try to use too many login node cores and will get terminated (and make the admins angry).

    Consider making single-step workplans to run single simulations entirely on the compute cluster.


CLI
^^^

.. code::

    cstar blueprint run my_blueprint.yaml



In Python
^^^^^^^^^


.. code:: python

    from pathlib import Path
    from cstar.cli.blueprint.run import run as run_blueprint

    run_blueprint(Path("/path/to/my/blueprint.yaml"))
