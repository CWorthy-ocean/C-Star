Blueprints
==========

Blueprints define the contract for communicating the available behaviors of an
application. A user-configured blueprint informs C-Star which behaviors are
desired.

.. _blueprint_schema:

Core Blueprint Schema
---------------------

The core attributes of a blueprints come from :class:`cstar.orchestration.models.Blueprint`.

.. rubric:: Core Blueprint Attributes

.. autosummary::

  ~cstar.orchestration.models.Blueprint.name
  ~cstar.orchestration.models.Blueprint.description
  ~cstar.orchestration.models.Blueprint.application
  ~cstar.orchestration.models.Blueprint.state
  ~cstar.orchestration.models.Blueprint.cpus_needed

The core blueprint attributes do not contain enough information to be executed.

Customizing Blueprints
----------------------

`Blueprint` subclasses are created to define attributes for each supported
application. These classes are responsible for exposing the set of configurable
parameters that are user-facing.


Custom Blueprint Example: `RomsMarblBlueprint`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`cstar.orchestration.models.RomsMarblBlueprint` contains all information
necessary to execute a coupled simulation using `UCLA-ROMS` with biogeochemistry
handled by `MARBL`. It adds the following attributes:

.. rubric:: RomsMarblBlueprint Attributes

.. autosummary::

  ~cstar.orchestration.models.RomsMarblBlueprint.valid_start_date
  ~cstar.orchestration.models.RomsMarblBlueprint.valid_end_date
  ~cstar.orchestration.models.RomsMarblBlueprint.code
  ~cstar.orchestration.models.RomsMarblBlueprint.initial_conditions
  ~cstar.orchestration.models.RomsMarblBlueprint.grid
  ~cstar.orchestration.models.RomsMarblBlueprint.forcing
  ~cstar.orchestration.models.RomsMarblBlueprint.partitioning
  ~cstar.orchestration.models.RomsMarblBlueprint.model_params
  ~cstar.orchestration.models.RomsMarblBlueprint.runtime_params
  ~cstar.orchestration.models.RomsMarblBlueprint.cdr_forcing

Explore the API reference of :class:`cstar.orchestration.models.RomsMarblBlueprint` 
for more detail on each item.


Preparing a Blueprint
---------------------

A blueprint can be prepared for execution in two ways:

1. Create a `YAML` file with the desired blueprint configuration.
2. Write `python` code to define a `RomsMarblBlueprint` instance.

We recommend our users to create the `YAML` file directly and execute it 
using the `cstar cli`.


RomsMarblBlueprint Example
--------------------------

This example demonstrates a configured `RomsMarblBlueprint`. Notice that:

- `ROMS` code can be built from a fork, branch, or even a git commit hash.
- Remote or local resources can be used to build and execute a simulation.
- C-Star handles both partioned and unpartitioned data.
- Runtime and compile-time behaviors can be customized

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


Checking validity
-----------------

Blueprints can be checked for errors using the CLI and in code.

.. tab-set::

   .. tab-item:: Validating via CLI

    Use the `check` command from the `cstar CLI`.

    .. code-block:: console

        cstar blueprint check my_blueprint.yaml

   .. tab-item:: Programmatic Validation

    Use the `deserialize` method to validate a `YAML` file in Python.

    .. code-block:: python

        from cstar.orchestration.models import RomsMarblBlueprint
        from cstar.orchestration.serialization import deserialize

        deserialize("my_blueprint.yaml", RomsMarblBlueprint)


Execution
---------

.. include:: snippets/review-config.rst

.. warning::
    The post-processing step joining partitioned data may consume all available cores of a login node and be terminated (and make the admins angry).

    - We *strongly* recommend setting ``CSTAR_NPROCS_POST`` to a small number (~2) when running a `ROMS-MARBL` blueprint directly on a HPC login node.
    - Consider making a :ref:`single-step workplan <single-step-wp-ex>` to run a simulation entirely on the compute cluster.

CLI
^^^

Use the `run` command from the `cstar CLI` to execute a blueprint.

.. code-block:: console

    cstar blueprint run my_blueprint.yaml


.. tab-set::

   .. tab-item:: Run via CLI

    Use the `run` command from the `cstar CLI`.

    .. code-block:: console

        cstar blueprint run my_blueprint.yaml

   .. tab-item:: Programmatic Execution

    Use a `SimulationRunner` to execute the blueprint.

    .. code-block:: python
      :caption: Executing a blueprint `YAML` file in python.

        from cstar.entrypoint.service import ServiceConfiguration
        from cstar.entrypoint.worker.worker import BlueprintRequest, JobConfig, SimulationRunner

        account_id = "your-account-id"
        queue_id = "wholenode"
        
        request = BlueprintRequest("my_blueprint.yaml")
        service_cfg = ServiceConfiguration()
        job_cfg = JobConfig(account_id, priority=queue_id)

        SimulationRunner(request, service_cfg, job_cfg)
