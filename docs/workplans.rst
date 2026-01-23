Workplans
=========

Example
-------

.. code:: yaml

    name: simple_work_plan
    description: can we do the thing
    state: draft

    steps:
      - name: job1
        application: roms_marbl
        blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_a.yaml
      - name: job2
        application: roms_marbl
        blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_b.yaml
        depends_on:
          - job1
      - name: job3
        application: roms_marbl
        blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_c.yaml

Schema details
--------------

TODO

Checking validity
-----------------


CLI
^^^

.. code::

    cstar workplan check my_workplan.yaml

In Python
^^^^^^^^^

.. code:: python

    from pathlib import Path
    from cstar.cli.workplan.check import check as check_workplan

    check_workplan(Path("/path/to/my/workplan.yaml"))


Execution
---------


.. attention::
    Review the `configuration options <configuration.rst>`_ available via environment variables before running.

    If you are running on a HPC (recommended), you will need to set the account and queue for SLURM to use, or you will get an error.


CLI
^^^

.. code::

    cstar workplan run my_workplan.yaml --run-id my-unique-id

You can run the same command later, with the same :term:`run ID` to check the status of your running jobs. Or, you can use a different run ID to re-run the workplan from scratch.

In Python
^^^^^^^^^


.. code:: python

    from pathlib import Path
    from cstar.cli.workplan.run import run as run_workplan

    run_workplan(Path("/path/to/my/workplan.yaml"), run_id = "my-unique-id")
