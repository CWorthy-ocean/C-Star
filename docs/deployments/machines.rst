
Support Machines
================

C-Star aims to be deployable on a wide range of HPC systems, but configuration
is needed on each system. This process is becoming more automated over time.

Here are the current systems where we have tested so far. Some systems have support
for building and running UCLA-ROMS via C-Star, but have not been tested or are not
yet supported for full workplan orchestration.

+--------------------------------------------------------------------------------------+------------+-------------------------+
| Name of system                                                                       | UCLA-ROMS  | Orchestration Support   |
|                                                                                      | build/run  |                         |
|                                                                                      | support    |                         |
+======================================================================================+============+=========================+
| `Anvil (RCAC) <https://www.rcac.purdue.edu/anvil>`__                                 | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Bouchet (YCRC) <https://docs.ycrc.yale.edu/clusters/bouchet/>`__                    | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Derecho (NCAR)                                                                      | ✅         | ❌ (PBS not             |
| <https://ncar-hpc-docs.readthedocs.io/en/latest/compute-systems/derecho/>`__         |            | currently supported)    |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Elja (IRHPC) <https://wiki.irei.is>`__                                              | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Expanse (SDSC) <https://www.sdsc.edu/support/user_guides/expanse.html>`__           | ✅         | Untested                |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Perlmutter (NERSC) <https://docs.nersc.gov/systems/perlmutter/architecture/>`__     | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| MacOS ARM64 (Apple Silicon)                                                          | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| Linux (x86_64 and aarch 64)                                                          | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+

On Derecho, which uses PBS, ``cstar blueprint submit`` and ``cstar workplan
submit`` are not available. Run ``cstar blueprint run`` from inside a PBS job
(an interactive ``qsub -I`` session or your own batch script).

.. _machines_unlisted:

On a cluster that isn't listed
------------------------------

C-Star treats a cluster that isn't named in the table like a Linux
workstation, and usually warns that it doesn't recognise the system. It builds
UCLA-ROMS with the compilers, MPI and netCDF from your conda environment, so
install ``cstar-ocean-standalone`` (see :doc:`../getting_started/installation`).
It submits no jobs: ``cstar blueprint submit`` and ``cstar workplan submit`` run
every step on the node where you start them.

So run your blueprints inside an allocation rather than on a login node (on a
PBS cluster, start one with ``qsub -I``):

.. code-block:: console

   salloc --nodes=1 --time=02:00:00     # plus your site's account and partition
   cstar blueprint run my_blueprint.yaml

If your cluster is in the table but C-Star didn't recognise it, for example on
a new login node, give its name in lowercase. To keep the workstation setup and
stop the warning, give the generic name the warning shows instead:

.. code-block:: console

   cstar env set CSTAR_SYSTEM perlmutter       # a system from the table
   cstar env set CSTAR_SYSTEM linux_x86_64     # the generic name on most clusters

``cstar env show`` prints the system in use.

To submit jobs from the login node and have C-Star load your site's modules,
the system needs to be added to C-Star (see
:doc:`../developers/system-registration`). Reach out to the repo maintainers and
we will help you set it up.
