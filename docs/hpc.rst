.. _hpc:

Working on HPC systems
======================

C-Star runs on a laptop with no special setup. On a cluster a few things
differ: the scheduler must be configured, data goes on the right file
system, and Python environments need protecting from the module system.
:doc:`machines` lists the systems C-Star has been tested on.

Configure the scheduler
-----------------------

Most settings are environment variables; :doc:`configuration` lists them
all and ``cstar env show`` prints the values in effect. Only the SLURM ones
are required, and a workplan run stops early if the account and queue are
missing. A shell profile for a cluster typically carries:

.. code-block:: bash

   export CSTAR_SLURM_ACCOUNT="my-allocation"     # the account or allocation jobs are charged to
   export CSTAR_SLURM_QUEUE="mpi"                 # the partition or queue to submit to
   export CSTAR_SLURM_MAX_WALLTIME="01:00:00"     # default time requested per job

   # Only if your system does not already define them (see "Where data goes"):
   # export PROJECT=/path/to/your/project/directory
   # export SCRATCH=/path/to/your/scratch/directory

These are the defaults for every step of a workplan; a step can override
them, for example to send Forge steps to a general-purpose queue and ROMS
steps to an MPI queue. See the compute overrides on the :doc:`workplans`
page.

Where a blueprint runs
----------------------

``cstar blueprint run`` runs where you invoke it. The ROMS-MARBL application
submits the model run to SLURM itself, so it can be started from a login
node; Forge does not, and generates every input file wherever the command
runs. For anything beyond a toy domain, run Forge from a compute node or
through a workplan, even a :ref:`single-step one <workplan_examples>`. If
you must run a ROMS-MARBL blueprint directly on a login node, set
``CSTAR_NPROCS_POST`` to a small number (about 2): the post-processing step
that joins partitioned output otherwise uses every core it can find.

Where data goes
---------------

Every C-Star application writes under its blueprint's ``working_dir``, and a
workplan assigns each step a directory under ``CSTAR_DATA_HOME/<run id>``. On
a supported HPC system ``CSTAR_DATA_HOME`` resolves to your scratch file
system unless you set it yourself: the first of ``$SCRATCH``,
``$SCRATCH_DIR`` and ``$LOCAL_SCRATCH`` that is set (the ``CSTAR_SCRATCH_DIRS``
list, see :doc:`configuration`), or, on Bouchet, which exports none of
them, the ``scratch_pi_*/<user>`` directory linked from your home.

Forge follows the same rule. A forge blueprint's **working directory** holds
the generated inputs and rendered files for one domain and is used exactly as
written. Inside a workplan it is the step's directory. Saved from the wizard,
it is ``<data home>/_forge_bp_runs/<name>`` when the machine's data home is on
scratch and ``~/cstar/_forge_bp_runs/<name>`` otherwise, so a blueprint
written on a laptop and copied to a cluster keeps pointing at home; Forge
warns when that happens, and you edit ``working_dir`` to move it. The
**source-data cache** holds the downloaded and user-staged datasets (GLORYS,
TPXO, and so on), is shared by every domain you generate and must survive
scratch purges, so it follows ``$PROJECT`` instead:

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - System
     - Source-data cache
   * - Laptop or workstation
     - ``~/cstar-forge-data/source-data``
   * - Anvil
     - ``$PROJECT/cstar-forge-data/source-data``
   * - Perlmutter
     - ``$PROJECT/cstar-forge-data/source-data`` if ``PROJECT`` is set,
       otherwise ``$SCRATCH/cstar-forge-data/source-data``
   * - Bouchet
     - ``$PROJECT/cstar-forge-data/source-data`` if ``PROJECT`` is set,
       otherwise ``<scratch_pi_*>/<user>/cstar-forge-data/source-data``

Some systems define ``PROJECT`` and ``SCRATCH`` for you (Anvil exports both,
Perlmutter exports ``SCRATCH``); check with ``echo $PROJECT $SCRATCH`` and set
them in your shell profile only if they are empty. ``PROJECT`` pointing at a
group directory shares one source-data cache with your collaborators;
``SCRATCH`` names the scratch root where the system does not export one, and
setting it is the simplest way to steer both workplan runs and forge output
onto scratch on a system C-Star does not know.

.. code-block:: console

   cstar forge show-paths          # the detected system and the resolved paths
   cstar forge show-paths --json

Keep the environment off ``~/.local``
--------------------------------------

On clusters, Python's user-site directory (``~/.local/lib/pythonX.Y``) is a
common cause of environments that break without explanation: a
module-provided Python of the same minor version shares it, and packages
installed there shadow the environment's. If you see
``ModuleNotFoundError`` for a package you know is installed, or pip installs
that vanish, set:

.. code-block:: console

   export PYTHONNOUSERSITE=1

in your shell profile, or in the environment's ``activate.d`` so it applies
whenever the environment is active.

Jupyter on the cluster
----------------------

If your cluster offers a hosted Jupyter (an OnDemand portal, for example),
register the environment as a kernel so notebooks, including the wizard
notebook, run inside it:

.. code-block:: console

   cstar env register-kernel

The kernel starts through a wrapper that activates the environment first,
so shell commands in notebooks see the environment's ``PATH`` and variables.

The wizard from a login node
----------------------------

Serve the wizard on the login node and forward its port from your laptop, or
build blueprints on your laptop and copy them over; see :doc:`wizard`. Apart
from ``working_dir``, which the wizard fills in for the machine it runs on,
forge blueprints do not depend on the machine they were written on.
