.. _hpc:

Working on HPC systems
======================

C-Star runs on a laptop with no special setup. On a cluster a few things
differ: the scheduler must be configured, data goes on the right file
system, and Python environments need protecting from the module system.
:doc:`machines` lists the systems C-Star has been tested on.

Configure the scheduler
-----------------------

Before running a workplan, set your SLURM account and queue and review the
other settings with ``cstar env show``; see :doc:`configuration`. A workplan
run fails early if the account and queue are missing.

Do not run simulations on a login node. Submit them through a workplan, even
a :ref:`single-step one <workplan_examples>`, so the work goes to compute
nodes. If you must run a ROMS-MARBL blueprint directly on a login node, set
``CSTAR_NPROCS_POST`` to a small number (about 2): the post-processing step
that joins partitioned output otherwise uses every core it can find.

Where data goes
---------------

Workplan runs are laid out under ``CSTAR_DATA_HOME``. On a supported HPC
system that resolves to your scratch file system unless you set it
yourself.

Forge keeps two kinds of data. The **source-data cache** holds the
downloaded and user-staged datasets (GLORYS, TPXO, and so on) and is shared
by every domain you generate. A forge blueprint's **working directory**
holds the generated inputs and rendered files for one domain. Both are
placed per system:

.. list-table::
   :header-rows: 1
   :widths: 18 42 40

   * - System
     - Source-data cache
     - Default working directories
   * - Laptop or workstation
     - ``~/cstar-forge-data/source-data``
     - ``~/cstar/_forge_bp_runs/<name>``
   * - Anvil
     - ``$PROJECT/cstar-forge-data/source-data``
     - ``$SCRATCH/cstar/_forge_bp_runs/<name>``
   * - Perlmutter
     - ``$PROJECT/cstar-forge-data/source-data`` if ``PROJECT`` is set,
       otherwise ``$SCRATCH/cstar-forge-data/source-data``
     - ``$SCRATCH/cstar/_forge_bp_runs/<name>``
   * - Bouchet
     - ``$PROJECT/cstar-forge-data/source-data`` if ``PROJECT`` is set,
       otherwise ``<scratch_pi_*>/<user>/cstar-forge-data/source-data``
     - ``<scratch_pi_*>/<user>/cstar/_forge_bp_runs/<name>``

Set ``PROJECT`` to a group directory to share one source-data cache with
your collaborators; set ``SCRATCH`` to override the scratch root where the
system does not export one. A forge blueprint whose ``working_dir`` is the
default form (``~/cstar/_forge_bp_runs/...``) is rebased onto scratch on
these systems; a working directory you set explicitly is used as written.

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
build blueprints on your laptop and copy them over; see :doc:`wizard`. Forge
blueprints do not depend on the machine they were written on.
