Registering a New System
========================

C-Star runs on a fixed set of known systems (see :doc:`machines`). To add
support for a new HPC system, register a *system context*: a small set of
files and a Python class that tell C-Star how to identify the system, which
modules to load, and how to manage jobs on it. 

The steps below describe the manual process; replace ``<system-name>`` with
the short lowercase name of the new system (e.g. ``anvil``, ``bouchet``,
``perlmutter``).

Prerequisites
-------------

* You must have an SSH connection configured for the system.

Overview
--------

Registration involves three artifacts:

#. ``cstar/additional_files/lmod_lists/<system-name>.lmod`` - lists ``lmod`` 
   modules C-Star loads on the system.
#. ``cstar/additional_files/env_files/<system-name>.env`` — environment
   variables C-Star sets on the system.
#. A ``SystemContext`` subclass in ``cstar/system/manager.py`` — identifies
   the system at runtime and describes its scheduler.

Step 1: SSH access
----------------------------

#. Add the system to your ``~/.ssh/config`` if not yet completed.
#. Connect to the target system.

Step 2: Select lmod modules
----------------------------

C-Star supports loading system-level dependencies via `LMOD <https://lmod.readthedocs.io/en/latest/>`__.

Required modules
~~~~~~~~~~~~~~~~

#. netCDF-Fortan
#. netCDF
#. parallel netCDF (optional, recommended to enable parallel IO support)
#. MPI (e.g. OpenMPI, MPICH)
#. HDF5
#. GNU gcc
#. cmake

Module Identification
~~~~~~~~~~~~~~~~~~~~~

Now, we identify the modules to be used by C-Star.

#. List the modules loaded by default (i.e. entries marked ``(d,L)`` in the
   output of ``module avail``).
#. Identify any required modules that are loaded by default. We will
   explicitly track default modules for stability.

   * Inspect dependencies pulled in by any identified modules with ``module spider <module-name>``.
   * Ensure the chosen module dependencies do not conflict or cause unloads.

#. Create ``cstar/additional_files/lmod_lists/<system-name>.lmod``
#. Add a name for each required module to the ``.lmod`` file

   * Add one module name per line.
   * Use comments where useful (e.g. identifying system defaults, hidden modules,
     or implicitly loaded modules).
   * We recommend adding default modules explicitly to ensure stability when
     default modules change.

Example (``perlmutter.lmod``)::

   gcc/11.2.0  # system default
   openmpi/4.0.6  # system default
   parallel-netcdf/1.11.2
   cmake/3.20.0
   ...

Step 3: Identify system-specific environment variables
------------------------------------------------------

#. Run ``module load <module-name>`` for every entry in the new
   ``.lmod`` file.
#. Capture the environment with ``env`` and locate the variables set by
   each loaded module (e.g. ``env | grep MPI``)
#. For each module, identify the variable pointing at its root
   installation directory. Confirm the MPI and netCDF root directories are
   correct by checking for ``lib`` and ``bin`` subdirectories.
#. Find a variable that uniquely identifies the system at runtime, e.g.
   ``HOSTNAME=<system-name>`` or ``SLURM_CLUSTER_NAME=<system-name>``. This
   is used by ``is_match`` in Step 5.

Step 4: Create the ``.env`` file
--------------------------------

Copy an existing file from ``cstar/additional_files/env_files/`` to
``<system-name>.env`` and edit it:

#. Set ``MPIHOME``, ``NETCDFHOME`` (and ``PNETCDFHOME`` if applicable) from
   the root-directory variables found in Step 3.
#. Extend ``PATH`` with the netCDF ``bin`` directory and ``LIBRARY_PATH``
   with the netCDF ``lib`` directory.
#. Set ``GIT_DISCOVERY_ACROSS_FILESYSTEM="1"`` so git works on distributed
   filesystems such as Lustre.
#. Remove any variables copied from the template that do not apply.

Example (``perlmutter.env``)::

    MPIHOME=${CRAY_MPICH_PREFIX}
    NETCDFHOME=${CRAY_NETCDF_PREFIX}
    PNETCDFHOME=${CRAY_PARALLEL_NETCDF_PREFIX}
    PATH=${PATH}:${NETCDFHOME}/bin
    LIBRARY_PATH=${LIBRARY_PATH}:${NETCDFHOME}/lib
    GIT_DISCOVERY_ACROSS_FILESYSTEM="1"

Step 5: Add a ``SystemContext`` subclass
----------------------------------------

#. In ``cstar/system/manager.py``, add ``class <system-name>SystemContext(SystemContext)``,
   placed in alphabetical order among the existing subclasses and decorated
   with ``@register_sys_context``, e.g.::

      @register_sys_context
      class ElcapitanSystemContext(SystemContext):
          ...

#. Set the ``name``, ``compiler``, ``mpi_prefix``, and ``docs`` class
   variables (see ``AnvilSystemContext`` for reference).
#. Locate the system's web-based SLURM documentation and implement
   ``create_scheduler``, returning a ``SlurmScheduler`` with one
   ``SlurmPartition`` per documented partition.
#. Implement ``is_match`` using the system-identifying environment variable
   from Step 3, e.g.::

    @override
    @classmethod
    def is_match(cls) -> bool:
        return os.getenv("HOSTNAME", "") == "<system-name>"

Finally, add the new system to the :doc:`machines` table, keeping the
rows in alphabetical order by system name.
