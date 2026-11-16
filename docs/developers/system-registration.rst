Registering a New System
========================

C-Star recognises a fixed set of systems (see :doc:`../deployments/machines`);
on any other cluster it runs as on a workstation (see :ref:`machines_unlisted`).
To add job submission and the site's modules for a new HPC system, add a
*system file*: one YAML file that tells C-Star how to identify the system,
which modules to load, which environment variables to set, and how to manage
jobs on it.

The steps below describe the manual process; replace ``<system-name>`` with
the short lowercase name of the new system (e.g. ``anvil``, ``bouchet``,
``perlmutter``).

Prerequisites
-------------

* You must have an SSH connection configured for the system.

Overview
--------

Registration adds one file, ``cstar/catalog/bundled/systems/<system-name>.yaml``.
Start from the file of the nearest existing system (``perlmutter.yaml`` for a
Cray system, ``anvil.yaml`` for a SLURM cluster with partitions). The
:download:`schema <../schemas/system/system_schema.1.0.0.json>` lists every key;
a file with a key it does not know is refused.

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

Next, identify the modules C-Star should load.

#. List the modules loaded by default (i.e. entries marked ``(d,L)`` in the
   output of ``module avail``).
#. Identify any required modules that are loaded by default, and list them
   explicitly anyway, so a change of site defaults doesn't change the build.

   * Inspect dependencies pulled in by any identified modules with ``module spider <module-name>``.
   * Ensure the chosen module dependencies do not conflict or cause unloads.
   * Filter the module list with ``module avail "<keyword>"`` - this will provide
     additional information to select related modules. Here, for example, most
     required modules come from the "built with GCC 11.2.0" group::

      ❯ module avail "netCDF" "HDF" "MPI"

      --------------------------------- Applications built with OpenMPI 4.0.6 ----------------------------------
         hdf5/1.10.7    (L,D)    netcdf-cxx4/4.3.1    (D)      parallel-netcdf/1.11.2
         netcdf-c/4.7.4 (L,D)    netcdf-fortran/4.5.3 (L,D)    py-mpi4py/3.0.3
      ----------------------------------- Applications built with GCC 11.2.0 -----------------------------------
         hdf/4.2.15  (L,D)    netcdf-c/4.7.4       netcdf-fortran/4.5.3    openmpi/4.0.6 (L,D)
         hdf5/1.10.7          netcdf-cxx4/4.3.1    openmpi/3.1.6           openmpi/4.1.6
      ------------------------------------------- Core Applications --------------------------------------------
         hdf/4.2.15    hdf5/1.10.7    netcdf-c/4.7.4    netcdf-cxx4/4.3.1    netcdf-fortran/4.5.3


#. List each required module under ``toolchains.cpu.modules`` in the system
   file, in the order they load.

   * Each entry is the argument of one ``module load``.
   * Use comments where useful (e.g. identifying system defaults, hidden modules,
     or implicitly loaded modules).
   * List default modules explicitly too, so the build doesn't change when
     the site's defaults do.

Example (``perlmutter.yaml``):

.. code-block:: yaml

   toolchains:
     cpu:
       compiler: gnu
       mpi_prefix: srun --kill-on-bad-exit=1
       modules:
         - cpu/1.0  # swaps out the login default gpu/1.0
         - PrgEnv-gnu/8.7.0  # login default, pinned so a change of site defaults does not change the build
         - gcc-native/14  # login default
         - cray-mpich/9.1.0  # login default
         - cray-hdf5/1.14.3.9
         - cray-netcdf/4.9.2.3  # provides netCDF-C and netCDF-Fortran (nf-config) in one prefix
         - cray-parallel-netcdf/1.12.3.21

Step 3: Identify system-specific environment variables
------------------------------------------------------

#. Run ``module load <module-name>`` for every entry in the module list.
#. Capture the environment with ``env`` and locate the variables set by
   each loaded module (e.g. ``env | grep MPI``)
#. For each module, identify the variable pointing at its root
   installation directory. Confirm the MPI and netCDF root directories are
   correct by checking for ``lib`` and ``bin`` subdirectories.
#. Find a variable that uniquely identifies the system at runtime, e.g.
   ``HOSTNAME=<system-name>`` or ``SLURM_CLUSTER_NAME=<system-name>``. This
   goes in the ``detect`` block in Step 5. A system whose name Lmod reports
   (``LMOD_SYSHOST`` or ``LMOD_SYSTEM_NAME``) needs none.

Step 4: Set the environment variables
-------------------------------------

Under ``toolchains.cpu.env``, in order (a ``${VAR}`` sees the entries above it
first, then what the modules set):

#. Set ``MPIHOME``, ``NETCDFHOME`` (and ``PNETCDFHOME`` if applicable) from
   the root-directory variables found in Step 3.
#. Extend ``PATH`` with the netCDF ``bin`` directory and ``LIBRARY_PATH``
   with the netCDF ``lib`` directory.
#. Set ``GIT_DISCOVERY_ACROSS_FILESYSTEM: "1"`` so git works on distributed
   filesystems such as Lustre.
#. Remove any variables copied from the other system that do not apply.

Example (``perlmutter.yaml``):

.. code-block:: yaml

       env:
         MPIHOME: ${CRAY_MPICH_PREFIX}/
         NETCDFHOME: ${CRAY_NETCDF_PREFIX}/
         PNETCDFHOME: ${CRAY_PARALLEL_NETCDF_PREFIX}/
         PATH: ${PATH}:${NETCDFHOME}bin
         LIBRARY_PATH: ${LIBRARY_PATH}:${NETCDFHOME}lib
         GIT_DISCOVERY_ACROSS_FILESYSTEM: "1"

Step 5: Describe the scheduler and detection
--------------------------------------------

#. Set ``name`` (the file's name without ``.yaml``) and ``docs``.
#. Locate the system's web-based SLURM documentation and add a ``scheduler``
   block with ``family: slurm``, one ``queues`` entry per documented
   partition or QOS, the ``primary_queue``, and ``require`` (whether a job
   must name an account or a queue). Quote a fixed walltime (``"12:00:00"``).
#. Add a ``detect`` rule using the system-identifying environment variable
   from Step 3, e.g.:

   .. code-block:: yaml

      detect:
        - {env: HOSTNAME, equals: <system-name>}

Once the file exists, ``cstar env set CSTAR_SYSTEM <system-name>`` makes C-Star
use it even before ``detect`` recognises the system, so you can test it on the
cluster. ``test_bundled_systems_golden.py`` pins what each bundled system gives
C-Star; regenerate its fixture with ``UPDATE_GOLDEN=1`` and review the diff.

Finally, add the new system to the :doc:`../deployments/machines` table, keeping the
rows in alphabetical order by system name.
