Registering a New System
========================

C-Star runs on a fixed set of known systems (see :doc:`machines`). To add
support for a new HPC system, register a *system context*: a small set of
files and a Python class that tell C-Star how to identify the system, which
modules to load, and how to submit jobs on it. The steps below describe the
manual process; replace ``<system-name>`` with the short lowercase name of
the new system (e.g. ``anvil``, ``bouchet``, ``perlmutter``).

Overview
--------

Registration involves four artifacts:

#. An SSH connection to the system, configured in ``~/.ssh/config``, used to
   investigate the system's software stack.
#. ``cstar/additional_files/lmod_lists/<system-name>.lmod`` — the lmod
   modules C-Star loads on the system.
#. ``cstar/additional_files/env_files/<system-name>.env`` — environment
   variables C-Star sets on the system.
#. A ``SystemContext`` subclass in ``cstar/system/manager.py`` — identifies
   the system at runtime and describes its scheduler.

Step 1: Configure SSH access
----------------------------

Add an entry for the system to ``~/.ssh/config`` if one does not already
exist, for example::

    Host <system-name>
        HostName <login-node-address>
        User <username>

All remaining steps require an SSH session on the system.

Step 2: Select lmod packages
----------------------------

Connect to the system and identify the modules C-Star needs:

#. List the modules loaded by default: entries marked ``(d,L)`` in the
   output of ``module avail``.
#. List the available netCDF packages with ``module avail "netCDF"``.
#. Choose a netCDF Fortran package built with the GNU compiler.
#. Choose a parallel netCDF package built with the same compiler version.
#. Inspect dependencies pulled in by the netCDF packages with
   ``module spider <package-name>``.
#. If a GCC compiler is not already loaded by default or as a dependency,
   find a compatible one with ``module avail``. Do the same for Open MPI
   and CMake.

Create ``cstar/additional_files/lmod_lists/<system-name>.lmod`` listing each
required package, one per line. Include packages loaded by default explicitly
for completeness, marked with a short comment, for example::

    gcc/11.2.0  # (system default)
    openmpi/4.0.6  # (system default)
    parallel-netcdf/1.11.2
    cmake/3.20.0

Step 3: Identify system-specific environment variables
------------------------------------------------------

Still on the system:

#. Run ``module load <package-name>`` for every entry in the new
   ``.lmod`` file.
#. Capture the environment with ``env`` and locate the variables set by
   each loaded module.
#. For each package, identify the variable pointing at its root
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
#. Comment out any variables copied from the template that do not apply.

Example (``perlmutter.env``)::

    MPIHOME=${CRAY_MPICH_PREFIX}/
    NETCDFHOME=${CRAY_NETCDF_PREFIX}/
    PNETCDFHOME=${CRAY_PARALLEL_NETCDF_PREFIX}/
    PATH=${PATH}:${NETCDFHOME}bin
    LIBRARY_PATH=${LIBRARY_PATH}:${NETCDFHOME}lib
    GIT_DISCOVERY_ACROSS_FILESYSTEM="1"

Step 5: Add a ``SystemContext`` subclass
----------------------------------------

In ``cstar/system/manager.py``, add
``class <SystemName>SystemContext(SystemContext)``, placed in alphabetical
order among the existing subclasses and decorated with
``@register_sys_context``:

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

Finally, add the new system to the table in :doc:`machines`, keeping the
rows in alphabetical order by system name.
