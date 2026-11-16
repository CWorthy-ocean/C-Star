.. _describe-cluster:

Describe your cluster
=====================

On a cluster C-Star does not list (see :ref:`machines_unlisted`), a *system
file* tells C-Star how to load the site's modules and submit jobs. It is one
YAML file that you write once, test on the login node, and keep in your own
catalog or share with your group.

Start from the nearest machine
------------------------------

List the systems C-Star knows, then print the file of the one most like your
cluster:

.. code-block:: console

   $ cstar system list
   anvil          bundled  <cstar>/catalog/bundled/systems/anvil.yaml
   bouchet        bundled  <cstar>/catalog/bundled/systems/bouchet.yaml
   ...
   perlmutter     bundled  <cstar>/catalog/bundled/systems/perlmutter.yaml

   $ cstar system show anvil > mycluster.yaml

Use ``anvil`` for a SLURM cluster whose queues are partitions, and
``perlmutter`` for a Cray system whose queues are QOS. ``cstar system show
derecho`` is the example for PBS.

Edit the file
-------------

Open ``mycluster.yaml`` and change what is specific to your site. The
:doc:`schema <../schemas/index>` lists every key; a file with a key it does
not know is refused. Change these, in this order.

**Name and docs.** ``name`` is a short lowercase name for the cluster, and
``docs`` links the site's user guide:

.. code-block:: yaml

   name: mycluster
   docs: https://hpc.example.edu/userguide

**Modules.** Under ``toolchains.cpu.modules``, list the modules the build
needs, in load order: a compiler, MPI, HDF5, netCDF-C and netCDF-Fortran, and
optionally parallel netCDF (which turns on parallel I/O). Each entry is the
argument of one ``module load``:

.. code-block:: yaml

   toolchains:
     cpu:
       compiler: gnu
       mpi_prefix: srun --kill-on-bad-exit=1
       modules:
         - gcc/11.2.0
         - openmpi/4.0.6
         - netcdf-fortran/4.5.3
         - parallel-netcdf/1.11.2

To find them, run ``module avail`` and note the entries marked ``(d)`` or
``(L)``, which the site loads by default. List the ones you need explicitly
even when they are defaults, so a change of site defaults does not change the
build. ``module avail netcdf`` narrows the list, and ``module spider
<name>`` shows what a module depends on. Pick a set that loads without
unloading anything.

**Environment variables.** After the modules load, ``env`` sets the variables
the ROMS build reads. Run ``module load`` for each entry, then ``module show
<name>`` or ``env | grep -i netcdf`` to find the variable each module sets
for its install prefix, and check that the netCDF and MPI prefixes each have
``lib/`` and ``bin/`` directories:

.. code-block:: yaml

       env:
         MPIHOME: ${OPENMPI_HOME}
         NETCDFHOME: ${NETCDF}
         PNETCDFHOME: ${PNETCDF}
         PATH: ${NETCDFHOME}/bin:${PATH}
         LIBRARY_PATH: ${LIBRARY_PATH}:${NETCDFHOME}/lib
         GIT_DISCOVERY_ACROSS_FILESYSTEM: "1"

Entries apply in order, and a ``${VAR}`` sees the entries above it first, then
whatever the modules set. Delete any variable copied from the example that
your site does not need. If netCDF-Fortran lives in a different prefix from
netCDF-C, set ``NETCDFFHOME`` as well, as ``anvil.yaml`` does.

**Scheduler.** ``scheduler`` names the family (``slurm`` or ``pbs``), the
queues jobs may use, and the one used by default. The site's user guide lists
the partitions or QOS. ``require`` says whether a job must name an account, a
queue, or both:

.. code-block:: yaml

   scheduler:
     family: slurm
     primary_queue: standard
     queues:
       - {name: standard, kind: partition}
       - {name: debug, kind: partition, max_walltime: "02:00:00"}
     require: [account, queue]

A queue's ``kind`` is ``partition`` or ``qos``. C-Star asks SLURM for each
queue's longest walltime unless you give one; quote a fixed walltime, as
above, because YAML reads an unquoted ``02:00:00`` as a number.

**Detection.** C-Star recognises the cluster by the name Lmod reports. If
yours does not report one, add a ``detect`` rule: an environment variable
that has the same value on login and compute nodes, or a directory that exists
only on this cluster:

.. code-block:: yaml

   detect:
     - {env: CLUSTER_NAME, equals: mycluster}
     # or: - {path: /mycluster/home}

Without a rule, ``cstar init`` sets ``CSTAR_SYSTEM`` for you (see below).

Check it on the login node
--------------------------

Copy the file to the cluster and run, on the login node:

.. code-block:: console

   $ cstar system check ./mycluster.yaml

It prints one line per step, each ``PASS``, ``FAIL`` or ``NOT CHECKED``: the
file's schema, whether each module loads, whether the prefix variables point
at real directories, whether the scheduler's commands work, and each queue's
walltime. Fix every ``FAIL`` and run it again. ``NOT CHECKED`` marks a step
that cannot run from here, and the last lines name what the check cannot test:
anything inside a job, a build, and a run. Try a small blueprint with
``cstar blueprint run`` once the check is clean.

Install it
----------

.. code-block:: console

   $ cstar init --system-file ./mycluster.yaml
   System: mycluster (from ./mycluster.yaml); scheduler: SLURM
   ...
   System file
     Copied the file to ~/cstar/catalog/systems/mycluster.yaml.
     C-Star does not detect 'mycluster' here: setting CSTAR_SYSTEM=mycluster.

``cstar init`` uses the file for the rest of the setup and copies it into your
catalog as ``<name>.yaml``, so the file's ``name`` must be the name you want
C-Star to use. It sets ``CSTAR_SYSTEM`` only when C-Star does not recognise
the machine by itself. ``cstar system add ./mycluster.yaml`` does only the
copy.

C-Star refuses a system file that every user can write; ``chmod o-w
mycluster.yaml`` fixes it.

Share it with your group
------------------------

Put the file in a catalog everyone on the cluster can read:

.. code-block:: console

   $ cstar system add ./mycluster.yaml --catalog /project/shared/cstar-catalog
   Added the system 'mycluster' as /project/shared/cstar-catalog/systems/mycluster.yaml
   ...

If that catalog is not in your own ``CSTAR_CATALOG``, the command also prints
the ``cstar env set CSTAR_CATALOG ...`` line that adds it. A colleague points ``cstar init`` at it:

.. code-block:: console

   $ cstar init --system-file /project/shared/cstar-catalog/systems/mycluster.yaml

``cstar init`` offers to add ``/project/shared/cstar-catalog`` to their
``CSTAR_CATALOG`` as a shared catalog, so they follow your later fixes.
If their ``CSTAR_CATALOG`` already lists it, plain ``cstar init`` finds the
file. A catalog's ``systems/`` directory is layered like the rest of the
catalog (see :ref:`catalog-systems`).

Contribute it
-------------

Once the file works, open a pull request that adds it to
``cstar/catalog/bundled/systems/`` (see :doc:`../developers/system-registration`).
After the next release, nobody on that machine needs any of the steps above.
