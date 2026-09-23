Installation
============

C-Star is published on `conda-forge <https://anaconda.org/conda-forge/cstar-ocean>`__
as ``cstar-ocean``. One install gives you the ``cstar`` command line, the
ROMS-MARBL application and Forge, the domain-generation application, including
its wizard.

Install with conda
------------------

Pick the variant that matches your machine:

.. tab-set::

   .. tab-item:: Laptop or workstation

      ``cstar-ocean-standalone`` installs C-Star together with a complete
      build toolchain (compilers, MPI, netCDF-Fortran, PnetCDF, CMake, rsync)
      from conda-forge, so ROMS can be compiled without any system
      dependencies:

      .. code-block:: console

         conda create -n cstar-env -c conda-forge cstar-ocean-standalone

   .. tab-item:: HPC system

      On a supported HPC system the compiler, MPI and netCDF toolchain comes
      from the site's environment modules, so install just ``cstar-ocean``:

      .. code-block:: console

         conda create -n cstar-env -c conda-forge cstar-ocean

      Do not install ``cstar-ocean-standalone`` or the conda ``compilers``
      packages into an environment you will activate on a cluster. They export
      ``CC``, ``CXX`` and ``FC`` on activation and put their own ``mpif90``
      ahead of the module toolchain on ``PATH``, which silently hijacks ROMS
      builds that should use the cluster's compilers.

Then activate the environment:

.. code-block:: console

   conda activate cstar-env

.. _verify-install:

Verify the installation
-----------------------

.. code-block:: console

   cstar --version
   cstar --help

The first command prints the C-Star version and the versions of its companion
packages; the second lists subcommands you may wish to explore in more detail.

After installing
----------------

Register for dataset access
   Generating a new domain downloads forcing data from public archives. Most
   sources need no account, but GLORYS ocean reanalysis (used for essentially
   every domain) and TPXO tides require a free registration and a one-time
   setup step. See :doc:`data_access`.

Register a Jupyter kernel
   If you intend to use Jupyter notebooks with your C-Star environment, for either your own analysis or
   to use the Jupyter version of the Forge wizard, activate your ``cstar_env`` conda environment
   and run:

   .. code-block:: console

      cstar env register-kernel

   This custom registration mechanism helps ensure compatibility with HPC Jupyter servers.
   The kernel launches through a wrapper that activates the environment
   first, so shell commands inside notebooks see the same ``PATH`` and
   variables you do. Run ``cstar env register-kernel --help`` for naming
   options.

Configure C-Star for your cluster
   On an HPC system, set your scheduler account and queue and review the
   other environment variables before running anything. ``cstar env show``
   prints the active configuration; see :doc:`configuration` and :doc:`hpc`.

Install from source
-------------------

Use this path if you plan to modify C-Star itself or need an unreleased
version. Clone the repository:

.. code-block:: console

   git clone https://github.com/CWorthy-ocean/C-Star.git
   cd C-Star

Create the environment from the file that matches your machine. Both create
an environment named ``cstar-env``; the laptop file bundles the same
conda-forge toolchain that ``cstar-ocean-standalone`` provides.

.. tab-set::

   .. tab-item:: Laptop or workstation

      .. code-block:: console

         conda env create -f environment-laptop.yml
         conda activate cstar-env

   .. tab-item:: HPC system

      .. code-block:: console

         conda env create -f environment-hpc.yml
         conda activate cstar-env

Then install C-Star into it. An editable install picks up your edits to the
checkout without reinstalling; the ``dev`` and ``docs`` extras add the test
suite and documentation dependencies.

.. tab-set::

   .. tab-item:: Editable

      .. code-block:: console

         pip install -e .

   .. tab-item:: Contributor (tests and docs)

      .. code-block:: console

         pip install -e ".[dev,docs]"

   .. tab-item:: Fixed copy

      .. code-block:: console

         pip install .

See :doc:`contributing` for running the tests and building this
documentation.

Updating
--------

Released versions update with conda:

.. code-block:: console

   conda update -n cstar-env -c conda-forge cstar-ocean

C-Star, ROMS-Tools and UCLA-ROMS are released together, and the conda
packages pin versions that are known to work with one another. Installing
a package from its GitHub ``main`` branch (``pip install
git+https://github.com/CWorthy-ocean/C-Star.git@main``) gets you an
unreleased change, but can pair versions that are not yet compatible; prefer
tagged releases unless you need a specific fix.
