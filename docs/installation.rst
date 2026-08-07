Installation
============

Install with conda (recommended)
--------------------------------

``C-Star`` is published on `conda-forge <https://anaconda.org/conda-forge/cstar-ocean>`__.
Pick the variant that matches your machine:

.. tab-set::

   .. tab-item:: Laptop / workstation

      ``cstar-ocean-standalone`` installs C-Star **plus a complete build
      toolchain** (compilers, MPI, netCDF-Fortran, PnetCDF, CMake, rsync) from
      conda-forge, so ROMS can be compiled without any system dependencies:

      .. code-block:: console
         :caption: Creating an environment with C-Star and its toolchain

         conda create -n cstar-env -c conda-forge cstar-ocean-standalone

   .. tab-item:: HPC system

      On a supported HPC system the compiler/MPI/netCDF toolchain comes from
      the site's Environment Modules, so install just the ``cstar-ocean``
      package:

      .. code-block:: console
         :caption: Creating an environment with C-Star (toolchain from modules)

         conda create -n cstar-env -c conda-forge cstar-ocean

Then activate the environment:

.. code-block:: console
   :caption: Activating the environment

   conda activate cstar-env

That's it — skip ahead to :ref:`verify-install`.

Install from source (developers)
--------------------------------

Use this path if you plan to modify C-Star itself, or need an unreleased
version.

Clone the repository
~~~~~~~~~~~~~~~~~~~~

To obtain the latest development version, clone `this
repository <https://github.com/CWorthy-ocean/C-Star>`__:

.. code-block:: console
   :caption: Cloning the repository

   git clone https://github.com/CWorthy-ocean/C-Star.git
   cd C-Star

Create a python virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Select **one** of the following environment configuration files provided
in the repository to create your environment.

* Use ``environment-hpc.yml`` on a supported HPC system (toolchain provided by
  Linux Environment Modules)
* Use ``environment-laptop.yml`` on a generic machine like a laptop — it
  bundles the same conda-forge toolchain that ``cstar-ocean-standalone``
  provides, for developers who prefer an editable install over the released
  conda package

.. tab-set::

   .. tab-item:: HPC environment

      .. code-block:: console
         :caption: Creating a virtual environment on HPC

         conda env create -f environment-hpc.yml

   .. tab-item:: Standard environment

      .. code-block:: console
         :caption: Creating a virtual environment on non-HPC

         conda env create -f environment-laptop.yml

Once the environment is created, ensure it is activated:

.. code-block:: console
   :caption: Activating the virtual environment

   conda activate cstar-env

Install C-Star
~~~~~~~~~~~~~~

Finally, install ``C-Star`` in your active conda environment:

.. tab-set::

   .. tab-item:: Developers

      This method installs the package in editable mode.

      It is recommended for developers modifying the source code.

      .. code-block:: console

         pip install -e .

   .. tab-item:: Users

      This method installs the package in non-editable mode.

      It is recommended for those building on top of C-Star.

      .. code-block:: console

         pip install .

   .. tab-item:: Contributors

      This method installs optional dependencies for development and documentation generation.

      It is required for those contributing code to C-Star.

      .. code-block:: console

         pip install -e .[dev,docs]

.. _verify-install:

Verify the installation
-----------------------

Execute the following command to verify that ``C-Star`` is installed correctly:

.. code-block:: console
   :caption: Verifying package installation

   cstar --version
