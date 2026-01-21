Installation
============

Clone the repository
--------------------

To obtain the latest development version, clone `this
repository <https://github.com/CWorthy-ocean/C-Star>`__:

.. code-block:: console
   :caption: Cloning the repository

   git clone https://github.com/CWorthy-ocean/C-Star.git
   cd C-Star

Create a python virtual environment
-----------------------------------

Select **one** of the following environment configuration files provided
in the repository to create your environment.

* Use ``ci/environment_hpc.yml`` on a supported HPC system (environment management by Linux Environment Modules)
* Use ``ci/environment.yml`` on a generic machine like a laptop (environment managed by conda)

.. tab-set::

   .. tab-item:: HPC environment

      .. code-block:: console
         :caption: Creating a virtual environment on HPC

         conda env create -f ci/environment_hpc.yml

   .. tab-item:: Standard environment

      .. code-block:: console
         :caption: Creating a virtual environment on non-HPC

         conda env create -f ci/environment.yml

Once the environment is created, ensure it is activated:

.. code-block:: console
   :caption: Activating the virtual environment

   conda activate cstar_env

Install C-Star
--------------

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

      This method installs optional dependencies for development and testing. 
      
      It is required for those contributing code to C-Star.

      .. code-block:: console

         pip install -e .[dev,test]

Verify the installation
-----------------------

Execute the following command to verify that ``C-Star`` is installed correctly:

.. code-block:: console
   :caption: Verifying package installation
   
   cstar --version
