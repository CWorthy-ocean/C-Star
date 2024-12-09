Release notes
=============

.. _v0.0.3-alpha:

v0.0.3-alpha (9th Dec 2024)
---------------------------

New features:
~~~~~~~~~~~~~
- Add support for SDSC Expanse HPC

Bugfixes:
~~~~~~~~~
- Fix bug where in certain circumstances environment variables were checked for before being set, prompting user to install already installed externals

.. _v0.0.1-alpha:

v0.0.1-alpha (6th Dec 2024)
---------------------------

The first release of C-Star!

This release provides basic functionality, including the ability to create, import, and export a ":term:`blueprint`" for reproducible ocean model simulations (a C-Star ":term:`Case`") using supported ocean models (ROMS, optionally with MARBL biogeochemistry) and run those Cases locally or on supported HPC systems (via Slurm and PBS). 
There is support for using existing model input data in netCDF format, or creating new input data via integration with the roms-tools library.

Note that the python API is not yet stable, and some aspects of the schema for the blueprint will likely evolve. 
Therefore whilst you are welcome to try out using the package, we cannot yet guarantee backwards compatibility. 
We expect to reach a more stable version in Q1 2025.
