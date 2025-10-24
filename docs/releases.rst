Release notes

.. _v1.0.0:
v1.0.0
------

New features:
~~~~~~~~~~~~~
- Add support for river forcing
- Add support for forcing corrections (used in legacy ROMS configurations)

Breaking Changes:
~~~~~~~~~~~~~~~~~
- Merge Case and Component classes into a single class, Simulation, simplifying internal/blueprint structure. Remove old Case and Component modules.
- Rename `caseroot` to `directory` 
- Rename BaseModel to ExternalCodeBase
- Remove `start_time` and `end_time` parameters from `InputDataset.get()`, these are now obtained from the corresponding attributes
- Rename `ROMSComponent.namelists` to `ROMSSimulation.runtime_code` and `ROMSComponent.additional_source_code` to `ROMSSimulation.compile_time_code`

Internal Changes:
~~~~~~~~~~~~~~~~~
- Update calls to `roms-tools` to reflect latest changes in API
- Update internal/test blueprints to reflect new structure
- New backend data retrieval system `cstar.io`
- Add ROMSRuntimeSettings class with ability to parse and create roms `.in` files
- Save partitioned `ROMSInputDatasets` in the same directory as their un-partitioned versions, rather than a subdirectory "PARTITIONED"

Documentation:
~~~~~~~~~~~~~~
- Run most examples in documentation locally (unless HPC-specific), so HPC access is not required
- Add 'How-to Guides' section
- Remove tutorial page on using roms-tools
- Replace all URLs to other doc pages with relative links

Bugfixes:
~~~~~~~~~
- Complete missing unit test coverage from Case and Component in new Simulation modules
- Add `expanduser` to `Path` instances to allow tildes to represent root in paths
- Correct issue where `AdditionalCode.modified_files` list indices did not correspond to `AdditionalCode.files`

  
=============
.. _v0.0.8-alpha:

v0.0.8-alpha ()
---------------------------

New features:
~~~~~~~~~~~~~
- Add Case.persist() and Case.restore() methods to allow continuation of work in a new session
- Add ExecutionHandler class to track tasks run locally (LocalProcess subclass) or submitted to a job scheduler (SchedulerJob subclass)
- Improved tracking of local InputDataset and AdditionalCode files to prevent repeat fetching
- Add ability to read blueprints from URL
- Remote yaml files are now accessed via requests rather than Pooch, negating need for hash checks

Bugfixes:
~~~~~~~~~
- git and DataSource information are now read-only attributes throughout
  

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
