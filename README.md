[![codecov](https://codecov.io/gh/CWorthy-ocean/C-Star/graph/badge.svg?token=HAPZGL2LWF)](https://codecov.io/gh/CWorthy-ocean/C-Star)

# Installation
### i. Create a new conda environment containing any dependencies:

```conda create -n "cstar_environment python=3.12 pooch pyyaml```

**If you are installing on an M2 (Apple silicon) Mac**, you'll also need to add:

```compilers=1.6.0 netcdf-fortran=4.6.1 mpich=4.1.2 nco=5.1.8 -c conda-forge ```

It may also be worth including `ipython`,`xarray`,`numpy`, `matplotlib` and `jupyter` in the environment to work with any output, though these are not needed to run C-Star.

### ii. Install C-Star locally with pip
With your C-star conda environment active, use `pip install -e .` at the top level of this repository.

# Using C-Star:
It is recommended that first-time users see the example notebook `examples/cstar_example_notebook.ipynb`. A summary is provided here:

## Overview of C-Star structures
- A Case (`cstar.Case`) is the primary object of C-Star. It contains all the necessary information for a user to run a reproducable Earth system simulation.
- A Case is built from Components (`cstar.Component`), each representing a specific configuration of a model of one part of the overall system being simulated. In this notebook we'll be working with an ocean circulation component and a biogeochemistry component.
- A Component object, meanwhile, consists of, at least, a base model (`cstar.BaseModel`), and optionally additional code (`cstar.AdditionalCode`), input datasets (`cstar.InputDataset`), and discretization information needed to run the base model in the specific configuration in question. In the simplest scenario, we can imagine a Case consisting of a single Component which is just a base model in an bundled example configuration (like an ocean double gyre) with itsings run in serial withtical initial and forcing data (i.e. no additional code, input datasets, or parallelization information needed).
- You can find more information on C-Star `Case`, `Component`, `BaseModel`, `AdditionalCode`, and `InputDataset` objects by querying, e.g., `cstar.Component?`.

## Constructing a C-Star Case:
A Case can be instantiated in one of two ways:

- using the standard constructor (after manually constructing the Component objects that will make up the Case):

```
	my_case=cstar.Case(list_of_component_objects,\
	 	  	 name='case_name',\
	  		 caseroot='/path/to/where/case/will/be/run')
```


- From a pre-defined "blueprint", using

```
	my_case=cstar.Case.from_blueprint('path/to/blueprint.yaml')
```

An example blueprint file is provided at

```<repository_top_level>/cstar/examples/cstar_blueprint_roms_marbl_example.yaml```

## Running a C-Star Case:
Once a case has been constructed, the sequence of steps to run it is as follows:

- `my_case.setup()`:
	- Prompts the user to install any external codebases that cannot be located on the machine
	- Downloads local copies of any input datasets and additional code described by each component's `AdditionalCode` and `InputDataset` objects that are needed to run the case to the `Case.caseroot` directory
- `my_case.build()` compiles an executable of the primary Component's BaseModel. The path to the executable is saved to the `BaseModel.exe_path` attribute
- `my_case.pre_run()` performs any pre-processing steps necessary to run the model
- `my_case.run(account_key='MY_ACCOUNT_KEY')` either executes the case or submits it to the appropriate job scheduler with the user's provided account key. If running on a machine without a scheduling system (such as a laptop), the optional `account_key` argument can be ignored. Additional arguments include `walltime='HH:MM:SS'` and `job_name='my_job_name'`
- `my_case.post_run()` performs any necessary post-processing steps to work with the output.
