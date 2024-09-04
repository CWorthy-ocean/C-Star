# Installation


## Create a new conda environment

```
conda create -n "cstar_environment" python=3.12 pooch pyyaml
```

**If you are installing on an M2 (Apple silicon) Mac**, you'll also need to add:

```
compilers=1.6.0 netcdf-fortran=4.6.1 mpich=4.1.2 nco=5.1.8 -c conda-forge 
```

It may also be worth including `ipython`,`xarray`,`numpy`, `matplotlib` and `jupyter` in the environment to work with any output, though these are not needed to run C-Star.

## Install from GitHub

To obtain the latest development version, clone [this repository](https://github.com/CWorthy-ocean/C-Star):


```
git clone https://github.com/CWorthy-ocean/C-Star.git
``` 
With your C-star conda environment active, install C-Star as follows:
```
cd C-Star
pip install -e .
``` 

