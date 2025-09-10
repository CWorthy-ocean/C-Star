CI_DIR      := $(patsubst %/,%, $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
CSTAR_CI_TAG_VERSION = $(shell git rev-parse --short HEAD)
ACTION_BUILD := "build"
ACTION_CLEAN := "clean"
ACTION_LOGIN := "login"
ACTION_PULL  := "pull"
ACTION_RUN   := "run"


.PHONY : _build
_build:
	CSTAR_CI_TAG_VERSION="$(CSTAR_CI_TAG_VERSION)" ./build.sh $(action) $(CI_DIR)/$(img) $(img)

IMG_BUILDBASE := buildbase
.PHONY: buildbase
buildbase:
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_BUILDBASE)

IMG_MPICH := mpich-4.2.2
.PHONY: mpich
mpich: # buildbase
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_MPICH)

IMG_MPI507 := mpi-5.0.7
.PHONY: openmpi
openmpi: # buildbase
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_MPI507)

IMG_HDF5 := hdf5
.PHONY: hdf5
hdf5: # buildbase mpi5
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_HDF5)

IMG_NETCDF := netcdf
.PHONY: netcdf
netcdf: # buildbase hdf5
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_NETCDF)

IMG_CONDA := condabase
.PHONY: conda
conda:
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_CONDA)

IMG_PYTHON := python
.PHONY: python
python:
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_PYTHON)

IMG_ROMS := roms
.PHONY: roms
roms: # netcdf
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_ROMS)

IMG_MARBL := roms-marbl
.PHONY: marbl
marbl: # netcdf roms
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_MARBL)

IMG_RUNNER := runner
.PHONY: runner
runner: # python roms marbl
	$(MAKE) _build action=$(ACTION_BUILD) img=$(IMG_RUNNER)


.PHONY: buildhelp
buildhelp:
	./build.sh

.PHONY: pull
pull:
	./build.sh $(ACTION_PULL) $(img)

.PHONY: pullall
pullall:
	$(MAKE) pull img=$(IMG_CONDA)
	$(MAKE) pull img=$(IMG_PYTHON)
	$(MAKE) pull img=$(IMG_BUILDBASE)
	$(MAKE) pull img=$(IMG_MPICH)
	$(MAKE) pull img=$(IMG_HDF5)
	$(MAKE) pull img=$(IMG_NETCDF)
	$(MAKE) pull img=$(IMG_ROMS)
	$(MAKE) pull img=$(IMG_MARBL)
	$(MAKE) pull img=$(IMG_RUNNER)

.PHONY: irun
irun:
	./build.sh $(ACTION_RUN) $(img)

.PHONY: clean
clean:
	./build.sh $(ACTION_CLEAN) $(img)

.PHONY: cleanall
cleanall:
	$(MAKE) clean img=$(IMG_RUNNER)
	$(MAKE) clean img=$(IMG_CONDA)
	$(MAKE) clean img=$(IMG_PYTHON)
	$(MAKE) clean img=$(IMG_MARBL)
	$(MAKE) clean img=$(IMG_ROMS)
	$(MAKE) clean img=$(IMG_NETCDF)
	$(MAKE) clean img=$(IMG_HDF5)
	$(MAKE) clean img=$(IMG_MPI507)
	$(MAKE) clean img=$(IMG_MPICH)
	$(MAKE) clean img=$(IMG_BUILDBASE)

.PHONY: login
login:
	./build.sh $(ACTION_LOGIN)

# note: swap openmpi for mpich below as desired
.PHONY: all
all: 
	$(MAKE) buildbase
	$(MAKE) python
	$(MAKE) mpich
	$(MAKE) hdf5
	$(MAKE) netcdf
	$(MAKE) roms
	$(MAKE) marbl
	$(MAKE) runner
