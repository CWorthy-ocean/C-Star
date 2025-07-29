CI_DIR      := $(patsubst %/,%, $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
ACCOUNT     := ankona
VERSION     := 1
REPO_URI    := docker.io
TAG_VERSION  = $(shell git rev-parse --short HEAD)


IMG_BUILDBASE := "buildbase"
.PHONY: buildbase
buildbase:
	echo "TAG_VERSION: $(TAG_VERSION)"
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_BUILDBASE) $(ACCOUNT) $(REPO_URI)

IMG_MPI507 := "mpi-5.0.7"
.PHONY: mpi5
mpi5: # buildbase
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_MPI507) $(ACCOUNT) $(REPO_URI)

IMG_HDF5 := "hdf5"
.PHONY: hdf5
hdf5: # buildbase mpi5
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_HDF5) $(ACCOUNT) $(REPO_URI)

IMG_NETCDF := "netcdf"
.PHONY: netcdf
netcdf: # buildbase hdf5
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_NETCDF) $(ACCOUNT) $(REPO_URI)

IMG_CONDA := "condabase"
.PHONY: conda
conda:
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_CONDA) $(ACCOUNT) $(REPO_URI)

IMG_PYTHON := "python"
.PHONY: python
python:
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_PYTHON) $(ACCOUNT) $(REPO_URI)

IMG_ROMS := "roms"
.PHONY: roms
roms: # netcdf
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_ROMS) $(ACCOUNT) $(REPO_URI)

IMG_MARBL := "roms-marbl"
.PHONY: marbl
marbl: # netcdf roms
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_MARBL) $(ACCOUNT) $(REPO_URI)

IMG_RUNNER := "runner"
.PHONY: runner
runner: # conda roms marbl
	TAG_VERSION=$(TAG_VERSION) ./build.sh $(CI_DIR) $(IMG_RUNNER) $(ACCOUNT) $(REPO_URI)

.PHONY: pull
pull:
	podman-hpc pull $(REPO_URI)/$(ACCOUNT)/cstar-$(img):latest

.PHONY: pullall
pullall:
	$(MAKE) pull img=$(IMG_CONDA)
	$(MAKE) pull img=$(IMG_PYTHON)
	$(MAKE) pull img=$(IMG_BUILDBASE)
	$(MAKE) pull img=$(IMG_MPI507)
	$(MAKE) pull img=$(IMG_HDF5)
	$(MAKE) pull img=$(IMG_NETCDF)
	$(MAKE) pull img=$(IMG_ROMS)
	$(MAKE) pull img=$(IMG_MARBL)
	$(MAKE) pull img=$(IMG_RUNNER)

.PHONY: irun
irun:
	podman-hpc run --rm -i -t $(ACCOUNT)/cstar-$(img):latest bash

.PHONY: clean
clean:
	podman-hpc images --format "{{.Repository}}:{{.Tag}}" | sed 's/^localhost\///' | grep "$(img)" | xargs -r podman-hpc rmi --force --ignore

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
	$(MAKE) clean img=$(IMG_BUILDBASE)

.PHONY: login
login:
	podman-hpc login "$(REPO_URI)"

.PHONY: all
all: 
	$(MAKE) buildbase
	$(MAKE) conda
	$(MAKE) python
	$(MAKE) mpi5
	$(MAKE) hdf5
	$(MAKE) netcdf
	$(MAKE) roms
	$(MAKE) marbl
	$(MAKE) runner
