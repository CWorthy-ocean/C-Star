# M2 Macbook (ARM64) version
# This line de-activates all implicit rules
.SUFFIXES:

# Set machine dependent definitions and rules.
include ../../src/Makedefs.inc

# Fortran Compiler
#FC = ifort
FC = gfortran
# - for pacific cluster uncomment:
#FC = mpiifort

# Normal options for the compiler
#FFLAGS =  -O3 -fp-model source $(LCDF)
FFLAGS =  -O3 $(LCDF)

# Debugging options for the compiler
#FFLAGS =  -g -check -warn
#FFLAGS = -g -traceback -check all -CA -CB -CS

# Library paths:
VPATH = ${NETCDFHOME}/include:${ROMS_ROOT}/NHMG/include
CPATH = ${MPIHOME}/include:${NETCDFHOME}/include:${ROMS_ROOT}/NHMG/include

# Rule to make *.o out of *.f90
%.o : %.f90
	$(FC) -c $(FFLAGS)  $<


sources = mg_autotune.f90 mg_cst.f90 mg_gather.f90 \
	mg_grids.f90 mg_horiz_grids.f90 mg_intergrids.f90 mg_mpi_exchange.f90\
	mg_mpi.f90 mg_namelist.f90 mg_netcdf_out.f90 mg_projection.f90\
	mg_relax.f90 mg_solvers.f90 mg_tictoc.f90 mg_vert_grids.f90 \
	nhmg_debug.f90 nhmg.f90 mg_netcdf_out_true.f90

objects = $(sources:.f90=.o)

libnhmg =  ../lib/libnhmg.a


.PHONY: all clean depend

all: $(libnhmg)

$(libnhmg): ${libnhmg}(${objects})
	cp *.mod ../include/

depend depend.mk:
#makedepf90 -D... -free -Wmissing -Wconfused  -nosrc -u ... ${sources} > depend.mk
	makedepf90 -free ${sources} > depend.mk

clean:
	rm -f ${libnhmg} ${objects} *.mod

include depend.mk
