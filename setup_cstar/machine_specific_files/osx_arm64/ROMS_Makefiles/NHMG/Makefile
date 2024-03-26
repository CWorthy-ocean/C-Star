# M2 Macbook (ARM64) version
# This line de-activates all implicit rules
.SUFFIXES:

# Fortran Compiler
#FC = ifort
FC = gfortran

# Normal options for the compiler
#FFLAGS =  -O3 -fp-model source -I/opt/mpich-3.2/include/
#FFLAGS =  -O3 -fp-model source -I/home/devin/mpich-install/include
FFLAGS = -O3 

# Debugging options for the compiler
#FFLAGS =  -g -check -warn

# Rule to make *.o out of *.f90
%.o : %.f90
	$(FC) -c $(FFLAGS)  $<


sources = mg_autotune.f90 mg_cst.f90 mg_gather.f90 \
	mg_grids.f90 mg_horiz_grids.f90 mg_intergrids.f90 mg_mpi_exchange.f90\
	mg_mpi.f90 mg_namelist.f90 mg_netcdf_out.f90 mg_projection.f90\
	mg_relax.f90 mg_solvers.f90 mg_tictoc.f90 mg_vert_grids.f90 \
	nhmg_debug.f90 nhmg.f90

objects = $(sources:.f90=.o)

libnhmg =  ../lib/libnhmg.a


.PHONY: all clean depend

all:
	cd src; make
