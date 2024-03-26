# M2 Macbook (ARM64) version
# ----------------------------------------------------

# Compilation rules for file types:
#   (used for every file until the final linking step)

.SUFFIXES: .o .fpp .F .f90

.F.o:
	@$(CPP) -P $(CPPFLAGS) $*.F | mpc > $*.fpp
	$(CFT) -c $(FFLAGS) -o $*.o $*.fpp $(LCDF)
	@/bin/rm -f $*.fpp
.F.fpp:
	$(CPP) -P $(CPPFLAGS) $*.F | mpc > $*.fpp
.fpp.o:
	$(CFT) -c $(FFLAGS) -o $*.o $*.fpp $(LCDF)
	@/bin/rm -f $*.fpp

.f90.o:
	$(CFT) -c $(FFLAGS) -o $*.o $*.f90 $(LCDF)
.F:
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<
.fpp:
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<
.f90:
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<
.o:
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<

# C-preprocessor (cpp):
#	cpp from Intel compiler package "fpp" treats __IFC as
#	a pre-defined, so there is no need to include it into CPPFLAGS, but
#	the standard CPP is not aware of this.

#   The CPP line below needs to have 'spaces' not 'tabs' unlike the rest.
    CPP = /usr/bin/cpp -traditional -D__IFC
	CPPFLAGS = -I${MPIHOME}/include -I${NETCDFHOME}/include -I${MARBL_ROOT}/include

#   Since we no longer keep .fpp files after compilation, if you want to still
#   see the .fpp to confirm what code is left after c-pre-processing (CPPFLAGS
#   removed) or for runtime debug line numbers, use the following
#   in the Compile/ folder (change 'main' for your file of interest):
#   /lib/cpp -traditional -D__IFC -P main.F | ./mpc > main.fpp

# Path names (non-hydrostatic & NetCDF libraries):
	NHMG_ROOT = $(ROMS_ROOT)/NHMG
	NHMG_LIB  = -L$(NHMG_ROOT)/lib -lnhmg
	NHMG_INC  = $(NHMG_ROOT)/include

	VPATH = ${MPIHOME}/include:${NHMG_INC}
	CPATH = ${MPIHOME}/include:${NHMG_INC}

# Get netCDF paths and options ('shell' is needed to use command)
	NETCDFF_INC=$(shell nf-config --fflags)
	NETCDFF_LIB=$(shell nf-config --flibs)
	NETCDFC_INC=$(shell nc-config --cflags)
	NETCDFC_LIB=$(shell nc-config --libs)

# MARBL
        MARBL_INC = -I${MARBL_ROOT}/include/gnu-mpi
    	MARBL_LIB = -lmarbl-gnu-mpi -L${MARBL_ROOT}/lib


# COMPILER SETTINGS:

# OpenMP flags:
#	note, -qopenmp is the new -openmp but is still needed for maya
#	OMP_FLAG = -fpp -openmp
#	OMP_FLAG = -qopenmp

# Large memory runs (e.g. for bgc):
#	LARGE_MEM_FLAG = -mcmodel=medium
#	LARGE_MEM_FLAG = -mcmodel=large

# Fortran compiler flags:
#	CFTFLAGS = -pc64 -auto

# Fortran compiler name and options:
#	- using non-intel MPI:
	CFT =  mpifort $(OMP_FLAG) $(CFTFLAGS) $(LARGE_MEM_FLAG)
#	- using intel MPI:
#	CFT = mpiifort $(OMP_FLAG) $(CFTFLAGS) $(LARGE_MEM_FLAG)

# Fortran loader name and options: (mpifort has replaced legacy mpif90)
#	- using non-intel MPI:
	LDR =  mpifort $(OMP_FLAG) $(CFTFLAGS) $(LARGE_MEM_FLAG)
#	- using intel MPI:
#	LDR = mpiifort $(OMP_FLAG) $(CFTFLAGS) $(LARGE_MEM_FLAG)

# Fortran compiler options/flags: (Legacy to have both CFTFLAGS & FFLAGS)
# - Optimized: (-ip = additional interprocedural optimizations for single-file compilation. )
#	FFLAGS = -O3 -ip
# - Optimized: (-flto = linktime optimisation, similarly to -ip/-ipo)
	FFLAGS = -O3
# - uncomment for debugging:
#	FFLAGS = -g -traceback -check all -CA -CB -CS
# - uncomment for traceback only (traceback not as accurate as full debug mode...)
#	FFLAGS = -O3 -ip -traceback
# - for code_check script ensure consistency with -fp-model strict:
#	FFLAGS = -O3 -fp-model strict
# - for profiling with vtune to see source code, also comment '@/bin/rm -f $*.fpp':
#	FFLAGS = -O3 -g -debug inline-debug-info -parallel-source-info=2
# - for profiling with grof to see source code, also comment '@/bin/rm -f $*.fpp':
#	FFLAGS = -O3 -ip -pg

# Options to link to libraries & modules (NetCDF, etc):
#	LCDF = $(NHMG_LIB) $(NETCDFF_LIB) $(NETCDFC_LIB) $(NETCDFF_INC) $(NETCDFC_INC)
#	LCDF = -lmpi -lnetcdf -lnetcdff -L$(MPIHOME)/lib -L$(NETCDFHOME)lib -I$(MPIHOME)/include -I$(NETCDFHOME)include	-L${MARBL_ROOT}/lib -lmarbl-gnu-mpi -I${MARBL_ROOT}/include/gnu-mpi ${NHMG_LIB}
      	LCDF = $(NHMG_LIB) $(NETCDFF_LIB) $(NETCDFC_LIB) $(MARBL_LIB) $(NETCDFF_INC) $(NETCDFC_INC) $(MARBL_INC)		    

# -------------------------------------------------------------

# Compiler settings info:

#	-fpp2 is required only if -openmp is present.
#	Not having -fpp2 here just causes compiler warning (-fpp is set to
#	level 2 by -openmp), but other than that has no effect.

#	Switch -pc80 increases precision of floating point operation to
#	64 bits (vs. 53 bits double precision default).
#
#	-qp  compiles and links for function profiling with  gprof(1);
#	this is the same as specifying -p or -pg.
#
#	Setting  FFLAGS = -O2 -mp (or lower optimization level) is needed
#	to pass ETALON_CHECK: -O3 causes roundoff-level differences from
#	the length of innermost i-loop (the results still pass ETALON_CHECK
#	if  NP_XI = NSUB_X = 1, regardless of partition in ETA-direction).
#	As of ifort v. 11.0.xxx -mp is superseeded by -fp-model <arg> flag.

#	Flags collected under LARGE_MEM_FLAG are needed only if exceeding
#	2 GBytes of memory: both -mcmodel (sets size of addressing pointers)
#	and -i-dynamic (ensures proper linking with Intel dynamic libraries
#	must be specified.

#	-pc = control of floating point precision
#	pc64 = double precision (53 bit)
#	pc80 = extended precision (64 bit)) - this is the default

#	-auto: This option places local variables (scalars and arrays of all
#	types), except those declared as SAVE, on the run-time stack. It is
#	as if the variables were declared with the AUTOMATIC attribute.
#	This option may provide a performance gain for your program, but if
#	your program depends on variables having the same value as the last
#	time the routine was invoked, your program may not function properly.
#   the default is 'auto-scalar'.

#	LARGE_MEM_FLAG:
#	-mcmodel: Tells the compiler to use a specific memory model to
#	generate code and store data.
#	-shared-intel: Causes Intel-provided libraries to be linked in
#	dynamically. This is the default for -mcmodel=medium or -mcmodel=large
#	(-shared-intel was previously -i-dynamic, now depracated)

#	LIBRARY & MODULE LINKING:
#	Had issues compiling ROMS to the Israeli cluster. This was because previously
#	LCDF was only including in the linking stage in Makefile. Now that we 'use' modules,
#	the compiler needs to find the module while compiling each file, otherwise it will
#	not know if those vars/subroutines used actually exist.
#	This was not an issue before with #include netcdf.inc because the external function
#	is declared within the source code, so it only needs to be linked at the end.
#	The order of $(NETCDFF_LIB) $(NETCDFC_LIB) was important, I am not sure why.

