##############################
#        USER OPTIONS        #

#intel (gnu) uses ifort (gfortran) compiler flags
COMPILER ?=intel
#MPI distribution. generic -> mpifort, intel -> mpiifort
MPI_DIST ?=generic
# Configure FFLAGS by setting BUILD_MODE to "debug","strict","vtune",or "grof"
BUILD_MODE ?=regular
# Keep pre-processed source code or not:
KEEP_PPSRC ?=false
##############################
#       FILE EXTENSIONS      #

#unprocessed  Fortran 77 code
UPF77_ext = .F
#unprocessed  Fortran 90 code
UPF90_ext = .F90
#preprocessed Fortran 77 code
PPF77_ext = .f
#preprocessed Fortran 90 code
PPF90_ext = .f90
#object files
OBJ_ext = .o

##############################
#    PRELIMINARY CHECKS      #

# If we're using MacOS, typically the filesystem is case-insensitive
# so we should use an alternative extension for preprocessed files:
OS := $(shell uname -s)
ifeq ($(OS),Darwin)
    PPF77_ext = .fpp
endif

# Check MPI distribution:
ifeq ($(MPI_DIST),generic)
	LDR =  mpifort
        CFT =  mpifort
else ifeq ($(MPI_DIST),intel)
        LDR = mpiifort
        CFT = mpiifort
endif

# Check whether mpifort is wrapping the anticipated compiler
ifeq ($(CFT),mpifort)
   ifeq ($(COMPILER),gnu)
      expected_fc=GNU Fortran
   else ifeq ($(COMPILER),intel)
      expected_fc=ifort
   endif
   MPIVER := $(shell mpifort --version | head -n1)
   ifeq ($(findstring $(expected_fc),$(MPIVER)),)
	_ := $(error "COMPILER is set to $(COMPILER) but mpifort wraps $(MPIVER). Call make with the COMPILER= argument (either intel or gnu)")
   endif
endif 

# For BUILD_MODE = "vtune" or "grof" always keep source code:
ifeq ($(BUILD_MODE),vtune)
   KEEP_PPSRC=true
else ifeq ($(BUILD_MODE),grof)
   KEEP_PPSRC=true
endif

#Check whether we're compiling with MARBL:
ifeq ($(wildcard cppdefs.opt),cppdefs.opt)
	USEMARBL := $(shell grep -q '^[^!]*\#[[:space:]]*define[[:space:]]*MARBL' cppdefs.opt && echo true || echo false )
endif

# Compilation rules for file types:
#   (used for every file until the final linking step)

.SUFFIXES: $(OBJ_ext) $(PPF77_ext) $(UPF77_ext) $(PPF90_ext)
# Unprocessed F77 -> object file (typically .F->.o)
$(UPF77_ext)$(OBJ_ext):
	@$(CPP) -P $(CPPFLAGS) $*$(UPF77_ext) | mpc > $*$(PPF77_ext)
	$(CFT) -c $(FFLAGS) -o $*$(OBJ_ext) $*$(PPF77_ext) $(LCDF)
	@if [ "$(KEEP_PPSRC)" = "false" ]; then \
	   /bin/rm -f $*$(PPF77_ext); \
	fi
# unprocessed F77 -> preprocessed (typically .F ->.f)
$(UPF77_ext)$(PPF77_ext):
	$(CPP) -P $(CPPFLAGS) $*$(UPF77_ext) | mpc > $*$(PPF77_ext)
# preprocessed F77 -> object file (typically .f->.o)
$(PPF77_ext)$(OBJ_ext):
	$(CFT) -c $(FFLAGS) -o $*$(OBJ_ext) $*$(PPF77_ext) $(LCDF)
	@if [ "$(KEEP_PPSRC)" = "false" ]; then \
	   /bin/rm -f $*$(PPF77_ext); \
	fi
# preprocessed F90 -> object file
$(PPF90_ext)$(OBJ_ext):
	$(CFT) -c $(FFLAGS) -o $*$(OBJ_ext) $*$(PPF90_ext) $(LCDF)
$(UPF77_ext):
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<
$(PPF77_ext):
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<
$(PPF90_ext):
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<
$(OBJ_ext):
	$(CFT) -o $@ $(FFLAGS) $(LDFLAGS) $<

# C-preprocessor (cpp):
#	cpp from Intel compiler package "fpp" treats __IFC as
#	a pre-defined, so there is no need to include it into CPPFLAGS, but
#	the standard CPP is not aware of this.

#   The CPP line below needs to have 'spaces' not 'tabs' unlike the rest.
        CPP = /usr/bin/cpp
	CPPFLAGS =  -traditional -D__IFC -I${MPIHOME}/include -I${NETCDFHOME}/include
ifeq ($(USEMARBL),true)
	CPPFLAGS+= -I${MARBL_ROOT}/include
endif

#   Since we no longer keep preprocessed files after compilation, if you want to still
#   see them to confirm what code is left after c-pre-processing (CPPFLAGS
#   removed) or for runtime debug line numbers, use the following
#   in the Compile/ folder (change 'main' for your file of interest):
#   /lib/cpp -traditional -D__IFC -P main$(UPF77_ext) | ./mpc > main.$(PPF77_ext)

# Path names (non-hydrostatic & NetCDF libraries):
	NHMG_ROOT = $(ROMS_ROOT)/NHMG
	NHMG_LIB  = -L$(NHMG_ROOT)/lib -lnhmg
	NHMG_INC  = $(NHMG_ROOT)/include

	VPATH = ${MPIHOME}/include:${NHMG_INC}
	CPATH = ${MPIHOME}/include:${NHMG_INC}

# Get netCDF paths and options ('shell' is needed to use command)
# Some systems return empty strings for `n{c,f}-config --args`, so using $(or as a fallback
	NETCDFF_INC=$(or $(shell nf-config --fflags), -I$(NETCDFHOME)include)
	NETCDFF_LIB=$(or $(shell nf-config --flibs) , -L$(NETCDFHOME)lib -lnetcdf -lnetcdff)
	NETCDFC_INC=$(or $(shell nc-config --cflags), -I$(NETCDFHOME)include)
	NETCDFC_LIB=$(or $(shell nc-config --libs),   -L$(NETCDFHOME)lib -lnetcdf)

# MARBL
ifeq ($(USEMARBL),true)
   ifeq ($(COMPILER),gnu)
        MARBL_INC = -I$(MARBL_ROOT)/include/gnu-mpi
        MARBL_LIB = -lmarbl-gnu-mpi -L$(MARBL_ROOT)/lib
   else ifeq ($(COMPILER),intel)
        MARBL_INC = -I$(MARBL_ROOT)/include/intel-mpi
        MARBL_LIB = -lmarbl-intel-mpi -L$(MARBL_ROOT)/lib
   endif
endif

# COMPILER SETTINGS:

# OpenMP flags:
ifeq ($(COMPILER),intel)
	OMP_FLAG = -qopenmp
else ifeq ($(COMPILER),gnu)
	OMP_FLAG = -cpp -fopenmp
endif

# Large memory runs (e.g. for bgc):
#	LARGE_MEM_FLAG = -mcmodel=medium
#	LARGE_MEM_FLAG = -mcmodel=large

# Fortran compiler flags:
ifeq ($(COMPILER),intel)
	CFTFLAGS= -pc64 -auto
endif

# Fortran compiler options:
	CFTFLAGS += $(OMP_FLAG) $(LARGE_MEM_FLAG)

# Fortran loader options:
	LDFLAGS = $(OMP_FLAG) $(CFTFLAGS) $(LARGE_MEM_FLAG)

# Fortran compiler options/flags:
# - Optimized: (-ip = additional interprocedural optimizations for single-file compilation. )
ifeq ($(COMPILER),intel)
	FFLAGS = -O3 -ip
    ifeq ($(BUILD_MODE),debug)
	FFLAGS = -g -traceback -check all
# for code_check script ensure consistency with -fp-model strict:
    else ifeq ($(BUILD_MODE),strict)
	FFLAGS += -fp-model strict
# - for profiling with vtune to see source code:
    else ifeq ($(BUILD_MODE),vtune)
	FFLAGS = -g -debug inline-debug-info -parallel-source-info=2
# - for profiling with grof to see source code:
    else ifeq ($(BUILD_MODE),grof)
	FFLAGS += -pg
    endif
else ifeq ($(COMPILER),gnu)
	FFLAGS = -O3 -fallow-argument-mismatch
    ifeq ($(BUILD_MODE),debug)
	FFLAGS = -fallow-argument-mismatch -g -fbacktrace -fcheck=all
    endif
endif


# Options to link to libraries & modules (NetCDF, etc):
        LCDF = $(NHMG_LIB) $(NETCDFF_LIB) $(NETCDFC_LIB) $(NETCDFF_INC) $(NETCDFC_INC)
ifeq ($(USEMARBL),true)
	LCDF += $(MARBL_LIB) $(MARBL_INC)
endif

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
