# Universal machine independent makefile for ROMS model
#========== ======= =========== ======== === ==== =====
# Set machine dependent definitions and rules.

include Makedefs.inc

# Build ROMS source list from unprocessed F77 files (typically .F extension) 
   SRCS = $(shell ls *$(UPF77_ext))

# Exclude files that are not part of main build
   EXCL = mpi_test checkkwds cppcheck srcscheck check_alfabeta check_rho_eos int_r3d sediment t3dmix_GP transp_nodes u3dbc_new visc3d_GP visc3d_S_FS
   EXCL := $(addsuffix $(UPF77_ext), $(EXCL))
   SRCS := $(filter-out $(EXCL), $(SRCS))

# Include files that do not exist yet but
# 	are generated as part of the build:
   INCL = check_srcs check_switches1 setup_kwds
   INCL := $(addsuffix $(UPF77_ext), $(INCL))
   SRCS := $(SRCS) $(INCL)

# Object and pre-processed source files are source list with extension changes
   RCS = $(SRCS:$(UPF77_ext)=$(PPF77_ext))
  OBJS = $(RCS:$(PPF77_ext)=$(OBJ_ext)) 

  SBIN = roms
 LROMS = libroms.a
 LROMS2 = 2/$(LROMS)
 LROMS3 = 2/$(LROMS) 3/$(LROMS)
 LROMS4 = 2/$(LROMS) 3/$(LROMS) 4/$(LROMS)

###########
# TARGETS #
###########

# ROMS excutable (This is the first target and hence the default):
$(SBIN): $(OBJS)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o a.out $(OBJS) $(LCDF) $(LMPI)
	mv a.out $(SBIN)


# Multilevel libraries

$(LROMS): $(OBJS)
	/bin/rm -f $(LROMS)
	ar r $(LROMS) $(OBJS)


check_forces: check_forces$(OBJ_ext) $(SBIN)
	cp -pv check_forces$(OBJ_ext) main$(OBJ_ext) 	
	$(LDR) $(FFLAGS) $(LDFLAGS) -o a.out $(OBJS) $(LCDF) $(LMPI)
	mv a.out check_forces

# Everything
.PHONY: all
all: tools depend $(SBIN)

# A program to test MPI halo exchange routines.
# - ------- -- ---- --- ---- -------- ---------
MPI_TEST = mpi_test	mpi_setup	exchange	mpi_exchange4\
							mpi_exchange8WA
MPI_TEST_RCS := $(addsuffix $(PPF77_ext), $(MPI_TEST))
MPI_TEST_OBJ := $(addsuffix $(OBJ_ext)  , $(MPI_TEST))

mpi_test: $(MPI_TEST_OBJ)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o mpi_test $(MPI_TEST_OBJ) $(LCDF) $(LMPI)

# Auxiliary utility programs
# --------- ------- --------
 TOOLS = cppcheck srcscheck checkkwds redefs

tools: $(TOOLS)

    TMP = .

cppcheck: cppcheck$(OBJ_ext)
	$(CFT) $(FFLAGS) $(LDFLAGS) -o cppcheck cppcheck$(OBJ_ext)

srcscheck: srcscheck$(OBJ_ext)
	$(CFT) $(FFLAGS) $(LDFLAGS) -o srcscheck srcscheck$(OBJ_ext)

checkkwds: checkkwds$(OBJ_ext)
	$(CFT) $(FFLAGS) $(LDFLAGS) -o checkkwds checkkwds$(OBJ_ext)

redefs: redefs$(OBJ_ext)
	$(CFT) $(FFLAGS) $(LDFLAGS) -o redefs redefs$(OBJ_ext)



checkdefs: check_switches1$(UPF77_ext) setup_kwds$(UPF77_ext)

check_switches1$(UPF77_ext): cppcheck cppdefs.opt
	./cppcheck cppdefs.opt
check_srcs$(UPF77_ext): srcscheck Makefile
	./srcscheck
setup_kwds$(UPF77_ext): checkkwds read_inp$(UPF77_ext)
	./checkkwds

.PHONY: depend
depend:  # makedepf90 is preferable but does not work on e.g. osx-arm64. Offer python fallback.
	@${ROMS_ROOT}/Tools-Roms/makedepf90 $(SRCS) > Make.depend  || \
	(echo "failed to generate dependency list with makedepf90, using python fallback"; \
	 ${ROMS_ROOT}/Tools-Roms/makedep.py  Make.depend)
	@echo 'Updated Make.depend (dependency list)' 

# Target to create tar file.
# ------ -- ------ --- -----
tarfile: clean
	tar cvf roms.tar Make* *$(UPF77_ext) *.h etalon_data.* README.*  *.in* *.mod

# Cleaning targets
# -------- -------
.PHONY: clean
clean:
	/bin/rm -rf *$(PPF77_ext) *$(OBJ_ext) *.a *.s *.mod *.trace *~ $(COMP_FILES)

.PHONY: allclean
allclean: clean
	cd 2; make -f ./Makefile clean; cd ..
	cd 3; make -f ./Makefile clean; cd ..
	cd 4; make -f ./Makefile clean; cd ..

.PHONY: clobber
clobber: clean
	/bin/rm -f check_switches1$(UPF77_ext) setup_kwds$(UPF77_ext) check_srcs$(UPF77_ext)
	/bin/rm -f $(SBIN) $(TOOLS) nsub
	/bin/rm -f core core.*
	/bin/rm -f *_genmod.mod *_genmod$(PPF90_ext)

.PHONY: help
help:
	@echo "Universal make procedure for ucla-roms, compatible with most UNIX systems."
	@echo "The following user options are available"
	@echo "COMPILER (fortran compiler):"
	@echo "  intel :  default, use ifort as compiler"
	@echo "  gnu :    use gfortran as compiler"
	@echo "MPI_DIST (mpi distribution):"
	@echo "   generic: default, uses mpifort as a wrapper for the fortran compiler"
	@echo "   intel  : use intel mpiifort as a wrapper for the fortran compiler"
	@echo "BUILD_MODE:"
	@echo "   regular : default, uses optimisation level 3"
	@echo "   debug   : compile with debug flags"
	@echo "   strict  : compile with -fp-model strict (intel only)"
	@echo "   vtune   : compile with flags for profiling with vtune (intel only)"
	@echo "   grof    : compile with flags for profiling with grof (intel only)"
	@echo "KEEP_PPSRC:"
	@echo "    true   : keep pre-processed source code after processing"
	@echo "    false  : delete pre-processed source code after processing"

# Automatically generated dependency list:
include Make.depend

