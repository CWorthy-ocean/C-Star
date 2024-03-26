# M2 Macbook (ARM64) version
include ../src/Makedefs.inc

# NEED TO OVERRIDE SOME OF MAKEDEFS.INC:

# - Can't use -ip optimization for tools (partit won't compile)
	FFLAGS = -O3 -fallow-argument-mismatch 
# - uncomment for debugging:
#	FFLAGS = -g -traceback -check all -CA -CB -CS
# - for profiling with grof to see source code, also comment '@/bin/rm -f $*.fpp':
#	FFLAGS = -O3 -pg

# OpenMP flags - needed for r2r_bry (and possibly others):
#	note, -qopenmp is the new -openmp but is still needed for maya
#        OMP_FLAG = -fpp -openmp
        OMP_FLAG = -cpp -fopenmp
#       OMP_FLAG = -qopenmp

# --------------------------------------------------------------------

ALL = partit ncjoin ncjoin_mpi extract_data_join nc3to4z zslice sample
#r2r_bry r2r_match_topo r2r_init

all:	$(info MAKING ROMS TOOLS:) mpc $(ALL)

#install: $(ALL)
#	mv $(ALL) ~/bin

# Note: the following .fpp: dependencies are needed to make everything
# go through CPP and take custom defined CPPFLAGS rather than compiling
# executables from .F directly.

chkindent: chkindent.fpp

# The following two are built from the same source
# and differ only CPP macro setting:

mreplace.fpp: mreplace.F
	$(CPP) -P $(CPPFLAGS) -DREPLACE mreplace.F > mreplace.fpp
	
partit: partit.o insert_node.o lenstr.o
	$(LDR) $(FFLAGS) $(LDFLAGS) -o partit partit.o insert_node.o lenstr.o $(LCDF)

ncjoin: ncjoin.o lenstr.o
	$(LDR) $(FFLAGS) $(LDFLAGS) -o ncjoin ncjoin.o lenstr.o $(LCDF)	
	
ncjoin_mpi: ncjoin_mpi.opt ncjoin_mod.o ncjoin_mpi.o lenstr.o
	$(LDR) $(FFLAGS) $(LDFLAGS) -o ncjoin_mpi ncjoin_mod.o ncjoin_mpi.o lenstr.o $(LCDF)
	
nc3to4z: nc3to4z.o lenstr.o read_string.o
	$(LDR) $(FFLAGS) $(LDFLAGS) -o nc3to4z nc3to4z.o lenstr.o read_string.o $(LCDF)
	
mpc: mpc.F Makefile
	$(CPP) $(CPPFLAGS) -P mpc.F > ./mpc.fpp
	$(CFT) $(FFLAGS) $(LDFLAGS) -o mpc ./mpc.fpp
	
extract_data_join: ncjoin_mod.o extract_data_join.o  lenstr.o
	$(LDR) $(FFLAGS) $(LDFLAGS) -o extract_data_join ncjoin_mod.o extract_data_join.o lenstr.o  $(LCDF)
	
particle_join: ncjoin_mod.o particle_join.o  lenstr.o
	$(LDR) $(FFLAGS) $(LDFLAGS) -o particle_join ncjoin_mod.o particle_join.o lenstr.o  $(LCDF)

R2R_INIT = r2r_init.o r2r_interp_init.o r2r_subs.o r2r_rotate.o r2r_vert_interp.o\
           set_depth.o etch_into_land.o spln2d.o tiling.o roms_read_write.o\
           read_string.o lenstr.o

r2r_init: $(R2R_INIT)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o r2r_init $(R2R_INIT) $(LCDF)

 R2R_BRY = r2r_bry.o r2r_bry_interp.o r2r_subs.o r2r_bry_rotate.o r2r_rotate.o\
           r2r_vert_interp.o set_depth.o etch_into_land.o spln2d.o tiling.o\
           def_bry_file.o roms_read_write.o read_string.o lenstr.o

r2r_bry: $(R2R_BRY)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o r2r_bry $(R2R_BRY) $(LCDF)

 ZSLICE = zslice.o set_depth.o sigma_to_z_intr.o read_add_write.o\
	  roms_read_write.o read_string.o lenstr.o tiling.o

 IPSLICE = ipslice.o set_depth.o sigma_to_z_intr.o rho_eos.o\
	   read_add_write.o roms_read_write.o read_string.o lenstr.o tiling.o

 SAMPLE = roms_read_write.o sample.o tiling.o\
          read_add_write.o read_string.o lenstr.o

R2R_MATCH_TOPO = r2r_match_topo.o r2r_interp_init.o mrg_zone_subs.o tiling.o\
                        spln2d.o roms_read_write.o read_string.o lenstr.o
                        
r2r_match_topo: $(R2R_MATCH_TOPO)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o r2r_match_topo $(R2R_MATCH_TOPO) $(LCDF)                                  
          
zslice: $(ZSLICE)
	$(LDR) $(FFLAGS) -o zslice $(ZSLICE) $(LCDF)

ipslice: $(IPSLICE)
	$(LDR) $(FFLAGS) -o ipslice $(IPSLICE) $(LCDF)

ipslice.fpp: zslice.F
	$(CPP) -P $(CPPFLAGS) -DIPSLICE zslice.F | ./mpc > ipslice.fpp

sample: $(SAMPLE)
	$(LDR) $(FFLAGS) -o sample $(SAMPLE) $(LCDF)

clean:
	/bin/rm -f *.fpp *.o *.a *.s *.trace *.mod

clobber: clean
	/bin/rm -f $(ALL) *.bak
	
include Make.depend
