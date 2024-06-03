include ../src/Makedefs.inc

# File extensions are set as variables in Makedefs.inc to
# account for case-insensitive filesystems. Default values are:

#UPF77_ext = .F   #unprocessed  Fortran 77 code
#UPF90_ext = .F90 #unprocessed  Fortran 90 code
#PPF77_ext = .f   #preprocessed Fortran 77 code
#PPF90_ext = .f90 #preprocessed Fortran 90 code
#OBJ_ext = .o     #object files


# - Can't use -ip optimization for tools (partit won't compile)
ifeq ($(findstring -ip,$(FFLAGS)),-ip)
    FFLAGS :=$(subst -ip,,$(FFLAGS))
endif
# --------------------------------------------------------------------

ALL = partit ncjoin ncjoin_mpi extract_data_join nc3to4z zslice sample
#r2r_bry r2r_match_topo r2r_init

all:	$(info MAKING ROMS TOOLS: ) mpc $(ALL)

#install: $(ALL)
#	mv $(ALL) ~/bin

# partit
PARTIT = partit insert_node lenstr
PARTIT := $(addsuffix $(OBJ_ext), $(PARTIT))
partit: $(PARTIT)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o partit $(PARTIT) $(LCDF)

# ncjoin
NCJOIN = ncjoin lenstr
NCJOIN := $(addsuffix $(OBJ_ext), $(NCJOIN))
ncjoin: $(NCJOIN)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o ncjoin $(NCJOIN) $(LCDF)
#ncjoin_mpi
NCJOIN_MPI = ncjoin_mod ncjoin_mpi lenstr
NCJOIN_MPI := $(addsuffix $(OBJ_ext), $(NCJOIN_MPI))

ncjoin_mpi: ncjoin_mpi.opt $(NCJOIN_MPI)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o ncjoin_mpi $(NCJOIN_MPI) $(LCDF)

# nc3to4z
NC3TO4Z = nc3to4z lenstr read_string
NC3TO4Z := $(addsuffix $(OBJ_ext),$(NC3TO4Z))
nc3to4z: $(NC3TO4Z)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o nc3to4z $(NC3TO4Z) $(LCDF)

# mpc	
mpc: mpc$(UPF77_ext) Makefile
	$(CPP) $(CPPFLAGS) -P mpc$(UPF77_ext) > ./mpc$(PPF77_ext)
	$(CFT) $(FFLAGS) $(LDFLAGS) -o mpc ./mpc$(PPF77_ext)

# extract_data_join
EXTRACT_DATA_JOIN = ncjoin_mod extract_data_join  lenstr
EXTRACT_DATA_JOIN := $(addsuffix $(OBJ_ext),$(EXTRACT_DATA_JOIN))
extract_data_join: $(EXTRACT_DATA_JOIN)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o extract_data_join $(EXTRACT_DATA_JOIN) $(LCDF)

# particle_join
PARTICLE_JOIN = ncjoin_mod particle_join  lenstr
PARTICLE_JOIN := $(addsuffix $(OBJ_ext), $(PARTICLE_JOIN))
particle_join: 
	$(LDR) $(FFLAGS) $(LDFLAGS) -o particle_join $(PARTICLE_JOIN) $(LCDF)

# r2r_init
R2R_INIT = r2r_init r2r_interp_init r2r_subs r2r_rotate r2r_vert_interp\
           set_depth etch_into_land spln2d tiling roms_read_write\
           read_string lenstr
R2R_INIT ::= $(addsuffix, $(OBJ_ext), $(R2R_INIT))
r2r_init: $(R2R_INIT)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o r2r_init $(R2R_INIT) $(LCDF)

# r2r_bry
R2R_BRY = r2r_bry r2r_bry_interp r2r_subs r2r_bry_rotate r2r_rotate\
           r2r_vert_interp set_depth etch_into_land spln2d tiling\
           def_bry_file roms_read_write read_string lenstr
R2R_BRY := $(addsuffix $(OBJ_ext),$(R2R_BRY))
r2r_bry: $(R2R_BRY)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o r2r_bry $(R2R_BRY) $(LCDF)

# zslice
ZSLICE = zslice set_depth sigma_to_z_intr read_add_write\
	  roms_read_write read_string lenstr tiling
ZSLICE := $(addsuffix $(OBJ_ext),$(ZSLICE))
zslice: $(ZSLICE)
	$(LDR) $(FFLAGS) -o zslice $(ZSLICE) $(LCDF)

# ipslice
IPSLICE = ipslice set_depth sigma_to_z_intr rho_eos\
	   read_add_write roms_read_write read_string lenstr tiling
IPSLICE := $(addsuffix $(OBJ_ext), $(IPSLICE))
ipslice: $(IPSLICE)
	$(LDR) $(FFLAGS) -o ipslice $(IPSLICE) $(LCDF)
ipslice$(PPF77_ext): zslice$(UPF77_ext)
	$(CPP) -P $(CPPFLAGS) -DIPSLICE zslice$(UPF77_ext) | ./mpc > ipslice$(PPF77_ext)

# r2r_match_topo
R2R_MATCH_TOPO = r2r_match_topo r2r_interp_init mrg_zone_subs tiling\
                        spln2d roms_read_write read_string lenstr
R2R_MATCH_TOPO := $(addsuffix $(OBJ_ext),$(R2R_MATCH_TOPO))
r2r_match_topo: $(R2R_MATCH_TOPO)
	$(LDR) $(FFLAGS) $(LDFLAGS) -o r2r_match_topo $(R2R_MATCH_TOPO) $(LCDF)                                  

# sample
SAMPLE = roms_read_write sample tiling\
          read_add_write read_string lenstr
SAMPLE := $(addsuffix $(OBJ_ext), $(SAMPLE))
sample: $(SAMPLE)
	$(LDR) $(FFLAGS) -o sample $(SAMPLE) $(LCDF)

# clean
clean:
	/bin/rm -f *$(PPF77_ext) *$(OBJ_ext) *.a *.s *.trace *.mod
# clobber
clobber: clean
	/bin/rm -f $(ALL) *.bak

# Note: the following .f: dependencies are needed to make everything
# go through CPP and take custom defined CPPFLAGS rather than compiling
# executables from .F directly.

chkindent: chkindent$(PPF77_ext)

# The following two are built from the same source
# and differ only CPP macro setting:

mreplace$(PPF77_ext): mreplace$(UPF77_ext)
	$(CPP) -P $(CPPFLAGS) -DREPLACE mreplace$(UPF77_ext) > mreplace$(PPF77_ext)

include Make.depend
