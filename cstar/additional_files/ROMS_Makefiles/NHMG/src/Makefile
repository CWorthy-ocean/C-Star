# This line de-activates all implicit rules
.SUFFIXES:

# Set machine dependent definitions and rules.
include ../../src/Makedefs.inc

# Library paths:
VPATH = ${NETCDFHOME}/include:${ROMS_ROOT}/NHMG/include
CPATH = ${MPIHOME}/include:${NETCDFHOME}/include:${ROMS_ROOT}/NHMG/include

# Build list of source files (typically most .f90 files)
SRCS = $(shell ls *$(PPF90_ext))

# Exclude some files from list
EXCL = mg_diagnostics mg_netcdf_out_false
EXCL := $(addsuffix $(PPF90_ext), $(EXCL))
SRCS := $(filter-out $(EXCL), $(SRCS))

# Object list is source list with modified file extension
OBJS = $(SRCS:$(PPF90_ext)=$(OBJ_ext))

libnhmg =  ../lib/libnhmg.a


.PHONY: all clean depend

all: $(libnhmg)

$(libnhmg): ${libnhmg}(${OBJS})
	cp *.mod ../include/

depend depend.mk:
	makedepf90 -free $(SRCS} > depend.mk

clean:
	rm -f ${libnhmg} ${OBJS} *.mod

include depend.mk
