FROM cstar-prebuilt-roms:latest AS roms_prebuilt

ENV INSTALL_BASE_DIR=/opt
ENV MARBL_ROOT=$INSTALL_BASE_DIR/MARBL

WORKDIR $INSTALL_BASE_DIR

COPY --from=roms_prebuilt $INSTALL_BASE_DIR $INSTALL_BASE_DIR

# Install MARBL
# (Makefile doesn't define ifx option but setting I_MPI_F90 to ifx above should fix this)
WORKDIR $INSTALL_BASE_DIR
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && wget https://github.com/marbl-ecosys/MARBL/archive/refs/tags/marbl0.45.0.tar.gz \ 
    && tar xvf marbl0.45.0.tar.gz \
    && mv MARBL-marbl0.45.0 $MARBL_ROOT  \
    && cd $MARBL_ROOT/src \
    && make intel USEMPI=TRUE"
