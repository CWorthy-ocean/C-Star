FROM cstar-prebuilt-ifx:latest AS compiler

ARG INSTALL_BASE_DIR=/opt
ARG ROMS_ROOT=$INSTALL_BASE_DIR/ucla-roms 
ARG ROMS_PATH=$ROMS_ROOT/Tools-Roms/

# ENV PATH=$PATH:$ROMS_ROOT/Tools-Roms/

WORKDIR $INSTALL_BASE_DIR

COPY --from=compiler $INSTALL_BASE_DIR $INSTALL_BASE_DIR

# Install ROMS
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && wget https://github.com/dafyddstephenson/ucla-roms/archive/refs/heads/update_makefiles.tar.gz \ 
    && tar xvf update_makefiles.tar.gz \
    && mv ucla-roms-update_makefiles $ROMS_ROOT  \
    && cd $ROMS_ROOT/Work \
    && make nhmg COMPILER=intel \
    && cd $ROMS_ROOT/Tools-Roms  \
    && make COMPILER=intel"
