FROM cstar-ifx-pb:1 AS compiler

ARG INSTALL_BASE_DIR=/opt
ARG ROMS_ROOT=$INSTALL_BASE_DIR/ucla-roms 
ARG ROMS_PATH=$ROMS_ROOT/Tools-Roms/
ARG REPO_ROMS="https://github.com/dafyddstephenson/ucla-roms/archive/refs/heads/update_makefiles.tar.gz"

ENV PATH=$PATH:$ROMS_PATH
WORKDIR $INSTALL_BASE_DIR

# Install and configure ROMS
RUN bash -c "source /opt/intel/oneapi/setvars.sh && \
    wget $REPO_ROMS && \
    tar xvf update_makefiles.tar.gz && \
    mv ucla-roms-update_makefiles $ROMS_ROOT && \
    cd $ROMS_ROOT/Work && \
    make nhmg COMPILER=intel && \
    cd $ROMS_PATH && \
    make COMPILER=intel"
