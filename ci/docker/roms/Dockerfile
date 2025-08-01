ARG IMG_NCDF="ankona/cstar-netcdf:latest"

FROM $IMG_NCDF


ARG SRC_DIR="/install"
ARG INSTALL_BASE_DIR=/opt
ENV ROMS_ROOT=$INSTALL_BASE_DIR/ucla-roms 
ENV ROMS_PATH=$ROMS_ROOT/Tools-Roms/
ARG REPO_ROMS="https://github.com/dafyddstephenson/ucla-roms/archive/refs/heads/update_makefiles.tar.gz"
ENV ROMS_PROFILE="/etc/profile.d/ucla-roms.sh"

WORKDIR "$SRC_DIR"

RUN echo "export ROMS_ROOT=$ROMS_ROOT" >> $ROMS_PROFILE && \
    echo "export ROMS_PATH=$ROMS_PATH" >> $ROMS_PROFILE && \
    echo 'export PATH=$PATH:$ROMS_PATH' >> $ROMS_PROFILE

RUN . /etc/profile && \
    wget -q "$REPO_ROMS" && \
    tar xvf update_makefiles.tar.gz && \
    mv ucla-roms-update_makefiles "$ROMS_ROOT" && \
    rm update_makefiles.tar.gz

RUN . /etc/profile && \
    cd "$ROMS_ROOT/Work" && \
    make nhmg COMPILER=gnu

RUN . /etc/profile && \
    cd $ROMS_PATH && \
    make COMPILER=gnu

WORKDIR $INSTALL_BASE_DIR

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD []
ENTRYPOINT ["/entrypoint.sh"]

