ARG IMG_NCDF="ankona/cstar-netcdf:latest"
ARG IMG_ROMS="ankona/cstar-roms:latest"
ARG IMG_MARBL="ankona/cstar-roms-marbl:latest"
ARG IMG_CONDA="ankona/cstar-condabase:latest"
ARG IMG_BUILDBASE="ankona/cstar-buildbase:latest"

ARG INSTALL_BASE_DIR="/opt"
ARG PROFILE_DIR="/etc/profile.d"


FROM $IMG_NCDF as stage_netcdf


FROM $IMG_ROMS as stage_roms


FROM $IMG_MARBL as stage_marbl


FROM $IMG_CONDA as stage_conda


FROM $IMG_BUILDBASE as final


ARG PROFILE_DIR
ARG INSTALL_BASE_DIR

ENV CSTAR_INTERACTIVE="0"
ENV GIT_DISCOVERY_ACROSS_FILESYSTEM="1"

COPY --from=stage_netcdf $PROFILE_DIR/*netcdf*.sh $PROFILE_DIR/*mpi*.sh $PROFILE_DIR/*hdf*.sh $PROFILE_DIR/
COPY --from=stage_roms $PROFILE_DIR/*roms*.sh $PROFILE_DIR/
COPY --from=stage_marbl $PROFILE_DIR/*marbl*.sh $PROFILE_DIR/
COPY --from=stage_conda $PROFILE_DIR/*conda*.sh $PROFILE_DIR/

COPY --from=stage_netcdf $INSTALL_BASE_DIR/openmpi-5.0.7/ $INSTALL_BASE_DIR/openmpi-5.0.7/
COPY --from=stage_netcdf $INSTALL_BASE_DIR/hdf5-1.14.6-install/ $INSTALL_BASE_DIR/hdf5-1.14.6-install/
COPY --from=stage_netcdf $INSTALL_BASE_DIR/netcdf-4.9.3-install/ $INSTALL_BASE_DIR/netcdf-4.9.3-install/
COPY --from=stage_netcdf $INSTALL_BASE_DIR/netcdf-fortran-4.6.2-install/ $INSTALL_BASE_DIR/netcdf-fortran-4.6.2-install/

COPY --from=stage_roms $INSTALL_BASE_DIR/ucla-roms/ $INSTALL_BASE_DIR/ucla-roms/
COPY --from=stage_marbl $INSTALL_BASE_DIR/marbl0.45.0/ $INSTALL_BASE_DIR/marbl0.45.0/
COPY --from=stage_conda $INSTALL_BASE_DIR/miniconda3/ $INSTALL_BASE_DIR/miniconda3/

WORKDIR $INSTALL_BASE_DIR

COPY blueprint.yml ./blueprint.yml

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD []
ENTRYPOINT ["/entrypoint.sh"]

