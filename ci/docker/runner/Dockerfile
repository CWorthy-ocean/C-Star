ARG IMG_NCDF="ankona/cstar-netcdf:latest"
ARG IMG_ROMS="ankona/cstar-roms:latest"
ARG IMG_MARBL="ankona/cstar-roms-marbl:latest"
ARG IMG_CONDA="ankona/cstar-condabase:latest"
ARG IMG_BUILDBASE="ankona/cstar-buildbase:latest"
# ARG CSTAR_SRC="/source/cstar"
# ARG CSTAR_ENV="$CSTAR_SRC/ci/environment.yml"

# ARG SRC_DIR="/install"
ARG INSTALL_BASE_DIR="/opt"
ARG PROFILE_DIR="/etc/profile.d"

# ARG CSTAR_REPO="https://github.com/ankona/C-Star"
# ARG CSTAR_HASH="CW-799"


FROM $IMG_NCDF as stage_netcdf


FROM $IMG_ROMS as stage_roms


FROM $IMG_MARBL as stage_marbl


FROM $IMG_CONDA as stage_conda


# FROM docker.io/continuumio/miniconda3:latest as final
FROM $IMG_BUILDBASE as final

# ARG CSTAR_SRC
# ARG CSTAR_ENV
# ARG CSTAR_REPO
# ARG CSTAR_HASH
# ARG RUNNER_PROFILE="90-conda.sh"
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

# RUN apt-get update && apt-get install -y --no-install-recommends wget git-all build-essential rsync neovim

# RUN conda init --all && \
#     . "/etc/profile" && \
#     conda activate base && \
#     mkdir -p "$CSTAR_SRC" && \
#     git clone "$CSTAR_REPO" "$CSTAR_SRC" && \
#     git -C "$CSTAR_SRC" checkout "$CSTAR_HASH" && \
#     conda env update --name base --file "$CSTAR_ENV" && \
#     conda clean -afy && \
#     pip install --no-cache-dir "$CSTAR_SRC" && \
#     rm -rf "$CSTAR_SRC"

# CMD ["--log-level", "DEBUG", "--blueprint-uri", "/blueprints/blueprint.yml", "--output-dir", "/output/worker"]
# ENTRYPOINT ["echo", "$SLURM_JOB_ID"]

WORKDIR $INSTALL_BASE_DIR

COPY blueprint.yml ./blueprint.yml

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD []
ENTRYPOINT ["/entrypoint.sh"]

