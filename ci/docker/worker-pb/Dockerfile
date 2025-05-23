ARG INSTALL_BASE_DIR="/opt"
ARG ROMS_ROOT=$INSTALL_BASE_DIR/ucla-roms
ARG MARBL_ROOT=$INSTALL_BASE_DIR/MARBL
ARG ROMS_PATH=$ROMS_ROOT/Tools-Roms/
ARG REPO_ROMS="https://github.com/dafyddstephenson/ucla-roms/archive/refs/heads/update_makefiles.tar.gz"
ARG REPO_MARBL="https://github.com/marbl-ecosys/MARBL/archive/refs/tags/marbl0.45.0.tar.gz"
ARG REPO_CSTAR="https://github.com/ankona/C-Star"
ARG REPO_CSTAR_HASH="CW-799"
ARG CSTAR_LOG_LEVEL="DEBUG"

FROM cstar-ifx-pb:1 as stage_roms
ARG INSTALL_BASE_DIR
ARG ROMS_ROOT
ARG ROMS_PATH
ARG REPO_ROMS

ENV ROMS_ROOT=$ROMS_ROOT
ENV PATH=$PATH:$ROMS_PATH

WORKDIR $INSTALL_BASE_DIR
RUN bash -c "source /opt/intel/oneapi/setvars.sh && \
    wget $REPO_ROMS && \
    tar xvf update_makefiles.tar.gz && \
    mv ucla-roms-update_makefiles $ROMS_ROOT && \
    cd $ROMS_ROOT/Work && \
    make nhmg COMPILER=intel && \
    cd $ROMS_PATH && \
    make COMPILER=intel"

FROM cstar-ifx-pb:1 as stage_marbl
ARG INSTALL_BASE_DIR
ARG ROMS_ROOT
ARG ROMS_PATH
ARG MARBL_ROOT
ARG REPO_MARBL

ENV ROMS_ROOT=$ROMS_ROOT
ENV MARBL_ROOT=$MARBL_ROOT
ENV PATH=$PATH:$ROMS_PATH

COPY --from=stage_roms $ROMS_ROOT $ROMS_ROOT

# (Makefile doesn't define ifx option but setting I_MPI_F90 to ifx above should fix this)
WORKDIR $INSTALL_BASE_DIR
RUN bash -c "source /opt/intel/oneapi/setvars.sh && \
    wget $REPO_MARBL && \
    tar xvf marbl0.45.0.tar.gz && \
    mv MARBL-marbl0.45.0 $MARBL_ROOT && \
    cd $MARBL_ROOT/src && \
    make intel USEMPI=TRUE"


FROM continuumio/miniconda3@sha256:14513208bcb22341302f7886482391f337f2532477e2f7ffee48c06353b2e96e as stage_conda
ARG ROMS_ROOT
ARG ROMS_PATH
ARG MARBL_ROOT
ARG REPO_CSTAR
ARG REPO_CSTAR_HASH
ARG CSTAR_LOG_LEVEL

ENV ROMS_ROOT=$ROMS_ROOT
ENV MARBL_ROOT=$MARBL_ROOT
ENV PATH=$PATH:$ROMS_PATH
ENV CSTAR_INTERACTIVE="0"
ENV GIT_DISCOVERY_ACROSS_FILESYSTEM="1"
ENV CSTAR_ROMS_PREBUILT="1"
ENV CSTAR_MARBL_PREBUILT="1"
ENV REPO_CSTAR_PATH="${HOME}/source/cstar"

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        software-properties-common wget git-all build-essential rsync neovim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=stage_roms $ROMS_ROOT $ROMS_ROOT
COPY --from=stage_marbl $MARBL_ROOT $MARBL_ROOT

RUN mkdir -p "${HOME}/app" "${HOME}/source"
WORKDIR "${HOME}"

RUN bash -c "conda init --all && \
    git clone $REPO_CSTAR $REPO_CSTAR_PATH && \
    git -C $REPO_CSTAR_PATH checkout $REPO_CSTAR_HASH && \
    echo \"conda activate base\" >> ${HOME}/.profile && \
    conda env update --name base --file $REPO_CSTAR_PATH/ci/environment.yml && \
    pip install --no-cache-dir $REPO_CSTAR_PATH && \
    rm -rf $REPO_CSTAR_PATH && \
    conda clean -afy"

# RUN conda install -y gdal
## needed for roms-tools when building image on macos...

WORKDIR "${HOME}/app"
COPY run.sh ./run.sh

CMD ["--log-level", "$CSTAR_LOG_LEVEL"]
ENTRYPOINT ["bash", "./run.sh", "--output-dir", "/output/worker", "--blueprint-uri", "/blueprints/blueprint.yml"]

