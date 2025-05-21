FROM docker.io/continuumio/miniconda3:latest

ARG CSTAR_REPO="https://github.com/ankona/C-Star"
ARG CSTAR_HASH="CW-799"

ENV CSTAR_INTERACTIVE="0"
ENV GIT_DISCOVERY_ACROSS_FILESYSTEM="1"

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install software-properties-common wget git-all build-essential rsync neovim -y

WORKDIR "${HOME}"
RUN mkdir -p "${HOME}/app" "${HOME}/source"

RUN git clone $CSTAR_REPO "${HOME}/source/cstar" && \
    git -C "${HOME}/source/cstar" checkout $CSTAR_HASH

RUN conda init --all
RUN echo "conda activate base" >> "${HOME}/.profile"
RUN conda env update --name base --file "${HOME}/source/cstar/ci/environment.yml"
# RUN conda install -y gdal
RUN conda clean -afy
## needed for roms-tools when building image on macos...

RUN pip install --no-cache-dir ~/source/cstar && \
    rm -rf "${HOME}/source/cstar"

WORKDIR "${HOME}/app"
COPY run.sh ./run.sh

CMD ["--log-level", "DEBUG", "--blueprint-uri", "/blueprints/blueprint.yml", "--output-dir", "/output/worker"]
ENTRYPOINT ["bash", "./run.sh"]

