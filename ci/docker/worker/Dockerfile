FROM docker.io/continuumio/miniconda3:latest

ARG CSTAR_REPO="https://github.com/ankona/C-Star"
ARG CSTAR_HASH="CW-799"

ENV CSTAR_INTERACTIVE=0
ENV GIT_DISCOVERY_ACROSS_FILESYSTEM=1

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install software-properties-common wget git-all build-essential rsync neovim -y

WORKDIR "${HOME}"
RUN mkdir -p "${HOME}/app" "${HOME}/source"

RUN echo "bar"
RUN git clone $CSTAR_REPO "${HOME}/source/cstar" && \
    git -C "${HOME}/source/cstar" checkout $CSTAR_HASH

RUN conda init --all && \
    conda env create --name cstar --file "${HOME}/source/cstar/ci/environment.yml" && \
    echo "conda activate cstar" >> "${HOME}/.profile" && \
    conda env create --name cstar --file ~/source/cstar/ci/environment.yml && \
    echo "conda activate cstar" >> ~/.profile && \
    conda clean -afy

RUN pip install ~/source/cstar

WORKDIR "${HOME}/app"
COPY run.sh ./run.sh

CMD ["--log-level", "DEBUG", "--blueprint-uri", "/blueprints/blueprint.yml", "--output-dir", "/output/worker"]
ENTRYPOINT ["bash", "./run.sh"]

