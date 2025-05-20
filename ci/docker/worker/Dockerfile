FROM docker.io/continuumio/miniconda3:latest

ARG CSTAR_REPO="https://github.com/ankona/C-Star"
ARG CSTAR_HASH="CW-799"
ARG USER_HOME="/root"

ENV CSTAR_INTERACTIVE=0
ENV GIT_DISCOVERY_ACROSS_FILESYSTEM=1

RUN apt-get update -y && \
    apt-get upgrade -y
RUN apt-get install software-properties-common wget git-all build-essential rsync neovim -y

RUN groupadd -r cstar-group && \
    useradd -r -g cstar-group cstar-user

RUN mkdir -p "${USER_HOME}"
# RUN mkdir -p /home/cstar-user && \
#     chown cstar-user /home/cstar-user
# USER cstar-user
# WORKDIR /home/cstar-user

WORKDIR $USER_HOME
RUN mkdir -p app config source data

# VOLUME ./data

RUN git clone $CSTAR_REPO "${USER_HOME}/source/c-star" && \
    git -C "${USER_HOME}/source/c-star" checkout $CSTAR_HASH

# USER cstar-user
# RUN mkdir -p logs
# VOLUME logs


RUN conda init --all && \
    conda env create --name cstar --file "${USER_HOME}/source/c-star/ci/environment.yml" && \
    echo "conda activate cstar" >> "${USER_HOME}/.bashrc" && \
    conda clean -afy

RUN . "${USER_HOME}/.bashrc" && \
    # conda init --all && \
    # conda activate cstar && \
    # pip uninstall cstar-ocean && \
    pip install "${USER_HOME}/source/c-star"

# RUN cp "${USER_HOME}/.bashrc" "${USER_HOME}/.bash_profile"
# RUN conda init --all && \
#     . ~/.bashrc && \
#     conda activate base && \
#     pip install ~/source/c-star

ENV BASH_ENV="${USER_HOME}/.bashrc"
WORKDIR "${USER_HOME}/app"
COPY run.sh ./run.sh
RUN chmod +x ./run.sh


# CMD ["python", "-m", "cstar.scripts.runner.worker", "--log-level", "DEBUG", "--blueprint-uri", "/blueprints/blueprint.yml", "--output-dir", "/output/workeroutput"]
CMD ["--log-level", "DEBUG", "--blueprint-uri", "/blueprints/blueprint.yml", "--output-dir", "/output/workeroutput"]
ENTRYPOINT ["bash", "./run.sh"]

