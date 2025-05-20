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

# RUN mkdir -p /home/root 
# RUN mkdir -p /home/cstar-user && \
#     chown cstar-user /home/cstar-user
# USER cstar-user
# WORKDIR /home/cstar-user

WORKDIR $USER_HOME
RUN mkdir -p app config source data

VOLUME ./data

RUN git clone $CSTAR_REPO "${USER_HOME}/source/c-star" && \
    git -C "${USER_HOME}/source/c-star" checkout $CSTAR_HASH

# USER root
# RUN conda init --all && \
#     . /opt/conda/bin/activate && \
#     conda env update --name base --file /home/cstar-user/source/c-star/ci/environment.yml && \
#     conda clean -afy

# USER cstar-user
# RUN mkdir -p logs
# VOLUME logs

# COPY entrypoint.py ~/app/entrypoint.py
# COPY mpitest.py ~/app/mpitest.py
# COPY environment.yml ~/config/environment.yml

# RUN . ~/.bashrc && \
#     pip install ~/source/c-star

RUN conda init --all && \
    conda env create --name cstar --file "${USER_HOME}/source/c-star/ci/environment.yml" && \
    echo "conda activate cstar" >> "${USER_HOME}/.bashrc" && \
    conda clean -afy

#RUN . /home/root/.bashrc && \
RUN . "${USER_HOME}/.bashrc" && \
    # conda init --all && \
    # conda activate cstar && \
    pip uninstall cstar-ocean && \
    pip install "${USER_HOME}/source/c-star"

# RUN conda init --all && \
#     . ~/.bashrc && \
#     conda activate base && \
#     pip install ~/source/c-star

WORKDIR "${USER_HOME}/app"
# RUN ls -la && pwd
# COPY entrypoint.py ./entrypoint.py


# CMD [".", "${USER_HOME}/.bashrc", "&&", "which", "python"]
CMD ["echo", "'sup"]
# CMD ["--log-level", "DEBUG", "--blueprint-uri", "/blueprints/blueprint.yml", "--output-dir", "/output"]
# ENTRYPOINT ["source", "~/.bashrc", "&&", "conda", "activate", "cstar", "&&" "python", "-m", "cstar.scripts.runner.worker"]
# ENTRYPOINT ["conda", "activate", "cstar", "&&" "python", "-m", "cstar.scripts.runner.worker"]
# ENTRYPOINT ["python", "-m", "cstar.scripts.runner.worker"]
# ENTRYPOINT ["/usr/bin/bash"]


