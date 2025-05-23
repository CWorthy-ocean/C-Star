FROM docker.io/linuxcontainers/debian-slim:latest

ARG MINICONDA_SCRIPT="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" 
ENV CONDA_HOME="/miniconda3"

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install software-properties-common wget -y

RUN mkdir -p $CONDA_HOME
ENV PATH="${CONDA_HOME}/bin:${PATH}"

WORKDIR $CONDA_HOME
RUN wget $MINICONDA_SCRIPT -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -u -p $CONDA_HOME && \
    $CONDA_HOME/bin/conda init --all && \
    rm /tmp/miniconda.sh

CMD ["conda", "--help"]

