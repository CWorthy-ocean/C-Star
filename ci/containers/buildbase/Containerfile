FROM ubuntu:24.04 as main

ENV TERM="xterm-256color"

RUN apt-get update && \
    apt-get install -y \
        binutils \
        build-essential \
        ca-certificates \
        git-all \
        gfortran \
        libstdc++-14-dev \
        libxml2-dev \
        libcurl4-openssl-dev \
        m4 \
        python3 \
        python3-pip \
        python3-venv \
        python-is-python3 \
        rsync \
        software-properties-common \
	    wget \
        zlib1g-dev \
        --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ldconfig


COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD []
ENTRYPOINT ["/entrypoint.sh"]
