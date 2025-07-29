ARG IMG_BUILDBASE="ankona/cstar-buildbase:latest"

FROM $IMG_BUILDBASE as main

ARG INSTALL_BASE_DIR="/opt"
ARG CSTAR_REPO="cworthy-ocean/c-star"
ARG CSTAR_HASH="develop"
ARG PY_PROFILE="/etc/profile.d/cstar.sh"

ARG CSTAR_HOME="$INSTALL_BASE_DIR/venv"

RUN python -m venv "$CSTAR_HOME" && \
    echo ". $CSTAR_HOME/bin/activate" >> "$PY_PROFILE" && \
    . /etc/profile && \
    git clone -b $CSTAR_HASH --single-branch "https://github.com/$CSTAR_REPO" /tmp/cstar && \
    cd /tmp/cstar && \
    pip install . --no-cache-dir && \
    rm -rf /tmp/cstar

WORKDIR $INSTALL_BASE_DIR

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD []
ENTRYPOINT ["/entrypoint.sh"]
