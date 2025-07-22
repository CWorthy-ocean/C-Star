ARG IMG_BUILDBASE="ankona/cstar-buildbase:latest"
ARG MAIN_STAGE="main"  # pass `add_test` to build the test program /runtest

FROM $IMG_BUILDBASE as main

ARG OPENMPI_URI="https://download.open-mpi.org/release/open-mpi/v5.0/openmpi-5.0.7.tar.gz"
ARG OMPI_TGZ_NAME="openmpi-5.0.7.tar.gz" 
ARG OMPI_TGZ_OUT_DIR="openmpi-5.0.7"

ARG SRC_DIR="/install"
ARG INSTALL_BASE_DIR="/opt"
ENV MPIHOME="$INSTALL_BASE_DIR/$OMPI_TGZ_OUT_DIR"
ARG MPI_PROFILE="/etc/profile.d/ompi-5.0.7.sh"

RUN mkdir -p "$SRC_DIR"

WORKDIR "$SRC_DIR"
RUN wget -q "$OPENMPI_URI" && \
    tar xvf "$OMPI_TGZ_NAME" && \
    cd "$OMPI_TGZ_OUT_DIR" && \
    ./configure \
        --prefix="$MPIHOME" --with-pic \
        --with-slurm \
        --with-pmix=internal && \
    make -j $(nproc) && make install && cd "$SRC_DIR" && \
    rm "$OMPI_TGZ_NAME" && \
    rm -rf "$OMPI_TGZ_OUT_DIR" && \
    ldconfig

ENV PATH="$MPIHOME/bin:$PATH"
ENV LD_LIBRARY_PATH="$MPIHOME/lib:$LD_LIBRARY_PATH"
ENV CPATH="$MPIHOME/include:$CPATH"
ENV LIBRARY_PATH="$MPIHOME/lib:$LIBRARY_PATH"
ENV LD_RUN_PATH="$MPIHOME/lib:$LD_RUN_PATH"

RUN mkdir -p "/etc/profile.d" && \
    echo "export MPIHOME=$MPIHOME" >> $MPI_PROFILE && \
    echo 'export PATH=$MPIHOME/bin:$PATH' >> $MPI_PROFILE && \
    echo 'export LD_LIBRARY_PATH=$MPIHOME/lib:$LD_LIBRARY_PATH' >> $MPI_PROFILE && \
    echo 'export CPATH=$MPIHOME/include:$CPATH' >> $MPI_PROFILE && \
    echo 'export LIBRARY_PATH=$MPIHOME/lib:$LIBRARY_PATH' >> $MPI_PROFILE && \
    echo 'export LD_RUN_PATH=$MPIHOME/lib:$LD_RUN_PATH' >> $MPI_PROFILE && \
    echo 'export OMPI_ALLOW_RUN_AS_ROOT=1' >> $MPI_PROFILE && \
    echo 'export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1' >> $MPI_PROFILE


# FROM main as add_test

# ARG TEST_ENV="/etc/profile.d/99-ring.sh"
# ENV RING_URI="https://gist.githubusercontent.com/ankona/42bd3fd28cbeec4da65ed57f0529b8dc/raw/3e0a3b7cac0536a0726a5fb36aceb5db1dba8506/ring.c"

# ENV MPI_TEST_HARNESS="/test/ring"
# ENV PATH="/test:$PATH"
# RUN mkdir -p "/test" && \
#     wget -q "$RING_URI" -O "/test/ring.c" && \
#     mpicc "/test/ring.c" -o $MPI_TEST_HARNESS && \
#     rm "/test/ring.c" && \
#     echo "#!/bin/bash" >> "$TEST_ENV" && \
#     echo "export MPI_TEST_HARNESS=$MPI_TEST_HARNESS" >> "$TEST_ENV" && \
#     printf '#!/bin/bash\nmpirun -n 2 --allow-run-as-root %s\n' "$MPI_TEST_HARNESS" >> "/test/runtest" && \
#     chmod +x "$TEST_ENV" && \
#     chmod +x "/test/runtest" && \
#     apt-get update && \
#     apt-get install -y --no-install-recommends neovim && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*


# FROM $MAIN_STAGE as final

# ARG INSTALL_BASE_DIR

# COPY --from=main /etc/profile.d/*mpi*.sh /etc/profile.d/
# COPY --from=main $INSTALL_BASE_DIR/openmpi*/ $INSTALL_BASE_DIR/
WORKDIR $INSTALL_BASE_DIR

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD []
ENTRYPOINT ["/entrypoint.sh"]
