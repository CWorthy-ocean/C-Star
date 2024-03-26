#!/bin/bash
set -e
rm -rvf "$(pwd)/externals/MARBL/"
rm -rvf "$(pwd)/externals/ucla-roms/"

conda env remove --prefix="$(pwd)/conda_envs/cstar_gnu"


