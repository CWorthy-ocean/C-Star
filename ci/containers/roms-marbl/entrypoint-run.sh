#!/bin/bash
source /etc/profile
exec "$@"
# cd /work/ROMS/compile_time_code
# make compile_clean
# make
# cd /work
# mpirun /work/ROMS/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in
# whoami
# mkdir -p /tmp/work_internal/compile_time_code
# cp -r /work/ROMS/compile_time_code /tmp/work_internal
# cd /tmp/work_internal/compile_time_code
# make compile_clean
# make
# echo "done with build"
# cd /work
# unset SLURM_JOBID
# env
# mpirun /tmp/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in
mkdir -p /opt/work_internal/compile_time_code
cp -r /work/ROMS/compile_time_code /opt/work_internal
cd /opt/work_internal/compile_time_code
make compile_clean
make
echo "done with build"
# cd /work
# unset SLURM_JOBID
# env
# mpirun /tmp/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in