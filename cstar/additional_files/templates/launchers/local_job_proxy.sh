#!/bin/sh
SENTINEL_PATH="{sentinel_path}"
BLUEPRINT_PATH="{blueprint_path}"

{env_vars}

# values from `Status` enum
RUNNING={running}
DONE={done}
FAILED={failed}

update_status() {{
    if [ "$(uname)" = "Darwin" ]; then
        sed -i '' "s/^status:.*$/status: $1/" "$2"
    else
        sed -i "s/^status:.*$/status: $1/" "$2"
    fi
}}

# wait for each dependency to complete, then verify that it succeeded --
# a dependency that exited without reaching `Done` must abort this step.
# POSIX sh has no arrays: the sentinel paths sit in the positional
# parameters and are consumed in step with the pid list.
set -- {dep_sentinels}
for DEP_PID in {pids}; do
    DEP_SENTINEL=$1
    shift
    while kill -0 "$DEP_PID" 2>/dev/null; do
        echo "Awaiting process $DEP_PID"
        sleep {delay}
    done

    DEP_STATUS=$(sed -n 's/^status: *//p' "$DEP_SENTINEL" 2>/dev/null)
    if [ "$DEP_STATUS" != "$DONE" ]; then
        echo "Dependency (pid $DEP_PID) ended with status '$DEP_STATUS'; aborting."
        update_status $FAILED $SENTINEL_PATH
        exit 1
    fi
done

# update status to running
update_status $RUNNING $SENTINEL_PATH

# run the target command
{command} &
JOB_PID=$!

wait $JOB_PID

# update the status to `Done` if target command is successful, otherwise `Failed`
RC=$?
STATUS=$FAILED
if [ $RC -eq 0 ]; then
    STATUS=$DONE
fi
update_status $STATUS $SENTINEL_PATH
exit $RC
