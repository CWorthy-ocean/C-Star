#!/bin/bash
SENTINEL_PATH="{sentinel_path}"
BLUEPRINT_PATH="{blueprint_path}"
DEP_PIDS=({pids})
DEP_SENTINELS=({dep_sentinels})

{env_vars}

# values from `Status` enum
RUNNING={running}
DONE={done}
FAILED={failed}

update_status() {{
    local status=$1
    if [ "$(uname)" = "Darwin" ]; then
        sed -i '' "s/^status:.*$/status: $status/" "$2"
    else
        sed -i "s/^status:.*$/status: $status/" "$2"
    fi
}}

# wait for each dependency to complete, then verify that it succeeded --
# a dependency that exited without reaching `Done` must abort this step.
for i in "${{!DEP_PIDS[@]}}"; do
    DEP_PID="${{DEP_PIDS[$i]}}"
    while kill -0 "$DEP_PID" 2>/dev/null; do
        echo "Awaiting process $DEP_PID"
        sleep {delay}
    done

    DEP_SENTINEL="${{DEP_SENTINELS[$i]}}"
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
