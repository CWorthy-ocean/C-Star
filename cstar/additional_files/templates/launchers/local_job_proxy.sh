#!/bin/bash
SENTINEL_PATH="{sentinel_path}"
BLUEPRINT_PATH="{blueprint_path}"
DEP_PIDS=({pids})

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

# wait for dependencies to complete.
for DEP_PID in "${{DEP_PIDS[@]}}"; do
    while kill -0 "$DEP_PID" 2>/dev/null; do
        echo "Awaiting process $DEP_PID"
        sleep {delay}
    done
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
