#!/bin/bash

# Generate a random wait time between 1 and 10 seconds
wait_time=$((RANDOM % 1 + 1))

# Wait for the generated time
sleep $wait_time

# Print a message indicating the wait is over
echo "ARQUIVO GERADO PELO APP" >> $AZ_BATCH_TASK_WORKING_DIR/output.txt