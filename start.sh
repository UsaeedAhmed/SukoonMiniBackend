#!/bin/bash

# Apply the patch to fix database path
echo "Applying database path patch..."
python database_manager_patch.py

# Check if database directory exists and is writable
echo "Checking database directory..."
mkdir -p /data
chmod 777 /data
touch /data/smart_home_energy.db
chmod 666 /data/smart_home_energy.db
ls -la /data

# Start the energy calculator in the background
echo "Starting energy calculator scheduler..."
python energy_calculator.py --scheduler --interval 15 &

# Store the PID of the background process
CALCULATOR_PID=$!

# Wait a moment to ensure it starts up properly
sleep 2

# Start the API in the foreground
echo "Starting API server..."
python api_app.py &

API_PID=$!

# Function to forward signals to child processes
forward_signal() {
  echo "Received signal, forwarding to children..."
  kill -TERM $CALCULATOR_PID $API_PID 2>/dev/null
}

# Set up signal handling
trap forward_signal SIGINT SIGTERM

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?