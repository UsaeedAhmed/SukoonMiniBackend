#!/bin/bash

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