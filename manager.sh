#!/bin/bash

VENV_DIR="venv"
SERVER_SCRIPT="server.py"
GENERATOR_SCRIPT="generator.py"
PID_DIR="pids"

# Create PID directory if it doesn't exist
mkdir -p $PID_DIR

# Function to initialize the project
init() {
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating virtual environment..."
        python3 -m venv $VENV_DIR
    fi

    echo "Activating virtual environment..."
    source $VENV_DIR/bin/activate

    echo "Installing requirements..."
    pip install -r requirements.txt

    echo "Initialization completed."
}

start_server() {
    echo "Starting server..."
    python $SERVER_SCRIPT > server.log 2>&1 &
    echo $! > $PID_DIR/server.pid
    # Wait for server to be ready
    sleep 2
}

start_generator() {
    echo "Starting generator..."
    python $GENERATOR_SCRIPT > generator.log 2>&1 &
    echo $! > $PID_DIR/generator.pid
}

stop_process() {
    local pid_file="$PID_DIR/$1.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        echo "Stopping $1 (PID: $pid)..."
        kill $pid 2>/dev/null || true
        rm "$pid_file"
    fi
}

status() {
    echo "Checking services status..."
    for service in "server" "generator"; do
        if [ -f "$PID_DIR/$service.pid" ]; then
            pid=$(cat "$PID_DIR/$service.pid")
            if ps -p $pid > /dev/null; then
                echo "$service is running (PID: $pid)"
            else
                echo "$service is not running (stale PID file)"
                rm "$PID_DIR/$service.pid"
            fi
        else
            echo "$service is not running"
        fi
    done
}

# Function to start the Python script
start() {
    if [ ! -d "$VENV_DIR" ]; then
        echo "Virtual environment not found. Please run init command first."
        exit 1
    fi

    echo "Activating virtual environment..."
    source $VENV_DIR/bin/activate

    # Start server first
    start_server
    
    # Wait for server to be ready (5 seconds)
    echo "Waiting for server to initialize..."
    sleep 5
    
    # Check if server is running before starting generator
    if [ -f "$PID_DIR/server.pid" ]; then
        pid=$(cat "$PID_DIR/server.pid")
        if ps -p $pid > /dev/null; then
            echo "Server is running, starting generator..."
            start_generator
        else
            echo "Server failed to start. Please check server.log for details."
            exit 1
        fi
    else
        echo "Server PID file not found. Server may have failed to start."
        exit 1
    fi
    
    echo "All services started. Use './manager.sh status' to check status."
}

# Function to stop the Python script
stop() {
    stop_process "generator"
    stop_process "server"
    echo "All services stopped."
}

# Check the command-line argument
case $1 in
    init)
        init
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    restart)
        stop
        sleep 2
        start
        ;;
    *)
        echo "Usage: $0 {init|start|stop|status|restart}"
        exit 1
esac