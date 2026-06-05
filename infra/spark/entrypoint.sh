#!/bin/bash
set -e

APP_HOME="${APP_HOME:-/home/${USER}/app}"
export USER="${USER}"
export LOGNAME="${LOGNAME:-$USER}"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-$USER}"
export HOME="${APP_HOME}/apps"

if ! mkdir -p "$HOME" 2>/dev/null || [ ! -w "$HOME" ]; then
    export HOME="/tmp"
fi

export JUPYTER_CONFIG_DIR="$HOME/.jupyter"
export JUPYTER_DATA_DIR="$HOME/.local/share/jupyter"
export JUPYTER_RUNTIME_DIR="/tmp/jupyter-runtime"
export SPARK_LOCAL_DIRS="/tmp/spark-local"
export SPARK_WORKER_DIR="/tmp/spark-worker"
export SPARK_LOG_DIR="/tmp/spark-logs"
export IVY_HOME="$HOME/.ivy2"

export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} -Duser.name=$USER -Duser.home=$HOME -Divy.home=$IVY_HOME -Divy.cache.dir=$IVY_HOME/cache"
export PYSPARK_PYTHON="${APP_HOME}/.venv/bin/python3"
export PYSPARK_DRIVER_PYTHON="${APP_HOME}/.venv/bin/python3"

mkdir -p "$JUPYTER_CONFIG_DIR" "$JUPYTER_DATA_DIR" "$JUPYTER_RUNTIME_DIR" \
    "$SPARK_LOCAL_DIRS" "$SPARK_WORKER_DIR" "$SPARK_LOG_DIR" "$IVY_HOME" "$IVY_HOME/cache" "$IVY_HOME/local"

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

echo "Starting Spark Master..."
$SPARK_HOME/sbin/start-master.sh --host 0.0.0.0 --port 7077 || true
echo "Starting Spark Worker..."
$SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077 || true
echo "Starting Spark History Server..."
$SPARK_HOME/sbin/start-history-server.sh || true
echo "Starting Jupyter Notebook..."
jupyter notebook --ip=0.0.0.0 --port=4041 --no-browser \
    --NotebookApp.token='' --NotebookApp.password='' \
    --notebook-dir="${APP_HOME}/apps"