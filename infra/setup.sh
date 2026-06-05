#!/bin/bash
set -e

RAPIDS_VERSION="24.08.0"
JAVA_VERSION="17"
SPARK_VERSION="3.5.1"
HADOOP_VERSION="3"

SPARK_HOME="$HOME/spark"
APP_HOME="$HOME/app"

echo "=== Install dependencies ==="
sudo apt-get update
sudo apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    python-is-python3 \
    openjdk-${JAVA_VERSION}-jre-headless \
    curl \
    wget \
    vim \
    sudo \
    whois \
    ca-certificates-java \
    procps

SPARK_DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

if [ ! -d "$SPARK_HOME" ]; then
    wget --verbose -O /tmp/apache-spark.tgz "${SPARK_DOWNLOAD_URL}"
    mkdir -p "$SPARK_HOME"
    tar -xf /tmp/apache-spark.tgz -C "$SPARK_HOME" --strip-components=1
    rm /tmp/apache-spark.tgz
else
    echo "There already is $SPARK_HOME"
fi

echo "===DOWNLAODING JARs ==="
mkdir -p "$SPARK_HOME/jars"
wget -O "$SPARK_HOME/jars/rapids-4-spark_2.12-${RAPIDS_VERSION}.jar" "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/${RAPIDS_VERSION}/rapids-4-spark_2.12-${RAPIDS_VERSION}.jar"

echo "=== Configuring Spark ==="
mkdir -p "$SPARK_HOME/logs" "$SPARK_HOME/event_logs"
chmod -R 0777 "$SPARK_HOME/event_logs" "$SPARK_HOME/logs"

cat <<EOF > "$SPARK_HOME/conf/spark-defaults.conf"
spark.eventLog.enabled true
spark.eventLog.dir file://${SPARK_HOME}/event_logs
spark.history.fs.logDirectory file://${SPARK_HOME}/event_logs
spark.plugins com.nvidia.spark.SQLPlugin
spark.sql.execution.arrow.pyspark.enabled true
EOF
echo "=== Configuring Python in $APP_HOME ==="
mkdir -p "$APP_HOME"
cd "$APP_HOME"

# Tworzenie wirtualnego środowiska, jeśli nie istnieje
if [ ! -d ".venv" ]; then
    echo "Tworzenie środowiska wirtualnego..."
    python3 -m venv .venv
fi

source .venv/bin/activate

pip install --upgrade pip

if [ -f "requirements.txt" ]; then
    echo "isntalling requirements.txt..."
    pip install -r requirements.txt

    pip install ipykernel

    python -m ipykernel install --user --name=spark-env --display-name "Python (Spark Project)"
else
    echo "WArning: no requirements.txt in $APP_HOME"
fi

echo "=== Updating ~/.bashrc ==="
BASHRC="$HOME/.bashrc"

grep -q "SPARK_HOME" "$BASHRC" || echo 'export SPARK_HOME="$HOME/spark"' >> "$BASHRC"
grep -q "JAVA_HOME" "$BASHRC" || echo 'export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"' >> "$BASHRC"
grep -q "PYTHONPATH" "$BASHRC" || echo 'export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"' >> "$BASHRC"

grep -q "SPARK_HOME/bin" "$BASHRC" || echo 'export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/python:${JAVA_HOME}/bin:$HOME/app/.venv/bin:$PATH"' >> "$BASHRC"

