FROM nvidia/cuda:12.2.2-runtime-ubuntu22.04

ENV RAPIDS_VERSION=24.08.0 \
    JAVA_VERSION=17 \
    SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    HADOOP_JAR_VERSION=3.3.4 \
    AWS_SDK_VERSION=1.12.262 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    USERNAME=hostuser \
    USER_UID=1000 \
    USER_GID=1000

ENV APP_HOME=/home/${USERNAME}/app
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/python:${JAVA_HOME}/bin:${PATH}" \
    PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

ARG DEVCONTAINER

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv python-is-python3 \
    python3.10 python3.10-venv python3.10-dev \
    build-essential python3-dev \
    openjdk-${JAVA_VERSION}-jre-headless \
    curl wget vim sudo whois ca-certificates-java procps nvidia-utils-525 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME \
    && echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

RUN mkdir -p ${SPARK_HOME}/jars ${SPARK_HOME}/logs ${SPARK_HOME}/event_logs \
    && wget -qO- "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
       | tar -xz -C ${SPARK_HOME} --strip-components=1 \
    && wget -q -O ${SPARK_HOME}/jars/rapids-4-spark_2.12-${RAPIDS_VERSION}.jar "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/${RAPIDS_VERSION}/rapids-4-spark_2.12-${RAPIDS_VERSION}.jar" \
    && wget -q -O ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_JAR_VERSION}.jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_JAR_VERSION}/hadoop-aws-${HADOOP_JAR_VERSION}.jar" \
    && wget -q -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    && chown -R $USERNAME:$USERNAME ${SPARK_HOME} \
    && chmod -R 0777 ${SPARK_HOME}/event_logs ${SPARK_HOME}/logs

COPY <<EOF ${SPARK_HOME}/conf/spark-defaults.conf
spark.eventLog.enabled true
spark.eventLog.dir file://${SPARK_HOME}/event_logs
spark.history.fs.logDirectory file://${SPARK_HOME}/event_logs
spark.plugins com.nvidia.spark.SQLPlugin
spark.sql.execution.arrow.pyspark.enabled true
spark.pyspark.python /usr/bin/python3.10
spark.pyspark.driver.python /usr/bin/python3.10
EOF

COPY infra/spark/entrypoint.sh ${SPARK_HOME}/entrypoint.sh
COPY --chown=... requirements.txt ./
COPY --chown=... scripts/ ./scripts/
RUN chmod +x ${SPARK_HOME}/entrypoint.sh

USER $USERNAME
WORKDIR $APP_HOME

COPY --chown=$USERNAME:$USERNAME requirements.txt ./
COPY --chown=$USERNAME:$USERNAME scripts/ ./scripts/

ARG AIRFLOW_VERSION=2.9.1
ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN python3 -m venv .venv \
    && .venv/bin/pip install --no-cache-dir --upgrade pip \
    && .venv/bin/pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" \
    && .venv/bin/pip install --no-cache-dir -r requirements.txt \
    && .venv/bin/python -m ipykernel install --prefix=.venv --name=spark-env --display-name "Python (Spark Project)"

ENV PATH="${APP_HOME}/.venv/bin:${PATH}"

EXPOSE 4040 4041 7077 18080 8888
ENTRYPOINT ["/opt/spark/entrypoint.sh"]