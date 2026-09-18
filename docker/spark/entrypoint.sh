#!/bin/bash
set -eo pipefail

if [ "$1" != "executor" ]; then
  exec "$@"
fi

mapfile -t java_opts < <(env | grep '^SPARK_JAVA_OPT_' | sort -t_ -k4 -n | sed 's/^[^=]*=//')

exec "${JAVA_HOME}/bin/java" \
  "${java_opts[@]}" \
  -Xms"${SPARK_EXECUTOR_MEMORY}" \
  -Xmx"${SPARK_EXECUTOR_MEMORY}" \
  -cp "${SPARK_CONF_DIR}:${SPARK_HOME}/jars/*${SPARK_CLASSPATH:+:$SPARK_CLASSPATH}" \
  org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend \
  --driver-url "${SPARK_DRIVER_URL}" \
  --executor-id "${SPARK_EXECUTOR_ID}" \
  --cores "${SPARK_EXECUTOR_CORES}" \
  --app-id "${SPARK_APPLICATION_ID}" \
  --hostname "${SPARK_EXECUTOR_POD_IP}" \
  --resourceProfileId "${SPARK_RESOURCE_PROFILE_ID}" \
  --podName "${SPARK_EXECUTOR_POD_NAME}"
