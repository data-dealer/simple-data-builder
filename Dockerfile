FROM bitnami/spark:3.3.1
USER root

RUN apt-get update && apt-get install -y curl

ENV SHARED_WORKSPACE=/opt/workspace
ENV SPARK_VERSION=3.3
RUN mkdir -p ${SHARED_WORKSPACE}
RUN mkdir -p ${SHARED_WORKSPACE}/jars

COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY requirements.txt ${SHARED_WORKSPACE}/requirements.txt
COPY requirements_test.txt ${SHARED_WORKSPACE}/requirements_test.txt
# COPY jars/deequ-2.0.3-spark-3.3.jar ${SHARED_WORKSPACE}/jars/deequ-2.0.3-spark-3.3.jar
RUN curl -o ${SHARED_WORKSPACE}/jars/deequ-2.0.3-spark-3.3.jar \
  https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.3-spark-3.3/deequ-2.0.3-spark-3.3.jar


WORKDIR ${SHARED_WORKSPACE}
RUN pip3 install -r requirements.txt
RUN pip3 install -r requirements_test.txt