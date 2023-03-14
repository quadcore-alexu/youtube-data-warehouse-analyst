FROM openjdk:8-jre-alpine

ENV SPARK_VERSION=3.3.2
ENV DELTA_VERSION=2.1

RUN apk update && \
    apk add --no-cache wget python3 py3-pip && \
    apk add --no-cache bash && \
    wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/0.4.0/delta-core_2.12-0.4.0.jar && \
    mv delta-core_2.12-0.4.0.jar /delta.jar && \
    apk del wget && \
    rm -rf /var/cache/apk/*

ENV SPARK_HOME /spark
ENV DELTA_HOME /delta

ENV PATH $PATH:$SPARK_HOME/bin:$DELTA_HOME/bin

CMD ["bash"]

