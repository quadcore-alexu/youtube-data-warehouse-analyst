FROM openjdk:8-jre

ENV SPARK_VERSION=3.3.2
ENV DELTA_VERSION=1.0.0

RUN apt-get update && apt-get install -y wget && \
    wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar && \
    mv delta-core_2.12-${DELTA_VERSION}.jar /delta.jar && \
    apt-get remove -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME /spark
ENV DELTA_HOME /delta

ENV PATH $PATH:$SPARK_HOME/bin:$DELTA_HOME/bin

CMD ["bash"]

