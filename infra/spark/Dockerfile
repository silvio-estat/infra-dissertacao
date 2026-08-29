FROM apache/spark:3.5.8-python3

USER root

ENV ICEBERG_VERSION=1.5.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367

RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    && curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

ENV OPENLINEAGE_VERSION=1.18.0
RUN curl -fsSL -o /opt/spark/jars/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar \
        https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OPENLINEAGE_VERSION}/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER 185
