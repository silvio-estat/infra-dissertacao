FROM apache/airflow:2.9.0-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Instala o provider com constraints para não atualizar o airflow core
RUN pip install --no-cache-dir \
        "apache-airflow-providers-apache-spark==4.7.1" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt"

# provider OpenLineage (compatível com Airflow 2.9)
RUN pip install --no-cache-dir \
        "apache-airflow-providers-openlineage" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt"

# Instala pyspark sem deps para evitar conflito com as constraints do airflow
RUN pip install --no-cache-dir --no-deps "pyspark==3.5.8"

# Instala o SDK de ingestão do OpenMetadata + extras Trino
# (versão 1.12.5.0 para compatibilidade com o servidor OM 1.12.5)
# Sem constraints para evitar conflito de dependências entre Airflow e OM
RUN pip install --no-cache-dir "openmetadata-ingestion[trino]==1.12.5.0"

# Baixa JARs do Iceberg e S3A para o diretório de JARs do pyspark local
# (driver em client-mode precisa dos mesmos JARs que o cluster)
ENV ICEBERG_VERSION=1.5.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367

ENV OPENLINEAGE_VERSION=1.18.0

RUN PYSPARK_JARS=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && curl -fsSL -o ${PYSPARK_JARS}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    && curl -fsSL -o ${PYSPARK_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSL -o ${PYSPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    && curl -fsSL -o ${PYSPARK_JARS}/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar \
        https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OPENLINEAGE_VERSION}/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar
