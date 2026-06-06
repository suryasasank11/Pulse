# Spark runner for Pulse silver jobs.
# IMPORTANT: pinned to Debian bookworm so we get Java 17 (openjdk-17), which
# Spark 3.5.3 officially supports. The default python:3.11-slim moved to Debian
# trixie, which only ships Java 21 -- and Spark 3.5.3 does NOT support Java 21
# (it crashes mid-job with a module-access error). Pinning bookworm fixes that.
#
# Build from the REPO ROOT:
#     docker build -t pulse-spark -f spark/Dockerfile .
FROM python:3.11-slim-bookworm

# Java 17 (supported by Spark 3.5.3); procps gives `ps`, which Spark calls.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

# PySpark 3.5.3 ships a Spark build that uses the Hadoop 3.3.4 client.
RUN pip install --no-cache-dir pyspark==3.5.3 boto3 python-dotenv

# Matching S3A jars so Spark can read/write s3a:// (one-time ~300 MB download).
ARG HADOOP_AWS=3.3.4
ARG AWS_SDK=1.12.262
RUN SPARK_JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") && \
    curl -fsSL -o "$SPARK_JARS/hadoop-aws-${HADOOP_AWS}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar" && \
    curl -fsSL -o "$SPARK_JARS/aws-java-sdk-bundle-${AWS_SDK}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK}/aws-java-sdk-bundle-${AWS_SDK}.jar"

ENV PYTHONPATH=/app
WORKDIR /app
COPY pulse/ ./pulse/
COPY spark/ ./spark/

ENTRYPOINT ["spark-submit"]