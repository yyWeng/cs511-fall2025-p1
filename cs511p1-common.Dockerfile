####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM openjdk:8

RUN apt update && \
    apt upgrade --yes && \
    apt install ssh openssh-server --yes

# Setup common SSH key.
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/shared_rsa -C common && \
    cat ~/.ssh/shared_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# ---- Common tools for downloads/logs (you can add others if needed) ----
RUN apt update && apt install -y --no-install-recommends \
      ca-certificates curl wget rsync vim net-tools iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# ====================== Hadoop 3.3.6 ======================
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop

RUN set -eux; \
    wget -q --tries=5 \
      https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz; \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt; \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}; \
    rm -f hadoop-${HADOOP_VERSION}.tar.gz; \
    mkdir -p /data/hdfs/namenode /data/hdfs/datanode /opt/hadoop/logs

# ====================== Spark 3.4.1 (Hadoop 3) ======================
ENV SPARK_VERSION=3.4.1
ENV HADOOP_MAJOR=3
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

RUN set -eux; \
    wget -q --tries=5 \
      https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz; \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz -C /opt; \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR} ${SPARK_HOME}; \
    rm -f spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz; \
    mkdir -p /var/log/spark

# ====================== Java path normalization ======================
ENV JAVA_HOME=/usr/local/openjdk-8
RUN ln -s /usr/local/openjdk-8 /usr/lib/jvm/java-8-openjdk-amd64 || true

# ====================== FINAL unified PATH ======================
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
RUN printf '%s\n' \
  'export PATH=/usr/local/openjdk-8/bin:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin:$PATH' \
  > /etc/profile.d/zz-cs511-path.sh
