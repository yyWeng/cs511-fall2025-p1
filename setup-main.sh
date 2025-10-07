#!/bin/bash
set -e

# 让当前构建阶段能用到 Java
export JAVA_HOME=/usr/local/openjdk-8

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# ===================== Setup HDFS (main) =====================
export HADOOP_HOME=/opt/hadoop
CONF_DIR="$HADOOP_HOME/etc/hadoop"

# 1) Hadoop 找到 Java
echo 'export JAVA_HOME=/usr/local/openjdk-8' >> "$CONF_DIR/hadoop-env.sh"

cat >/etc/profile.d/hadoop.sh <<'EOF'
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
EOF

# 2) core-site.xml
cat > "$CONF_DIR/core-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://main:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop</value>
  </property>
</configuration>
EOF

# 3) hdfs-site.xml
cat > "$CONF_DIR/hdfs-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/data/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/data/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>main:9000</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>main:9870</value>
  </property>
</configuration>
EOF

# 4) mapred-site.xml（HDFS 测试用，保持 local）
cat > "$CONF_DIR/mapred-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
</configuration>
EOF

# 5) workers（HDFS datanodes）
cat > "$CONF_DIR/workers" <<'EOF'
main
worker1
worker2
EOF

# 6) 数据目录
mkdir -p /data/hdfs/namenode /data/hdfs/datanode

# ===================== Setup Spark (main) =====================
export SPARK_HOME=/opt/spark
mkdir -p "$SPARK_HOME/conf"

# spark-env.sh：Master 主机名 + 资源
cat > "$SPARK_HOME/conf/spark-env.sh" <<'EOF'
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=main
# 控制 worker 资源，避免 OOM
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1g
EOF
chmod +x "$SPARK_HOME/conf/spark-env.sh"

# workers（Spark）
cat > "$SPARK_HOME/conf/workers" <<'EOF'
main
worker1
worker2
EOF

# spark-defaults：默认直连 master，并能读 HDFS
cat > "$SPARK_HOME/conf/spark-defaults.conf" <<'EOF'
spark.master                     spark://main:7077
spark.hadoop.fs.defaultFS        hdfs://main:9000
spark.eventLog.enabled           false
EOF
