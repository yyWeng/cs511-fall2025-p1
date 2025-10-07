#!/bin/bash

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################
/etc/init.d/ssh start
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/shared_rsa
# 把 root 自己的 id_rsa.pub 装到 worker 的 authorized_keys（用于 Hadoop/Spark 远程启动）
ssh-copy-id -i ~/.ssh/id_rsa -o 'IdentityFile ~/.ssh/shared_rsa' -o StrictHostKeyChecking=no -f worker1 || true
ssh-copy-id -i ~/.ssh/id_rsa -o 'IdentityFile ~/.ssh/shared_rsa' -o StrictHostKeyChecking=no -f worker2 || true
# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# ===== 环境变量 =====
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# 让 Hadoop 脚本以 root 身份启动各角色（容器场景必需）
export HDFS_NAMENODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export HDFS_DATANODE_USER=root

# Hadoop 的 ssh 选项：使用共享密钥，关闭指纹确认
export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ~/.ssh/shared_rsa"

# 数据目录
mkdir -p /data/hdfs/namenode /data/hdfs/datanode

# ===== 首次启动才格式化 NameNode =====
if [ ! -f /data/hdfs/namenode/current/VERSION ]; then
  echo "Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

# ===== 启动 HDFS =====
echo "Starting HDFS via start-dfs.sh..."
start-dfs.sh

# 简单等待
sleep 3
echo "==== HDFS report (short) ===="
hdfs dfsadmin -report | sed -n '1,80p' || true

# ===== 启动 Spark =====
if [ -d /opt/spark ]; then
  export SPARK_HOME=/opt/spark
  export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

  # Master
  "$SPARK_HOME/sbin/start-master.sh" || true

  # 本机也起一个 worker（第三个 worker）
  "$SPARK_HOME/sbin/start-worker.sh" spark://main:7077 || true

  # 远程启动 worker1/worker2
  ssh $HADOOP_SSH_OPTS worker1 "/opt/spark/sbin/start-worker.sh spark://main:7077" || true
  ssh $HADOOP_SSH_OPTS worker2 "/opt/spark/sbin/start-worker.sh spark://main:7077" || true
fi

echo "All services started. Tailing logs to keep container alive..."
# 若有日志就跟随日志；否则兜底挂起
tail -F /opt/hadoop/logs/* 2>/dev/null || tail -f /dev/null
