#!/bin/bash

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################
/etc/init.d/ssh start
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/shared_rsa
# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# 供主机通过 ssh 执行 Hadoop/Spark 脚本时使用
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# 让远程 start-dfs.sh 在 worker 上以 root 身份启动 datanode
export HDFS_DATANODE_USER=root

# 预留 Spark 的 PATH（主机会用 ssh 调用）
if [ -d /opt/spark ]; then
  export SPARK_HOME=/opt/spark
  export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
fi

echo "Worker is ready. Waiting..."
tail -f /dev/null
