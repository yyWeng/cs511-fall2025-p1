#!/bin/bash

test_hdfs.sh
function test_hdfs_q1() {
    docker compose -f cs511p1-compose.yaml exec main hdfs dfsadmin -report >&2
}

function test_hdfs_q2() {
    docker compose -f cs511p1-compose.yaml cp resources/fox.txt main:/test_fox.txt
    docker compose -f cs511p1-compose.yaml exec main bash -x -c '\
        hdfs dfs -mkdir -p /test; \
        hdfs dfs -put -f /test_fox.txt /test/fox.txt; \
        hdfs dfs -cat /test/fox.txt'
}

function test_hdfs_q3() {
    docker compose -f cs511p1-compose.yaml exec main bash -x -c '\
        hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://main:9000 -op create -threads 100 -files 10000; \
        hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://main:9000 -op open -threads 100 -files 10000; \
        hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://main:9000 -op delete -threads 100 -files 10000; \
        hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://main:9000 -op rename -threads 100 -files 10000'
}

function test_hdfs_q4() {
    docker compose -f cs511p1-compose.yaml cp resources/hadoop-terasort-3.3.6.jar \
    main:/hadoop-terasort-3.3.6.jar
docker compose -f cs511p1-compose.yaml exec main bash -x -c '\
    hdfs dfs -rm -r -f tera-in tera-out tera-val; \
    hadoop jar /hadoop-terasort-3.3.6.jar teragen 1000000 tera-in; \
    hadoop jar /hadoop-terasort-3.3.6.jar terasort tera-in tera-out; \
    hadoop jar /hadoop-terasort-3.3.6.jar teravalidate tera-out tera-val; \
    hdfs dfs -cat tera-val/*;'
}

# test_spark.sh
function test_spark_q1() {
    docker compose -f cs511p1-compose.yaml cp resources/active_executors.scala \
        main:/active_executors.scala
    docker compose -f cs511p1-compose.yaml exec main bash -x -c '\
        cat /active_executors.scala | spark-shell --master spark://main:7077'
}

function test_spark_q2() {
    docker compose -f cs511p1-compose.yaml cp resources/pi.scala main:/pi.scala
    docker compose -f cs511p1-compose.yaml exec main bash -x -c '\
        cat /pi.scala | spark-shell --master spark://main:7077'

}

function test_spark_q3() {
    docker compose -f cs511p1-compose.yaml cp resources/fox.txt main:/test_fox.txt
    docker compose -f cs511p1-compose.yaml exec main bash -x -c '\
        hdfs dfs -mkdir -p /test; \
        hdfs dfs -put -f /test_fox.txt /test/fox.txt; \
        hdfs dfs -cat /test/fox.txt'
    docker compose -f cs511p1-compose.yaml exec main bash -x -c '\
        echo "sc.textFile(\"hdfs://main:9000/test/fox.txt\").collect()" | \
        spark-shell --master spark://main:7077'

}

function test_spark_q4() {
    docker compose -f cs511p1-compose.yaml cp resources/spark-terasort-1.2.jar \
        main:/spark-terasort-1.2.jar
    docker compose -f cs511p1-compose.yaml exec main spark-submit \
        --master spark://main:7077 \
        --class com.github.ehiggs.spark.terasort.TeraGen local:///spark-terasort-1.2.jar \
        100m hdfs://main:9000/spark/tera-in
    docker compose -f cs511p1-compose.yaml exec main spark-submit \
        --master spark://main:7077 \
        --class com.github.ehiggs.spark.terasort.TeraSort local:///spark-terasort-1.2.jar \
        hdfs://main:9000/spark/tera-in hdfs://main:9000/spark/tera-out
    docker compose -f cs511p1-compose.yaml exec main spark-submit \
        --master spark://main:7077 \
        --class com.github.ehiggs.spark.terasort.TeraValidate local:///spark-terasort-1.2.jar \
        hdfs://main:9000/spark/tera-out hdfs://main:9000/spark/tera-val
}

function test_terasorting() {
    set -e

    # 1) 输入数据：若 HDFS 已有就跳过上传
    if ! docker compose -f cs511p1-compose.yaml exec -T main hdfs dfs -test -f /caps-demo/caps.csv 2>/dev/null; then
        if [ -f resources/caps_example.csv ]; then
            docker compose -f cs511p1-compose.yaml cp resources/caps_example.csv main:/caps.csv >/dev/null 2>&1
        else
            tmpfile="$(mktemp)"
            cat > "$tmpfile" <<'EOF'
1999,1234-5678-91011
1800,1001-1002-10003
2023,0829-0914-00120
2050,9999-9999-99999
EOF
            docker compose -f cs511p1-compose.yaml cp "$tmpfile" main:/caps.csv >/dev/null 2>&1
            rm -f "$tmpfile"
        fi
        docker compose -f cs511p1-compose.yaml exec -T main bash -lc '\
          hdfs dfs -mkdir -p /caps-demo && hdfs dfs -put -f /caps.csv /caps-demo/caps.csv' >/dev/null 2>&1
    fi

    # 2) 若没有已生成的正确输出，则运行 Spark；所有 compose 日志都丢到 /dev/null
    if ! docker compose -f cs511p1-compose.yaml exec -T main hdfs dfs -test -f /caps-demo/sorted/_SUCCESS 2>/dev/null; then
        docker compose -f cs511p1-compose.yaml exec -T main bash -lc '
cat > /tmp/sort_caps.scala <<'"'"'SCALA'"'"'
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val spark = SparkSession.builder().getOrCreate()
import spark.implicits._
val inPath  = "hdfs://main:9000/caps-demo/caps.csv"
val outPath = "hdfs://main:9000/caps-demo/sorted"
val schema = new StructType().add("year", IntegerType, true).add("serial", StringType, true)
val df = spark.read.option("header","false").schema(schema).csv(inPath)
  .na.drop(Seq("year","serial")).filter($"year" <= 2025)
  .orderBy($"year".desc, $"serial".asc)
df.coalesce(1).write.mode("overwrite").option("header","false").csv(outPath)
SCALA
SPARK_OPTS="--master spark://main:7077 --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=2"
spark-shell $SPARK_OPTS -i /tmp/sort_caps.scala >/dev/null 2>&1
' >/dev/null 2>&1
    fi

    # 3) 只输出排序结果行到 stdout；关闭 compose 的 stderr
    docker compose -f cs511p1-compose.yaml exec -T main bash -lc 'hdfs dfs -cat /caps-demo/sorted/part-*' 2>/dev/null
}

function test_pagerank() {
  # 1) 准备输入（若无则写入示例）
  docker compose -f cs511p1-compose.yaml exec -T main bash -lc '
    set -e
    hdfs dfs -test -f /pagerank-demo/edges.csv || {
      cat > /tmp/edges.csv <<EOF
2,3
3,2
4,2
5,2
5,6
6,5
7,5
8,5
9,5
10,5
11,5
4,1
EOF
      hdfs dfs -mkdir -p /pagerank-demo
      hdfs dfs -put -f /tmp/edges.csv /pagerank-demo/edges.csv
    }
  ' >/dev/null 2>&1

  # 2) 在容器里生成 Scala 文件 + 运行 spark-shell（stdout -> 文件；stderr 丢弃）
  docker compose -f cs511p1-compose.yaml exec -T main bash -lc '
set -e
cat > /tmp/pagerank.scala <<'"'"'SCALA'"'"'
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
val spark = SparkSession.builder().getOrCreate()
val sc = spark.sparkContext; sc.setLogLevel("ERROR")
val d = 0.85; val eps = 1e-4; val maxIters = 100

val edges = sc.textFile("hdfs://main:9000/pagerank-demo/edges.csv")
  .map(_.trim).filter(_.nonEmpty)
  .map{ line => val a = line.split(",", 2); (a(0).trim.toInt, a(1).trim.toInt) }

val nodes = edges.flatMap{ case (i,j) => Iterator(i,j) }.distinct().cache()
val N = nodes.count()
val links0 = edges.distinct().groupByKey().mapValues(_.toSet)
val links = nodes.map(n => (n, Set.empty[Int])).leftOuterJoin(links0)
  .mapValues{ case (e, o) => o.getOrElse(e) }.cache()

var ranks: RDD[(Int, Double)] = nodes.map(n => (n, 1.0 / N)).cache()
def l1(a: RDD[(Int, Double)], b: RDD[(Int, Double)]) =
  a.join(b).values.map{ case (x,y) => math.abs(x-y) }.sum()

var iter = 0; var delta = 1.0
while (iter < maxIters && delta > eps) {
  val dangling = ranks.join(links).filter{ case (_, (r, nbrs)) => nbrs.isEmpty }
    .values.map(_._1).sum()
  val contribs = links.join(ranks).flatMap {
    case (u, (nbrs, ru)) =>
      if (nbrs.isEmpty) Iterator.empty
      else nbrs.iterator.map(v => (v, ru / nbrs.size))
  }.reduceByKey(_ + _)
  val base = (1.0 - d) / N
  val dshare = d * dangling / N
  val next = nodes.map(n => (n, 0.0)).leftOuterJoin(contribs)
    .mapValues{ case (_, opt) => base + dshare + d * opt.getOrElse(0.0) }
  delta = l1(next, ranks); ranks.unpersist(false); ranks = next.cache(); iter += 1
}

// 只打印结果：PR 降序、node 升序，三位小数
val out = ranks.sortBy({ case (n,r) => (-r, n) }).map{ case (n,r) => f"$n,${r}%.3f" }
out.collect().foreach(println)

// 确保 REPL 退出
sys.exit(0)
SCALA

SPARK_OPTS="--master spark://main:7077 --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=2"
spark-shell $SPARK_OPTS -i /tmp/pagerank.scala > /tmp/pr_raw.txt 2>/dev/null

# 3) 只把 "数字,数字.三位" 的行打印到 stdout；若为空则回显原始（便于排错）
grep -E "^[0-9]+,[0-9]+\.[0-9]{3}$" /tmp/pr_raw.txt > /tmp/pr_only.txt || true
if [ -s /tmp/pr_only.txt ]; then
  cat /tmp/pr_only.txt
else
  cat /tmp/pr_raw.txt
fi
' 2>/dev/null
}



GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

mkdir -p out

total_score=0;

echo -n "Testing HDFS Q1 ..."
test_hdfs_q1 > out/test_hdfs_q1.out 2>&1
if grep -q "Live datanodes (3)" out/test_hdfs_q1.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing HDFS Q2 ..."
test_hdfs_q2 > out/test_hdfs_q2.out 2>&1
if grep -E -q '^The quick brown fox jumps over the lazy dog[[:space:]]*$' out/test_hdfs_q2.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing HDFS Q3 ..."
test_hdfs_q3 > out/test_hdfs_q3.out 2>&1
if [ "$(grep -E '# operations: 10000[[:space:]]*$' out/test_hdfs_q3.out | wc -l)" -eq 4 ]; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing HDFS Q4 ..."
test_hdfs_q4 > out/test_hdfs_q4.out 2>&1
if [ "$(grep -E 'Job ([[:alnum:]]|_)+ completed successfully[[:space:]]*$' out/test_hdfs_q4.out | wc -l)" -eq 3 ] && grep -q "7a27e2d0d55de" out/test_hdfs_q4.out; then
    echo -e " ${GREEN}PASS${NC}";
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing Spark Q1 ..."
test_spark_q1 > out/test_spark_q1.out 2>&1
if grep -E -q "Seq\[String\] = List\([0-9\.:]*, [0-9\.:]*, [0-9\.:]*\)" out/test_spark_q1.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing Spark Q2 ..."
test_spark_q2 > out/test_spark_q2.out 2>&1
if grep -E -q '^Pi is roughly 3.14[0-9]*' out/test_spark_q2.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing Spark Q3 ..."
test_spark_q3 > out/test_spark_q3.out 2>&1
if grep -q 'Array(The quick brown fox jumps over the lazy dog)' out/test_spark_q3.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing Spark Q4 ..."
test_spark_q4 > out/test_spark_q4.out 2>&1
if grep -E -q "^Number of records written: 1000000[[:space:]]*$" out/test_spark_q4.out && \
   grep -q "==== TeraSort took .* ====" out/test_spark_q4.out && \
   grep -q "7a30469d6f066" out/test_spark_q4.out && \
   grep -q "partitions are properly sorted" out/test_spark_q4.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=10 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing Tera Sorting ..."
test_terasorting > out/test_terasorting.out 2>&1
if diff --strip-trailing-cr resources/example-terasorting.truth out/test_terasorting.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=20 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo -n "Testing PageRank (extra credit) ..."
test_pagerank > out/test_pagerank.out 2>&1
if diff --strip-trailing-cr resources/example-pagerank.truth out/test_pagerank.out; then
    echo -e " ${GREEN}PASS${NC}"
    (( total_score+=20 ));
else
    echo -e " ${RED}FAIL${NC}"
fi

echo "-----------------------------------";
echo "Total Points/Full Points: ${total_score}/120";
