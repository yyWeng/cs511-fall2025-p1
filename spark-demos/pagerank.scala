// spark-demos/pagerank.scala
// 读取 HDFS: hdfs://main:9000/pagerank-demo/edges.csv （每行 i,j）
// 输出 HDFS: hdfs://main:9000/pagerank-demo/out  （仅作为备份；验证时用 stdout）
// 同时在控制台打印：每行 "node,rank(3 decimals)"，按 PR 降序、node 升序

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

val spark = SparkSession.builder().getOrCreate()
val sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

// ===== 参数 =====
val inPath  = "hdfs://main:9000/pagerank-demo/edges.csv"
val outPath = "hdfs://main:9000/pagerank-demo/out"
val d = 0.85
val eps = 1e-4
val maxIters = 100

// ===== 读入边 (i,j) =====
val edges: RDD[(Int, Int)] = sc.textFile(inPath)
  .map(_.trim).filter(_.nonEmpty)
  .map { line =>
    val Array(a, b) = line.split(",", 2)
    (a.trim.toInt, b.trim.toInt)
  }

// 所有节点（包含入点和出点），用于补齐无出边节点
val nodes: RDD[Int] = edges.flatMap{ case (i,j) => Iterator(i,j) }.distinct().cache()
val N: Long = nodes.count()
require(N > 0, "Empty graph.")

// 邻接表（无重复）
val links0: RDD[(Int, Set[Int])] =
  edges.distinct().groupByKey().mapValues(_.toSet)

// 给所有节点补齐邻接（没有出边的为 Set()）
val links: RDD[(Int, Set[Int])] = nodes
  .map(n => (n, Set.empty[Int]))
  .leftOuterJoin(links0)
  .mapValues { case (empty, opt) => opt.getOrElse(empty) }
  .cache()

// 初始均匀 PR
var ranks: RDD[(Int, Double)] = nodes.map(n => (n, 1.0 / N)).cache()

def l1Diff(a: RDD[(Int, Double)], b: RDD[(Int, Double)]): Double = {
  a.join(b).values.map{ case (x,y) => math.abs(x-y) }.sum()
}

// 迭代
var iter = 0
var delta = 1.0
while (iter < maxIters && delta > eps) {
  // dangling mass：无出边节点的 rank 之和
  val danglingMass = ranks.join(links)
    .filter{ case (_, (r, nbrs)) => nbrs.isEmpty }
    .values.map(_._1).sum()

  // 贡献：有出边的平均分摊
  val contribs: RDD[(Int, Double)] = links.join(ranks).flatMap {
    case (u, (nbrs, ru)) =>
      if (nbrs.isEmpty) Iterator.empty
      else {
        val share = ru / nbrs.size
        nbrs.iterator.map(v => (v, share))
      }
  }

  val sumContribs = contribs.reduceByKey(_ + _)

  val base = (1.0 - d) / N
  val danglingShare = d * danglingMass / N

  val newRanks: RDD[(Int, Double)] = nodes
    .map(n => (n, 0.0))
    .leftOuterJoin(sumContribs)
    .mapValues { case (_, opt) => base + danglingShare + d * opt.getOrElse(0.0) }

  // 计算 L1 差异
  delta = l1Diff(newRanks, ranks)
  ranks.unpersist(blocking = false)
  ranks = newRanks.cache()
  iter += 1
}

// 排序 & 格式化：PR 降序、node 升序；保留三位小数
val ranked = ranks.sortBy({ case (n, r) => (-r, n) }, ascending = true)
val linesOut: Array[String] = ranked
  .map { case (n, r) => f"$n,${r}%.3f" }
  .collect()

// 控制台打印（供 test/对比），严格只打印结果行
println(linesOut.mkString("\n"))

// 也写一份到 HDFS（可选）
import spark.implicits._
spark.createDataset(linesOut.toSeq)
  .coalesce(1)
  .write.mode("overwrite").text(outPath)
