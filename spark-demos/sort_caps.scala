// spark-demos/sort_caps.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().getOrCreate()
import spark.implicits._

val inPath  = "hdfs://main:9000/caps-demo/caps.csv"
val outPath = "hdfs://main:9000/caps-demo/sorted"

val schema = new StructType()
  .add("year", IntegerType, true)
  .add("serial", StringType,  true)

val df = spark.read
  .option("header", "false")
  .schema(schema)
  .csv(inPath)
  .na.drop(Seq("year","serial"))
  .filter($"year" <= 2025)
  .orderBy($"year".desc, $"serial".asc)

// 为了对比方便，合并为单个文件
df.coalesce(1)
  .write.mode("overwrite")
  .option("header","false")
  .csv(outPath)

println(s"Done. Wrote sorted caps to: $outPath")
