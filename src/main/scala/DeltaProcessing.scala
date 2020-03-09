import org.apache.spark.sql.SparkSession

class DeltaProcessing(spark: SparkSession) {
  val ss: SparkSession = spark

  def createDeltaFrame(): Unit = {
    val data = ss.range(0, 5)
    data.write.format("delta").mode("overwrite").save("Data/delta-parquet2.parquet")
  }

  def readDeltaFrame(): Unit = {
    val df = spark.read.format("delta").load("Data/delta-parquet2.parquet")
    df.show()
  }
}
