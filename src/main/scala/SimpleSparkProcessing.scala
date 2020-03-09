import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SimpleSparkProcessing(spark: SparkSession) {
  val ss: SparkSession = spark

  import ss.implicits._

  def createCol(df1: DataFrame): Unit = {
    df1.show(false)
    val df2 = df1.withColumn("CPF", when($"ID" === 1, lit("123.456.78")).otherwise(lit("null")))
    df2.write.mode("overwrite").format("parquet").option("delimiter", "|").save("Data/base-parquet.parquet") //saving using parquet
  }

  def processWithJoin(df1: DataFrame, df2: DataFrame): Unit = {
    val df3 = df1.as("nm")
      .join(df2.as("tel"), $"nm.ID" === $"tel.ID", "inner")
      .drop($"tel.ID")
    df3.orderBy($"nm.ID").show(false)
  }

  def geSimpletDF(): (DataFrame, DataFrame) ={
    (Seq(("1", "eduardo"), ("2", "jose"), ("3", "joana")).toDF("ID", "NAME"), Seq(("1", "123"), ("2", "456"), ("3", "789"), ("1", "987")).toDF("ID", "TEL"))
  }
}
