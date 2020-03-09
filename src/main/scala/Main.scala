import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession //for 'when'

object Main extends App {
  val sc = new SparkConf().setMaster("local[4]").setAppName("Spark-test")
  val ss = SparkSession.builder().config(sc).getOrCreate() //spark context

//  val simple = new SimpleSparkProcessing(ss)
//  val df = simple.geSimpletDF()
//  simple.processWithJoin(df._1, df._2)

  val delta = new DeltaProcessing(ss)
  delta.createDeltaFrame()
}
