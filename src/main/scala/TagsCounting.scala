import org.apache.spark.sql.SparkSession


object TagsCounting extends App {

  val sparkSession: SparkSession = SparkSession.builder
    .master("local")
    .appName("SparkTest")
    .getOrCreate()

}
