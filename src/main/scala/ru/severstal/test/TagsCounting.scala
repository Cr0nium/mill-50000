package ru.severstal.test

import java.net.URL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{broadcast, col}

object TagsCounting extends App {

  val sparkSession: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Severstal_test")
    .getOrCreate()

  private var long_referance: URL = getClass.getClassLoader.getResource("long.csv")
  private var rolls_referance: URL = getClass.getClassLoader.getResource("rolls.csv")

  val schemaLong = StructType(List(
    StructField("ts", TimestampType, nullable = true),
    StructField("value", LongType, nullable = true),
    StructField("tag", StringType, nullable = true)
  ))

  val schemaRolls = StructType(List(
    StructField("roll_id", LongType, nullable = true),
    StructField("roll_start", TimestampType, nullable = true),
    StructField("roll_end", TimestampType, nullable = true)
  ))

  val longDF = sparkSession
    .read
    .option("header", "true")
    .schema(schemaLong)
    .option("delimiter",";")
    .csv(long_referance.getPath)

  val rollsDF = sparkSession
    .read
    .option("header", "true")
    .schema(schemaRolls)
    .option("delimiter",";")
    .csv(rolls_referance.getPath)

  val rollsAndLongLeftJoinDF = longDF.join(broadcast(rollsDF), col("ts") between(col("roll_start"), col("roll_end")), "right")
      .drop("ts", "roll_start", "roll_end")
  rollsAndLongLeftJoinDF.createOrReplaceTempView("tmp_view")

  val resultAggDF = sparkSession.sql("select roll_id, tag, max(value) as max, mean(value) as mean, percentile_approx(value, 0.5, 100) as median, " +
    "percentile_approx(value, 0.99, 100) as 99_percentile, percentile_approx(value, 0.01, 100) as 1_percentile from tmp_view group by roll_id, tag order by roll_id")

  resultAggDF
    .orderBy("roll_id")
    .coalesce(1)
    .write
    .format("csv")
    .option("header", "true")
    .save("H:\\res\\finalScala")

}
