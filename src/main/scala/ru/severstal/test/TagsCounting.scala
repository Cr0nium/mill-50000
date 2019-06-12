package ru.severstal.test

import java.net.URL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

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
//  longDF.createOrReplaceGlobalTempView("long_view")

  longDF.show(false)
  longDF.printSchema()

  val rollsDF = sparkSession
    .read
    .option("header", "true")
    .schema(schemaRolls)
    .option("delimiter",";")
    .csv(rolls_referance.getPath)
//  rollsDF.createOrReplaceGlobalTempView("long_view")

  rollsDF.show(false)
  rollsDF.printSchema()


}
