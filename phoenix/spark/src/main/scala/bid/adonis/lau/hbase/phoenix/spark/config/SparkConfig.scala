package bid.adonis.lau.hbase.phoenix.spark.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.debug.maxToStringFields", 256)
    .config("spark.sql.broadcastTimeout", 36000)
    .config("spark.driver.maxResultSize", "10g")
    .enableHiveSupport()
    .appName("SparkPhoenix")
    .getOrCreate()
}
