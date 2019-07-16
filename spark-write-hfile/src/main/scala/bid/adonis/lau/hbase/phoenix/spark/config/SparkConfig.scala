package bid.adonis.lau.hbase.phoenix.spark.config

import org.apache.spark.sql.SparkSession

/**
  * SparkConfig
  *
  * @author: adonis lau
  * @email: adonis.lau.dev@gmail.com
  * @date: 2019-07-11 18:10
  */
object SparkConfig {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.debug.maxToStringFields", 256)
    .config("spark.sql.broadcastTimeout", 36000)
    .config("spark.driver.maxResultSize", "10g")
    .enableHiveSupport()
    .appName("SparkWriteHFile")
    .getOrCreate()
}
