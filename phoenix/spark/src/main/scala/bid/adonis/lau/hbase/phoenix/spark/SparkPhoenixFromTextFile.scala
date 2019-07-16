package bid.adonis.lau.hbase.phoenix.spark

import bid.adonis.lau.hbase.phoenix.spark.config.SparkConfig
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
  * @Author adonis
  * @Date 2019-07-09 12:23
  * @Description
  */
object SparkPhoenixFromTextFile {

  private val spark = SparkConfig.spark
  val tableName = "HOMEINNS_USERINFO_SPARK"
  val zkUrl = "adonis:2181"
  val filepath = "hdfs://adonis/hive/homeinns/userinfo"

  def main(args: Array[String]): Unit = {
    val fields = Array("NAME", "CARDNO", "DESCRIOT", "CTFTP", "CTFID", "GENDER", "BIRTHDAY", "ADDRESS", "ZIP", "DIRTY", "DISTRICT1", "DISTRICT2", "DISTRICT3", "DISTRICT4", "DISTRICT5", "DISTRICT6", "FIRSTNM", "LASTNM", "DUTY", "MOBILE", "TEL", "FAX", "EMAIL", "NATION", "TASTE", "EDUCATION", "COMPANY", "CTEL", "CADDRESS", "CZIP", "FAMILY", "VERSION", "ID")
    val schema = StructType(fields.map(fieldName => StructField(fieldName, StringType)))

//    val lines = spark.sparkContext.textFile("hdfs://adonis/hive/homeinns/userinfo")
//      .map(_.split("\n")).map(Row(_: _*))
//    val data = spark.createDataFrame(lines, schema)

    val data: DataFrame = spark
      .read
      .format("com.databricks.spark.csv")
      .option("inferSchema","true")
      .option("delimiter","\u0005")
      .load(filepath)
      .toDF("NAME", "CARDNO", "DESCRIOT", "CTFTP", "CTFID", "GENDER", "BIRTHDAY", "ADDRESS", "ZIP", "DIRTY", "DISTRICT1", "DISTRICT2", "DISTRICT3", "DISTRICT4", "DISTRICT5", "DISTRICT6", "FIRSTNM", "LASTNM", "DUTY", "MOBILE", "TEL", "FAX", "EMAIL", "NATION", "TASTE", "EDUCATION", "COMPANY", "CTEL", "CADDRESS", "CZIP", "FAMILY", "VERSION", "ID")

    data.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("table", tableName)
      .option("zkUrl", zkUrl)
      .save()


  }
}
