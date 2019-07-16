package bid.adonis.lau.hbase.phoenix.spark

import bid.adonis.lau.hbase.phoenix.spark.config.SparkConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @Author adonis
  * @Date 2019-07-09 12:23
  * @Description
  */
object SparkPhoenixFromRcHive {
  private val spark = SparkConfig.spark
  val tableName = "HOMEINNS_USERINFO_RCFILE"
  val zkUrl = "adonis:2181"

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val fields = Array("NAME", "CARDNO", "DESCRIOT", "CTFTP", "CTFID", "GENDER", "BIRTHDAY", "ADDRESS", "ZIP", "DIRTY", "DISTRICT1", "DISTRICT2", "DISTRICT3", "DISTRICT4", "DISTRICT5", "DISTRICT6", "FIRSTNM", "LASTNM", "DUTY", "MOBILE", "TEL", "FAX", "EMAIL", "NATION", "TASTE", "EDUCATION", "COMPANY", "CTEL", "CADDRESS", "CZIP", "FAMILY", "VERSION", "ID")
    val schema = StructType(fields.map(fieldName => StructField(fieldName, StringType)))

    spark.sql(
      """
        |select
        |`name`,
        |`cardno`,
        |`descriot`,
        |`ctftp`,
        |`ctfid`,
        |`gender`,
        |`birthday`,
        |`address`,
        |`zip`,
        |`dirty`,
        |`district1`,
        |`district2`,
        |`district3`,
        |`district4`,
        |`district5`,
        |`district6`,
        |`firstnm`,
        |`lastnm`,
        |`duty`,
        |`mobile`,
        |`tel`,
        |`fax`,
        |`email`,
        |`nation`,
        |`taste`,
        |`education`,
        |`company`,
        |`ctel`,
        |`caddress`,
        |`czip`,
        |`family`,
        |`version`,
        |`id`
        |from homeinns.userinfo_rcfile
      """.stripMargin)
      .write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("table", tableName)
      .option("zkUrl", zkUrl)
      .save()
  }
}
