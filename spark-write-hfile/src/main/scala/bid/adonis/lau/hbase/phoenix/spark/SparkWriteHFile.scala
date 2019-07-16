package bid.adonis.lau.hbase.phoenix.spark

import java.io.IOException
import java.util.Date

import bid.adonis.lau.hbase.phoenix.spark.config.SparkConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable

/**
  * 使用BulkLoad的方式将Hive数据导入HBase
  *
  * @author: adonis lau
  * @email: adonis.lau.dev@gmail.com
  * @date: 2019-07-15 14:16
  */
object SparkWriteHFile {

  def main(args: Array[String]) {
    println("程序开始运行", new Date())

    // 基础参数
    val rowKeyField = "ID"
    val quorum = "adonis"
    val clientPort = "2181"

    val timeMillis = System.currentTimeMillis
    val hBaseTempTable = "HOMEINNS_USERINFO_HFILE" + timeMillis

    val hfilePath = s"hdfs://adonis:8020/user/${System.getProperty("user.name")}/hbase/hfile/" + timeMillis
    println("文件保存路径为", hfilePath)

    val spark = SparkConfig.spark

    // 从hive表读取数据
    println("开始查询数据", new Date())
    val limitNum = if (args != null && args.length > 0 && args(0) != null) args(0) else 10
    val datahiveDF = spark.sql(
      s"""
         | SELECT
         |   `NAME`,
         |   `CARDNO`,
         |   `DESCRIOT`,
         |   `CTFTP`,
         |   `CTFID`,
         |   `GENDER`,
         |   `BIRTHDAY`,
         |   `ADDRESS`,
         |   `ZIP`,
         |   `DIRTY`,
         |   `DISTRICT1`,
         |   `DISTRICT2`,
         |   `DISTRICT3`,
         |   `DISTRICT4`,
         |   `DISTRICT5`,
         |   `DISTRICT6`,
         |   `FIRSTNM`,
         |   `LASTNM`,
         |   `DUTY`,
         |   `MOBILE`,
         |   `TEL`,
         |   `FAX`,
         |   `EMAIL`,
         |   `NATION`,
         |   `TASTE`,
         |   `EDUCATION`,
         |   `COMPANY`,
         |   `CTEL`,
         |   `CADDRESS`,
         |   `CZIP`,
         |   `FAMILY`,
         |   `VERSION`,
         |   `ID`
         | FROM
         |   HOMEINNS.USERINFO_ORC
         | LIMIT $limitNum
        """.stripMargin)
    println("数据查询完成", new Date())

    // 获取表结构字段，去掉rowKey字段
    var fields = datahiveDF.columns.dropWhile(rowKeyField.equalsIgnoreCase)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", quorum)
    hBaseConf.set("hbase.zookeeper.property.clientPort", clientPort)

    // 表不存在则建Hbase临时表
    creteHTable(hBaseTempTable, hBaseConf)

    val hbaseContext = new HBaseContext(spark.sparkContext, hBaseConf)

    // 将DataFrame转换bulkload需要的RDD格式
    val rddnew = datahiveDF.rdd.map(row => {
      val rowKey = row.getAs[String](rowKeyField)

      fields.map(field => {
        val fieldValue = row.getAs[String](field)
        (Bytes.toBytes(rowKey), Array((Bytes.toBytes("info"), Bytes.toBytes(field), Bytes.toBytes(fieldValue))))
      })
    }).flatMap(array => {
      array
    })

    // 使用HBaseContext的bulkload生成HFile文件
    println("数据生成HFile开始", new Date())
    hbaseContext.bulkLoad[Put](rddnew.map(record => {
      val put = new Put(record._1)
      record._2.foreach(putValue => put.addColumn(putValue._1, putValue._2, putValue._3))
      put
    }), TableName.valueOf(hBaseTempTable), (t: Put) => putForLoad(t), hfilePath)

    println("数据生成HFile完成", new Date())

    val conn = ConnectionFactory.createConnection(hBaseConf)
    val hbTableName = TableName.valueOf(hBaseTempTable.getBytes())
    val regionLocator = new HRegionLocator(hbTableName, classOf[ClusterConnection].cast(conn))
    val realTable = conn.getTable(hbTableName)
    HFileOutputFormat2.configureIncrementalLoad(Job.getInstance(), realTable, regionLocator)

    // bulk load start
    if (args != null && args.length > 1 && args(1) != null && args(1).equalsIgnoreCase("save")) {
      println("开始load数据", new Date())
      val loader = new LoadIncrementalHFiles(hBaseConf)
      val admin = conn.getAdmin
      loader.doBulkLoad(new Path(hfilePath), admin, realTable, regionLocator)
    }

    spark.stop()
    println("程序运行结束", new Date())
  }

  /**
    * 创建HBase表
    *
    * @param tableName 表名
    */
  def creteHTable(tableName: String, hBaseConf: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(hBaseConf)
    val hBaseTableName = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    if (!admin.tableExists(hBaseTableName)) {
      val tableDesc = new HTableDescriptor(hBaseTableName)
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes))
      admin.createTable(tableDesc)
    }
    connection.close()
  }

  /**
    * Prepare the Put object for bulkload function.
    *
    * @param put The put object.
    * @throws java.io.IOException            IOException
    * @throws java.lang.InterruptedException InterruptedException
    * @return Tuple of (KeyFamilyQualifier, bytes of cell value)*/
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def putForLoad(put: Put): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
    val ret: mutable.MutableList[(KeyFamilyQualifier, Array[Byte])] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (cells <- put.getFamilyCellMap.entrySet().iterator()) {
      val family = cells.getKey
      for (value <- cells.getValue) {
        val kfq = new KeyFamilyQualifier(CellUtil.cloneRow(value), family, CellUtil.cloneQualifier(value))
        ret.+=((kfq, CellUtil.cloneValue(value)))
      }
    }
    ret.iterator
  }
}


