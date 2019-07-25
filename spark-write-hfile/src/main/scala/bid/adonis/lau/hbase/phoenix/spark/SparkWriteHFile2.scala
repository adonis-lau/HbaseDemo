package bid.adonis.lau.hbase.phoenix.spark

import java.util.Date

import bid.adonis.lau.hbase.phoenix.spark.config.SparkConfig
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * @Author adonis
  * @Date 2019-07-17 13:23
  * @Description
  */
object SparkWriteHFile2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    try {
      val spark = SparkConfig.spark
      val timeMillis = System.currentTimeMillis

      val hdfsPath = s"hdfs://adonis:8020/user/${System.getProperty("user.name")}/hbase/hfile/" + timeMillis

      val zkQuorum = "adonis"
      val zkPort = "2181"
      val tableName = "HOMEINNS_USERINFO_HFILE_2" + timeMillis

      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", zkQuorum)
      conf.set("hbase.zookeeper.property.clientPort", zkPort)
      conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      val table = new HTable(conf, tableName)

      // 表不存在则建Hbase临时表
      creteHTable(tableName, conf)

      lazy val job = Job.getInstance(conf)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable]) //设置MapOutput Key Value 的数据类型
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat2.configureIncrementalLoad(job, table)

      val limitNum = if (args != null && args.length != 0) args(0) else 10
      println("开始查询数据", new Date())
      val data = spark.sql(
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

      val fields = data.columns.sorted

      println("数据生成HFile开始", new Date())
      val dataRdd = data.rdd.map(line => {
        var kvlist: Seq[KeyValue] = List()
        var rowkey: Array[Byte] = null
        var cn: Array[Byte] = null
        var v: Array[Byte] = null
        var kv: KeyValue = null
        val cf: Array[Byte] = "USERINFO".getBytes //列族
        rowkey = Bytes.toBytes((line.getAs[String]("ID") + line.getAs[String]("ADDRESS") + line.getAs[String]("ZIP")).hashCode) //key
        for (i <- fields.indices) yield {
          cn = fields(i).getBytes //列的名称
          v = if (StringUtils.isNotBlank(line.getAs[String](fields(i)))) Bytes.toBytes(line.getAs[String](fields(i))) else Bytes.toBytes("") //列的值
          //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
          kv = new KeyValue(rowkey, cf, cn, v) //封装一下 rowkey, cf, clounmVale, value
          kvlist = kvlist :+ kv //将新的kv加在kvlist后面（不能反 需要整体有序）
        }
        (new ImmutableBytesWritable(rowkey), kvlist)
      })

      //RDD[(ImmutableBytesWritable, Seq[KeyValue])] 转换成 RDD[(ImmutableBytesWritable, KeyValue)]
      val result: RDD[(ImmutableBytesWritable, KeyValue)] = dataRdd.flatMapValues(s => {
        s.iterator
      })
      //写入到hfile数据
      result.repartition((data.count() / 1000000).toInt)
        .repartitionAndSortWithinPartitions(new HFilePartitioner(conf, table.getStartKeys, (data.count() / 1000000).toInt))
        .sortBy(x => x._1, ascending = true)
        .saveAsNewAPIHadoopFile(hdfsPath,
          classOf[ImmutableBytesWritable], classOf[KeyValue],
          classOf[HFileOutputFormat2], job.getConfiguration)
      println("数据生成HFile完成", new Date())

      if (args != null && args.length > 2 && args(1) != null && args(1).equalsIgnoreCase("save")) {
        println("开始load数据", new Date())
        //将保存在临时文件夹的hfile数据保存到hbase中
        val bulkLoader = new LoadIncrementalHFiles(conf)
        bulkLoader.doBulkLoad(new Path(hdfsPath), table)
      }
      println("程序运行结束", new Date())
    } catch {
      case e: Exception => e.printStackTrace()
    }
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
}

/**
  * 要保证处于同一个region的数据在同一个partition里面，那么首先我们需要得到table的startkeys
  * 再根据startKey建立一个分区器
  * 分区器有两个关键的方法需要去实现
  * 1. numPartitions 多少个分区
  * 2. getPartition给一个key，返回其应该在的分区  分区器如下：
  *
  * 参数splits为table的startKeys
  * 参数numFilesPerRegion为一个region想要生成多少个hfile，便于理解  先将其设置为1 即一个region生成一个hfile
  * h可以理解为它在这个region中的第几个hfile（当需要一个region有多个hfile的时候）
  * 因为startKeys是递增的，所以找到第一个大于key的region，那么其上一个region，这是这个key所在的region
  *
  * @param conf              配置属性
  * @param splits            table的startKeys
  * @param numFilesPerRegion 一个region想要生成多少个hfile
  */
private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
  val fraction: Int = 1 max numFilesPerRegion min 128

  override def getPartition(key: Any): Int = {
    def bytes(n: Any) = n match {
      case s: String => Bytes.toBytes(s)
      case s: Long => Bytes.toBytes(s)
      case s: Int => Bytes.toBytes(s)
    }

    val h = (key.hashCode() & Int.MaxValue) % fraction
    for (i <- 1 until splits.length)
      if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h

    (splits.length - 1) * fraction + h
  }

  override def numPartitions: Int = splits.length * fraction
}

