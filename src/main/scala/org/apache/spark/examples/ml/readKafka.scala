package org.apache.spark.examples.ml

import com.alibaba.fastjson.JSON
import javaUtils._
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object readKafka {

  // spark 参数设置
  val conf: SparkConf = new SparkConf()
    .setAppName("Streaming GBTLogisticRegression Classifier")
    .setMaster("local[2]")
  conf.set("spark.streaming.backpressure.enabled", "true")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(3))
  ssc.checkpoint("_checkpoint")
  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()


  // 配置数据, 需要取的用户实时偏好
  val userPrefAlias:Map[String,String]=  Map (
    "旅游结婚" -> "featpref_1", "行摄旅拍" ->"featpref_2", "悠悠古镇" ->"featpref_3", "玩转都市" -> "featpref_4",
    "自然风光" ->"featpref_5", "扫货圣地" ->"featpref_6", "吃货必逛" -> "featpref_7", "海滨度假" ->"featpref_8",
    "户外运动" ->"featpref_9", "名胜人文" -> "featpref_10", "养生之旅" ->"featpref_11", "探索猎奇" ->"featpref_12",
    "休闲娱乐" -> "featpref_13", "亲子同行"->"featpref_14", "畅玩乐园"->"featpref_15"
  )

  val featsSelect = List("旅游结婚","行摄旅拍","悠悠古镇","玩转都市","自然风光","扫货圣地","吃货必逛","海滨度假","户外运动",
    "名胜人文","养生之旅","探索猎奇","休闲娱乐","亲子同行","畅玩乐园")
  val catesSelect = List("跟团游","自助游","自驾游","火车票","酒店","门票")

  // 引入读取HBase函数的类
  val readhbase =  new readHBase()

  // 引入自定义Utils
  val gbtlrUtils = new GBTLRUtils()

  // 数据框DataFrame格式
  val schema: StructType = new createSchema().getSchema
  //  schema.printTreeString()  // 打印数据框结构图

  // 加载模型


  /**
    * 输入visitor_trace,从redis获取实时偏好数据
    * @param visitor_trace
    * @param redisClient
    * @return prefMap 用户的实施特征
    */
  def getUserPref(visitor_trace: String,redisClient:Jedis):Map[String,Double]= {

    val prefMap = mutable.Map[String,Double]("旅游结婚"->0,"行摄旅拍"->0,"悠悠古镇"->0,"玩转都市"->0,
      "自然风光" -> 0,"扫货圣地" -> 0, "吃货必逛" -> 0,"海滨度假" -> 0,"户外运动"->0,"名胜人文" -> 0,"养生之旅" -> 0,
      "探索猎奇" -> 0, "休闲娱乐" -> 0,"亲子同行" -> 0,"畅玩乐园"->0,"跟团游"->0,"自助游"->0,"自驾游"->0,
      "火车票"->0,"酒店"->0, "门票"->0,"valumax"->0)

    val data  = redisClient.get("feat_" + visitor_trace)
    // 构建初始数据
    if(!(data == null)){
      val parseData = JSON.parseObject(data,classOf[UserFeatures])
      val uniqueId = parseData.getUniqueId
      val feats = parseData.getFeats
      var poiMax:Double = 0.0

      for (i <-Array.range(0, feats.size())) {
        val subfeat = feats.get(i)
        val cate = subfeat.getCate
        val feat = subfeat.getFeat
        val sc = subfeat.getSc

        // 取theme主题下前台展示的偏好 valf
        if (cate == "THEME" &&  featsSelect.contains(feat)) {
          prefMap(feat) = subfeat.getValf
        }
        // 取最大POI偏好 valu
        else if (cate == "DEST"){
          if (subfeat.getValu>poiMax) poiMax=subfeat.getValu
        }
        // 取品类偏好 valf
        else if (cate == "PRODUCTTYPE" && sc == "PRODUCT_TYPE1" && catesSelect.contains(feat)){
          prefMap(feat) = subfeat.getValf
        }
      }
      prefMap("valumax")= poiMax
      prefMap.toMap
    }
    else {
      prefMap.toMap
    }

  }


  /**
    *
    * @param visitor_trace
    * @param redisClient
    * @return 推荐列表
    */

  def getRecPois(visitor_trace: String, redisClient: Jedis):ArrayBuffer[String] = {
    val data  = redisClient.get("rec_poi2_"+visitor_trace)
    val recPois = new ArrayBuffer[String]()
    if (!(data == null)){
      val parseData = JSON.parseObject(data,classOf[UserPois]).getPois
      for (i <- Array.range(0, parseData.size())){
        val poi = parseData.get(i).getPoiid.toString
        recPois += poi
    }
      recPois
    }
      recPois
  }

  /**
    * 将实时特征与离线特征组合起来
    * @param vt   visitor_trace
    * @param refStr visitor_trace的实时偏好
    * @param recPois  推荐列表
    * @param poiTab   poi离线HBase表
    * @param vtPoiTab  vtPoi离线HBase表
    * @return
    */

  def UnionPoiFeat(vt: String, refStr:Map[String,Any],
                   recPois: Array[String], poiTab: Table, vtPoiTab: Table) = {
      println("推荐列表：" + recPois.mkString(","))

      val unionPoiFeat = recPois map { poi =>
        // 根据poi，产生poi离线数据
        val poiOffFeats = readhbase.getPoiFeats(poi,poiTab)

        // 根据vt、poi，从vtPoiTab中取vt/poi的交互离线特征
        val poiRealFeats = readhbase.getUserPoiFeat(vt,poi,vtPoiTab)

        // 联立特征
        gbtlrUtils.unionData(refStr, poiOffFeats)
      }
      unionPoiFeat.toArray
  }

  def main(args: Array[String]): Unit = {

    // redis服务器
    val redisHost = "hx-dc-master.redis.tuniu.org"
    val redisPort = 26000

    // kafka 服务器
    val brokers = "flume001.tuniu.org:9092,flume002.tuniu.org:9092,flume003.tuniu.org:9092,flume004.tuniu.org:9092"
    val topics = "ta_parser"
    // Specify the kafka Broker Options
    val kafkaParameters = Map[String, String]("metadata.broker.list" -> brokers,
    "group.id"-> "rs_zk")
    val topicSet = topics.split(",").toSet

    // 获取kafka消息队列，过滤不符合要求的值
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParameters, topicSet).map(_._2)
      .filter{ x=>
        val vt = JSON.parseObject(x).get("visitor_trace").toString
        // 某些
        val clientType = Option(JSON.parseObject(x).get("client_type")).getOrElse("20")
        val os = JSON.parseObject(x).get("operating_system").toString
        println(clientType)
        val ctArray = List("10","11","12","13","14","15","20","21","22","24","25")
        val check = (vt != "00000000-0000-0000-0000-000000000000" && vt != "0" && vt!= "") && ctArray.contains(clientType) && os!="Win7"
        check
      }


    /**
      * 根据visitor_trace,获取实时偏好、poi推荐列表;
      * 实时特征存放在Redis中
      * 再根据推荐列表获取poi的离线特征，用户与Poi的离线交互特征
      * 离线特征存放在HBase表中
      *
      */
    val prefDataMap = messages.mapPartitions { partition =>

      // 每个分区建立redis链接，因为Redis链接不能序列化;
      val r = new Jedis(redisHost, redisPort)

      // HBase 链接同样需要序列化
      // HBase 服务器
      val hBaseConf = HBaseConfiguration.create()
      hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      hBaseConf.set("hbase.zookeeper.quorum", "hadoop0007.tuniu.org,hadoop0008.tuniu.org")
      val conn =  ConnectionFactory.createConnection(hBaseConf)
      // poi表
      val poiTab = conn.getTable(TableName.valueOf("rs_recommend_rank_poi"))
      // vtPoi表
      val vtPoiTab = conn.getTable(TableName.valueOf("rs_recommend_rank_vtpoi"))

      val res = partition.map { x =>
        val vt = JSON.parseObject(x).get("visitor_trace").toString
        val bookCity = JSON.parseObject(x).get("bookcity_code").toString
        val os = JSON.parseObject(x).get("operating_system").toString
        println("Visitor_trace:" + vt)

        // 读取visitor_trace偏好
        val refStr = getUserPref(vt, r)
        println("用户实时偏好 :" + refStr)

        // 读取推荐列表
        val recPois = getRecPois(vt, r).toArray

        // 如果poi列表不为空，对列表排序；否则返回原来的列表
        if (recPois.length > 0){

          UnionPoiFeat(vt,refStr,recPois,poiTab,vtPoiTab)
        } else recPois
    }
      // 最终的特征
      res
    }.print(10)

    ssc.start
    ssc.awaitTermination
  }
}
