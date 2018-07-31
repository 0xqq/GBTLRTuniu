package org.apache.spark.examples.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.gbtlr.GBTLRClassifier
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object GBDTLRTuniu {
  /**
    * 读取数据
    * @param spark
    * @param filename
    * @return
    */
  def dataRead(spark: SparkContext,filename: String):Array[RDD[Row]] = {
    //val warehouse_location = "/user/zhangkun6/"
    val file = spark.textFile(filename)
    file.take(10).foreach(println)
    val header = file.first()
    val ratings = file.filter(x => x != header).map(_.split("\t")).map(x=>{
      val click: Int = x(0).toInt
      val os:String = x(1)
      val newuser:Int = x(2).toInt
      val ifbrowsed:Int = x(3).toInt
      val user_pv_total:Int = x(4).toInt
      val user_pv_1:Int = x(5).toInt
      val user_pv_2:Int = x(6).toInt
      val user_pv_3:Int = x(7).toInt
      val user_pv_4:Int = x(8).toInt
      val user_pv_5:Int = x(9).toInt
      val user_pv_6:Int = x(10).toInt
      val user_pv_7:Int = x(11).toInt
      val featpref_1:Double = x(12).toDouble
      val featpref_2:Double = x(13).toDouble
      val featpref_3:Double = x(14).toDouble
      val featpref_4:Double = x(15).toDouble
      val featpref_5:Double = x(16).toDouble
      val featpref_6:Double = x(17).toDouble
      val featpref_7:Double = x(18).toDouble
      val featpref_8:Double = x(19).toDouble
      val featpref_9:Double = x(20).toDouble
      val featpref_10:Double = x(21).toDouble
      val featpref_11:Double = x(22).toDouble
      val featpref_12:Double = x(23).toDouble
      val featpref_13:Double = x(24).toDouble
      val featpref_14:Double = x(25).toDouble
      val featpref_15:Double = x(26).toDouble
      val sum_pv_1:Int = x(27).toInt
      val sum_pv_2:Int = x(28).toInt
      val sum_pv_3:Int = x(29).toInt
      val sum_pv_4:Int = x(30).toInt
      val sum_pv_5:Int = x(31).toInt
      val sum_pv_6:Int = x(32).toInt
      val sum_pv_7:Int = x(33).toInt
      val poi_price:Double = x(34).toDouble
      val poi_outdays:Double = x(35).toDouble
      val user_locprovince:String = x(36)
      val user_locregion:String = x(37)
      val user_loccountry:String = x(38)
      val poi_locprovince:String = x(39)
      val poi_locregion:String = x(40)
      val poi_loccountry:String = x(41)
      val samecity:Int = x(42).toInt
      val sameprovince:Int = x(43).toInt
      val sameregion:Int = x(44).toInt
      val samecountry:Int = x(45).toInt
      val catepref_1: Double = x(46).toDouble
      val catepref_2: Double = x(47).toDouble
      val catepref_3: Double = x(48).toDouble
      val catepref_4: Double = x(49).toDouble
      val catepref_5: Double = x(50).toDouble
      val catepref_6: Double = x(51).toDouble
//      val valumax_n:Double = x(52).toDouble
//      val valumax_i:Double = x(53).toDouble
      val valumax_all:Double = x(52).toDouble
      val total_person_num:Int =x(53).toInt
      val last_person_num:Int = x(54).toInt
      val last_month_person_num:Int = x(55).toInt
      val last_year_person_num:Int = x(56).toInt
      val poi_level:String = x(57)
      val idts:Double = x(58).toDouble
      val poipref_1:Double = x(59).toDouble
      val poipref_2:Double = x(60).toDouble
      val poipref_3:Double = x(61).toDouble
      val poipref_4:Double = x(62).toDouble
      val poipref_5:Double = x(63).toDouble
      val poipref_6:Double = x(64).toDouble
      val poipref_7:Double = x(65).toDouble
      val poipref_8:Double = x(66).toDouble
      val poipref_9:Double = x(67).toDouble
      val poipref_10:Double = x(68).toDouble
      val poipref_11:Double = x(69).toDouble
      val itf_poi_price:Double = x(70).toDouble
      val itf_bookcitypoi_price:Double = x(71).toDouble
      val itf_bookcitypoi_num:Int = x(72).toInt

      Row(click,os,newuser,ifbrowsed,user_pv_total,user_pv_1,user_pv_2,user_pv_3,
        user_pv_4,user_pv_5,user_pv_6,user_pv_7,featpref_1,featpref_2,featpref_3,featpref_4,featpref_5,featpref_6,
        featpref_7,featpref_8,featpref_9,featpref_10,featpref_11,featpref_12,featpref_13,featpref_14,featpref_15,
        sum_pv_1,sum_pv_2,sum_pv_3,sum_pv_4,sum_pv_5,sum_pv_6,sum_pv_7,poi_price,poi_outdays,user_locprovince,
        user_locregion,user_loccountry,poi_locprovince,poi_locregion,poi_loccountry,samecity,sameprovince,
        sameregion,samecountry,catepref_1,catepref_2,catepref_3,catepref_4,catepref_5,catepref_6,valumax_all,
        total_person_num,last_person_num,last_month_person_num,last_year_person_num,poi_level,idts,poipref_1,poipref_2,
        poipref_3,poipref_4,poipref_5,poipref_6,poipref_7,poipref_8,poipref_9,poipref_10,poipref_11,itf_poi_price,
        itf_bookcitypoi_price,itf_bookcitypoi_num)
    })
    ratings.take(10).foreach(println)
    val Array(data0,data1) = ratings.randomSplit(Array(0.8,0.2))
    Array(data0,data1)
  }


  /**
    *构建数据框结构, 与读取的数据结构保持一致
    * @return  schema
    */


  /**
    * pipeline： 包含数据预处理和模型训练
    * @param train  训练样本
    * @return pipeline
    */
  def dataProcessPipeline(train:DataFrame):Unit ={
    train.persist(StorageLevel.MEMORY_AND_DISK)
    val numTrain = train.count()
    //  =============== 处理数值变量 ====================
    // 暂时不对数据变量处理
    println("="*50+"处理数值变量"+"="*50)
    val numColnames = Array("sum_pv_1","sum_pv_2","sum_pv_3","sum_pv_4","sum_pv_5","sum_pv_6",
      "sum_pv_7","poi_outdays","poi_price","user_pv_1","user_pv_2","user_pv_3","user_pv_4")


    // ================ 处理分类变量 ====================
    println("="*50+"处理分类变量"+"="*50)
    val catColnames = Array("os","user_locregion","poi_locregion","user_locprovince","poi_locprovince","poi_level")
    val indexers = catColnames.map(name => new StringIndexer()
      .setInputCol(name).setOutputCol(s"${name}_idx").setHandleInvalid("skip"))
    val encoders = catColnames.map(name=> new OneHotEncoder()
      .setInputCol(s"${name}_idx").setOutputCol(s"${name}_enc"))
    val assembler = new VectorAssembler()
    // 建树前不独热化
//    assembler.setInputCols(Array("user_locProvince_idx", "user_locProvince_idx", "user_pv_1",
//      "user_pv_2", "user_pv_3", "user_pv_4", "user_pv_5",
//      "user_pv_6","user_pv_7", "user_pv_8", "user_pv_9","user_pv_10",
//      "sum_pv_1","sum_pv_2","sum_pv_3","sum_pv_4","sum_pv_5","sum_pv_6","sum_pv_7",
//      "sum_pv_1","sum_pv_2","sum_pv_3","sum_pv_4","sum_pv_5","sum_pv_6","sum_pv_7",
//      "sum_pv_8","sum_pv_9","sum_pv_10","poi_price","poi_outdays"))
    // 建树前独热化
    assembler.setInputCols(Array("os_idx","newuser","ifbrowsed","user_pv_total","user_pv_1","user_pv_2","user_pv_3",
      "user_pv_4","user_pv_5","user_pv_6","user_pv_7","featpref_1","featpref_2","featpref_3","featpref_4","featpref_5",
      "featpref_6", "featpref_7","featpref_8","featpref_9","featpref_10","featpref_11","featpref_12","featpref_13",
      "featpref_14","featpref_15", "sum_pv_1","sum_pv_2","sum_pv_3","sum_pv_4","sum_pv_5","sum_pv_6","sum_pv_7",
      "poi_price","poi_outdays", "user_locregion_enc","user_locprovince_enc", "poi_locregion_enc","poi_locprovince_enc",
      "samecity", "sameprovince","sameregion","samecountry","catepref_1","catepref_2","catepref_3",
      "catepref_4","catepref_5","catepref_6","valumax_all","poi_level_enc",
      "total_person_num","last_person_num","last_month_person_num","last_year_person_num","idts","poipref_1","poipref_2",
      "poipref_3","poipref_4","poipref_5","poipref_6","poipref_7","poipref_8","poipref_9","poipref_10","poipref_11",
    "itf_poi_price","itf_bookcitypoi_price","itf_bookcitypoi_num"))
    assembler.setOutputCol("features")
    val gbtlrModel = new GBTLRClassifier()
      .setFeaturesCol("features")   //col($(featuresCol))
      .setLabelCol("click")   //col($(labelCol))
      .setGBTMaxIter(50)    // 在集群上训练时候可能会有RDD超过2G的报错，此时迭代次数和深度设小点
      .setMaxDepth(6)
      .setLRMaxIter(800)
      .setRegParam(0.01)
      .setStandardization(true)
      .setElasticNetParam(0.5)
      .setAggregationDepth(100)   // 当特征维度比较高的时候，该参数应当设大点，训练速度比较快

//    val LRmodel = new LogisticRegression().setMaxIter(50).setRegParam(0.1).setElasticNetParam(0.5)
//      .setFeaturesCol("features").setLabelCol("y_idx")

    // 建树前独热化
    println("*"*50+"开始训练"+"*"*50)
    val pipeline = new Pipeline().setStages(indexers ++ encoders  :+ assembler :+ gbtlrModel)
      .fit(train)

//     建树前不独热化
//    val pipeline = new Pipeline().setStages(indexers :+ assembler :+ gbtlrModel).fit(train)
//    println("************train data************************")
//
//    // 训练集预测
//    val newdata = pipeline.transform(train)
//    newdata.show()

//    val labelIndex = new StringIndexer().setInputCol("label").setOutputCol("label_index")
//    val jobIndex = new StringIndexer().setInputCol("job").setOutputCol("job_index")
//    val maritalIndex = new StringIndexer().setInputCol("marital").setOutputCol("marital_index")
//    val educationIndex = new StringIndexer().setInputCol("education").setOutputCol("education_index")
//    val defaultIndex = new StringIndexer().setInputCol("default").setOutputCol("default_Index")
//    val housingIndex = new StringIndexer().setInputCol("housing").setOutputCol("housing_Index")
//    val loanIndex = new StringIndexer().setInputCol("loan").setOutputCol("loan_index")
//    val monthIndex = new StringIndexer().setInputCol("month").setOutputCol("month_index")
//    val pipeline = new Pipeline().setStages(Array(labelIndex,jobIndex,maritalIndex,educationIndex,defaultIndex
//    ,housingIndex,loanIndex,monthIndex)).fit(traing)

    pipeline.write.overwrite().save("/user/zhangkun6/gbtlr/pipeline")
    println("*"*50+"训练完成"+"*"*50)
    train.unpersist()
  }

  /**
    * 计算AUC
    * @param data  预测dataframe
    * @param labelCol  标签列，真实值
    * @param predCol   预测列，预测值: 0/1 或者概率
    * @return
    */
  def AUCEvaluation(data:DataFrame,labelCol:String,predCol:String):Double = {
    val metrics = new  BinaryClassificationEvaluator()
      .setLabelCol(labelCol)
      .setRawPredictionCol(predCol)
      .setMetricName("areaUnderROC")
    metrics.evaluate(data)
  }

  /**
    * 阈值：prediction > threshold 为正样本
    * 计算不同阈值下，决策指标，包括 precision、recall、F1 measure
    * @param dataset
    * @param labelCol  真实值 取值为0 1
    * @param predCol   预测值  取值为 0 1
    * @return
    */
  def decisionEvaluation(dataset: Dataset[_], labelCol:String, predCol:String):Unit = {
    val scoreAndLabels = dataset.select(col(predCol), col(labelCol).cast(DoubleType)).rdd.map {
      case Row(rawPrediction: Vector, label: Double) => (rawPrediction(1), label)
      case Row(rawPrediction: Double, label: Double) => (rawPrediction, label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // F-measure
    val f1Score = metrics.fMeasureByThreshold(1)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName(s"GBTLRTuniu Recommendation Rank")
    val sc = new SparkContext(conf)
    // 设置checkpoint存储目录，防止迭代次数过多导致stackOverFlow
//    sc.setCheckpointDir("/user/zhangkun6/gbtlr/checkpoint")
    val warehouse_location = "/user/zhangkun6/warehouse"
    val spark = SparkSession.builder
      .appName(" Spark SQL Hive integration ")
      .config("spark.sql.warehouse.dir", warehouse_location)
      .enableHiveSupport()
      .getOrCreate()

//    val conf = new SparkConf().setAppName("GBTLRTuniu").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val spark = SparkSession
//      .builder()
//      .getOrCreate()

    // 得到训练集和测试集
    val Array(traing, test) = dataRead(sc,"/user/zhangkun6/warehouse/rank.csv")
    traing.cache()
    traing.take(10).foreach(println)
    test.cache()
    test.take(10).foreach(println)

    //构建DataFrame样式数据
    val schema = new createSchema().getSchema
    schema.printTreeString()
    val dfTrain: DataFrame = spark.createDataFrame(traing,schema)
    val dfTest: DataFrame = spark.createDataFrame(test,schema)
    traing.unpersist()
    test.unpersist()
    dfTrain.show()

    // 训练集预处理、 训练，构建pipeline
    dataProcessPipeline(dfTrain)
    val ppl = PipelineModel.load("/user/zhangkun6/gbtlr/pipeline")


    // pipeline 拟合测试数据，包含数据预处理和预测
    val dfTrainP = ppl.transform(dfTrain)
    val dfTestP = ppl.transform(dfTest)
    dfTestP.show(200)

    // AUC
    val auc_tr = AUCEvaluation(dfTrainP, labelCol = "click",predCol="rawPrediction")
    println(s"The AreaUnderROC of Train is: ${auc_tr}")
    val auc_te = AUCEvaluation(dfTestP, labelCol = "click",predCol="rawPrediction")
    println(s"The AreaUnderROC of Test is: ${auc_te}")

    // 不建议在训练模型时使用该方法；建议训练结束后，加载模型，判断评估最合适的阈值
//    decisionEvaluation(dfTestP, labelCol = "click",predCol="probability")

  }
}
