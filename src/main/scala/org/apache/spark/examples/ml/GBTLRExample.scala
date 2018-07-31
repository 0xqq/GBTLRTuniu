package org.apache.spark.examples.ml


import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.gbtlr.{GBTLRClassificationModel, GBTLRClassifier}

// scalastyle:off println


object GBTLRExample {
  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)
    val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("gbtlr example")
        .getOrCreate()

    import spark.implicits._

    val startTime = System.currentTimeMillis()

    val dataset = spark.read.option("header", "true").option("inferSchema", "true")
        .option("delimiter", ";").csv("data/bank/bank-full.csv")

    val columnNames = Array("job", "marital", "education",
      "default", "housing", "loan", "contact", "month", "poutcome", "y")

    // stringIndexer 将字符转为数字
    val indexers = columnNames.map(name => new StringIndexer()
        .setInputCol(name).setOutputCol(name + "_index"))
    val pipeline = new Pipeline().setStages(indexers)
    val data1 = pipeline.fit(dataset).transform(dataset)
    val data2 = data1.withColumnRenamed("y_index", "label1")

    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("age", "job_index", "marital_index",
      "education_index", "default_index", "balance", "housing_index",
      "loan_index", "contact_index", "day", "month_index", "duration",
      "campaign", "pdays", "previous", "poutcome_index"))
    assembler.setOutputCol("features")

    val data3 = assembler.transform(data2)
    val data4 = data3.randomSplit(Array(4, 1))
    data4(0).show()
    // features的schema
    print(AttributeGroup.fromStructField(data4(0).schema("features")))
//    println("----------------------------------------------")
//    val feat = data4(0).select(col="features")
//    feat.rdd.map{x=>x.mkString(",")}.take(10).foreach(println)
//    println("----------------------------------------------")

    val gBTLRClassifier = new GBTLRClassifier()
        .setFeaturesCol("features")   //col($(featuresCol))
        .setLabelCol("label1")   //col($(labelCol))
        .setGBTMaxIter(30)
        .setMaxDepth(6)
        .setMaxBins(20)
        .setLRMaxIter(100)
        .setRegParam(0.01)
        .setElasticNetParam(0.5)

    val model = gBTLRClassifier.fit(data4(0))
    model.write.overwrite().save("D://spark-gbtlr-master/model")
    val model2 = GBTLRClassificationModel.load("D://spark-gbtlr-master/model")

    val results = model2.transform(data4(1))
    results.select("features","rawPrediction","probability","prediction","label1").show(200)

    val summary = model2.evaluate(data4(1))
    val endTime = System.currentTimeMillis()
    val auc = summary.binaryLogisticRegressionSummary
        .asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"Training and evaluating cost ${(endTime - startTime) / 1000} seconds")
    println(s"The model's auc: $auc")
  }
}

// scalastyle:on println
