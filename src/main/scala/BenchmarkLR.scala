import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import org.apache.logging.log4j.scala.Logging

import net.sf.geographiclib.Geodesic

object BenchmarkLR extends Logging {

  def trainAndTest(spark: SparkSession, data: Dataset[Row], feature: String,
                   label: String, numIter: Int, lblCompare: String = "") = {

    import spark.implicits._

    // Get all distinct values of feature and sort
    val uniqueDist = data.select(feature).distinct.sort(asc(feature))

    // Take a third as test set, the remaining as train set
    val splitDist = uniqueDist.randomSplit(Array(1, 2), 42)
    val testDist = splitDist(0)
    val trainDist = splitDist(1)

    //--------------------------------------------------------------------------
    // TRAINING
    //--------------------------------------------------------------------------

    // Join to get the full training data
    val trainData = data.select(feature, label)
      .join(trainDist, usingColumns=Seq(feature))

    val maxfeat = uniqueDist.agg(max(col(feature))).head().getDouble(0)
    val maxlbl = data.agg(max(col(label))).head().getInt(0)

    val trainDataNormalized = trainData.select(col(feature) / maxfeat as feature, col(label) / maxlbl as label)
    val trainDataSet = trainDataNormalized.select(feature).withColumn("x0", lit(1))
    val trainDataSetValid = trainDataNormalized.select(label)

    var params = Seq(1.0).toDF("a").withColumn("b", lit(1.0))
    val alpha = 0.1
    val trainDataSetValidMat = Utils.dataframeToMatrix(trainDataSetValid)
    val trainDataSetMat = Utils.dataframeToMatrix(trainDataSet)

    // Cache to speed-up since used in every iteration
    trainDataSetValidMat.cache()
    trainDataSetMat.cache()

    logger.trace(s"Starting training iterations (${numIter})")
    for (i <- 1 to numIter) {
      val paramsMat = Utils.dataframeToMatrix(params)
      val pred = trainDataSetMat.multiply(paramsMat.transpose)
      val update = Utils.gradDesc(trainDataSetValidMat, pred, trainDataSetMat)
      params = params.select(col("a") - alpha * update(0) as "a",
        col("b") - alpha * update(1) as "b")
      if (i % 5 == 0) {
        val sqerr = Utils.squaredErr(trainDataSetValidMat, pred)
        logger.trace(s"Error rate after ${i} iterations is ${sqerr}")
      }
    }

    logger.trace("Done with iterations")
    logger.trace(s"Params are a = ${params.head().getDouble(0)} and b = ${params.head().getDouble(1)}")

    //--------------------------------------------------------------------------
    // TESTING
    //--------------------------------------------------------------------------

    logger.trace("Now testing")
    val testData = data.select(feature, label)
      .join(testDist, usingColumns=Seq(feature))
    val testDataNormalized = testData.select(col(feature) / maxfeat as feature, col(label) / maxlbl as label)
    val testDataSet = testDataNormalized.select(feature).withColumn("x0", lit(1))
    val testDataSetValid = testDataNormalized.select(label)

    val testDataSetValidMat = Utils.dataframeToMatrix(testDataSetValid)
    val testDataSetMat = Utils.dataframeToMatrix(testDataSet)
    val paramsMat = Utils.dataframeToMatrix(params)
    val testpred = testDataSetMat.multiply(paramsMat.transpose)
    val testsqerr = Utils.squaredErr(testDataSetValidMat, testpred)
    logger.trace(s"Error on test set is ${testsqerr}")

    if (lblCompare != "") {
      val testDataCompare = data.join(testDist, usingColumns=Seq(feature))
        .select(col(lblCompare) / maxlbl as lblCompare)
      val testDataCompareMat = Utils.dataframeToMatrix(testDataCompare)
      val testsqerr2 = Utils.squaredErr(testDataSetValidMat, testDataCompareMat)
      logger.trace(s"Error with compare data is ${testsqerr2}, are we doing better?")
    }
  }

  def run(spark: SparkSession, datasetLoader: DatasetLoader, numIter: Int,
          gmap: Boolean) {

    logger.trace("Loading datasets from MonetDB")
    val stations = datasetLoader.load("stations2017")
    val tripdata = datasetLoader.load("tripdata2017")

    val freqStations = tripdata.filter(col("stscode") =!= col("endscode"))
      .groupBy("stscode", "endscode").agg(count("id").alias("numtrips"))
      .filter(col("numtrips") >= 50)

    val freqStationsCord = freqStations.join(stations, col("stscode") === col("scode"))
      .withColumnRenamed("slatitude", "stlat").withColumnRenamed("slongitude", "stlong")
      .drop("sispublic").drop("scode").drop("sname")
      .join(stations, col("endscode") === col("scode"))
      .withColumnRenamed("slatitude", "enlat").withColumnRenamed("slongitude", "enlong")
      .drop("sispublic").drop("scode").drop("sname")

    if (gmap) {

      logger.info("Using Google Maps data for distance")
      val gmdata = datasetLoader.load("gmdata2017")
      val gTripData = gmdata.join(tripdata, usingColumns=Seq("stscode", "endscode"))
        .join(freqStationsCord, usingColumns=Seq("stscode", "endscode"))
        .select("id", "duration", "distm", "durations")
        .withColumn("distm", col("distm").cast("double"))
        .withColumnRenamed("distm", "gdistm")
        .withColumnRenamed("durations", "gduration")

      trainAndTest(spark, gTripData, "gdistm", "duration", numIter, "gduration")

    } else {

      logger.info("Using Vincenty's formula and coordinates for distance")

      val geoDistance = (lat1: Double, lon1: Double, lat2: Double, lon2: Double)
        => Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2).s12

      val geoDistanceUDF = udf(geoDistance)
      val freqStationsDist = freqStationsCord.withColumn("vdistm",
          round(geoDistanceUDF(col("stlat"), col("stlong"), col("enlat"), col("enlong"))))

      // Join to get the station combinations where we have data
      val tripData = tripdata.join(freqStationsDist, usingColumns=Seq("stscode", "endscode"))
        .select("id", "duration", "vdistm")

      trainAndTest(spark, tripData, "vdistm", "duration", numIter)

    }
  }
}
