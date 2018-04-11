import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix

import org.apache.logging.log4j.scala.Logging

import net.sf.geographiclib.Geodesic

object BenchmarkLRBasic extends Logging {

  def run(spark: SparkSession, datasetLoader: DatasetLoader) {

    import spark.implicits._

    val time = new TimeProfiler("lr-basic")
    time.start()
    val tripdata2017 = datasetLoader.load("tripdata2017")
    time.tick()
    val stations2017 = datasetLoader.load("stations2017")
    time.tick()
    val freqStations = tripdata2017.filter(col("stscode") =!= col("endscode"))
      .groupBy("stscode", "endscode").agg(count("id").alias("numtrips"))
      .filter(col("numtrips") >= 50)
    time.tick()

    val gmdata2017 = datasetLoader.load("gmdata2017")
    time.tick()
    val gtripData = gmdata2017.join(tripdata2017, usingColumns=Seq("stscode", "endscode"))
      .join(freqStations, usingColumns=Seq("stscode", "endscode"))
      .select("id", "duration", "gdistm", "gduration")
      .withColumn("gdistm", col("gdistm").cast("double"))
    time.tick()

    val guniqueTripDist = gtripData.select("gdistm").distinct.sort(asc("gdistm"))
    time.tick()
    val gsplitTripDist = guniqueTripDist.randomSplit(Array(1, 2), 42)
    time.tick()
    val gtestTripDist = gsplitTripDist(0)
    time.tick()
    val gtrainTripDist = gsplitTripDist(1)
    time.tick()
    var gtrainData = gtripData.select("gdistm", "duration")
      .join(gtrainTripDist, usingColumns=Seq("gdistm"))
    time.tick()

    val gmaxdist = guniqueTripDist.agg(max(col("gdistm"))).head().getDouble(0)
    time.tick()
    val gmaxduration = gtripData.agg(max(col("duration"))).head().getInt(0)
    time.tick()
    gtrainData = gtrainData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")
    time.tick()

    val gtrainDataSet = gtrainData.select("gdistm").withColumn("x0", lit(1)).select("x0", "gdistm")
    time.tick()
    val gtrainDataSetDuration = gtrainData.select("duration")
    time.tick()
    var gparams = Seq(1.0).toDF("a").withColumn("b", lit(1.0))
    time.tick()

    def dataframeToMatrix(df: Dataset[Row]) : BlockMatrix = {
      val assembler = new VectorAssembler().setInputCols(df.columns).setOutputCol("vector")
      val df2 = assembler.transform(df)
      return new IndexedRowMatrix(df2.select("vector").rdd.map{
        case Row(v: Vector) => Vectors.fromML(v)
      }.zipWithIndex.map { case (v, i) => IndexedRow(i, v) }).toBlockMatrix()
    }
    time.tick()

    val gtrainDataSetMat = dataframeToMatrix(gtrainDataSet)
    time.tick()
    var gparamsMat = dataframeToMatrix(gparams)
    time.tick()
    var gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
    time.tick()

    def squaredErr(actual: BlockMatrix, predicted: BlockMatrix) : Double = {
      var s: Double = 0
      val it = actual.subtract(predicted).toLocalMatrix().rowIter
      while (it.hasNext) {
        s += scala.math.pow(it.next.apply(0), 2)
      }
      return s / (2 * actual.numRows())
    }
    time.tick()

    val gtrainDataSetDurationMat = dataframeToMatrix(gtrainDataSetDuration)
    time.tick()
    var gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
    time.tick()
    println(gsqerr)
    time.tick()

    def gradDesc(actual: BlockMatrix, predicted: BlockMatrix,
                 indata: BlockMatrix) : Seq[Double] = {
      val m = predicted.subtract(actual).transpose.multiply(indata).toLocalMatrix()
      val n = actual.numRows()
      return Seq(m.apply(0, 0) / n, m.apply(0, 1) / n)
    }
    time.tick()

    val alpha = 0.1
    time.tick()

    var gupdate = gradDesc(gtrainDataSetDurationMat, gpred, gtrainDataSetMat)
    time.tick()
    gparams = gparams.select(col("a") - alpha * gupdate(0) as "a",
      col("b") - alpha * gupdate(1) as "b")
    time.tick()
    gparams.show(1)
    time.tick()

    time.tick()
    gparamsMat = dataframeToMatrix(gparams)
    time.tick()
    gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
    time.tick()
    gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
    time.tick()
    println(gsqerr)
    time.tick()

    // Cache to speed-up since used in every iteration
    gtrainDataSetMat.cache()
    time.tick()
    gtrainDataSetDurationMat.cache()
    time.tick()

    for (i <- 0 to 999) {
      val gparamsMat = dataframeToMatrix(gparams)
      gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
      val gupdate = gradDesc(gtrainDataSetDurationMat, gpred, gtrainDataSetMat)
      gparams = gparams.select(col("a") - alpha * gupdate(0) as "a",
        col("b") - alpha * gupdate(1) as "b")
      if ((i+1)%100 == 0) {
        println(s"Error rate after ${i+1} iterations is ${squaredErr(gtrainDataSetDurationMat, gpred)}")
      }
      time.tick()
    }

    gparams.show(1)
    time.tick()
    gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
    time.tick()
    println(gsqerr)
    time.tick()

    var gtestData = gtripData.select("gdistm", "duration")
      .join(gtestTripDist, usingColumns=Seq("gdistm"))
    time.tick()
    gtestData = gtestData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")
    time.tick()
    val gtestDataSet = gtestData.select("gdistm").withColumn("x0", lit(1)).select("x0", "gdistm")
    time.tick()
    val gtestDataSetDuration = gtestData.select("duration")
    time.tick()

    time.tick()
    val gtestDataSetDurationMat = dataframeToMatrix(gtestDataSetDuration)
    time.tick()
    val gtestDataSetMat = dataframeToMatrix(gtestDataSet)
    time.tick()
    gparamsMat = dataframeToMatrix(gparams)
    time.tick()
    val gtestpred = gtestDataSetMat.multiply(gparamsMat.transpose)
    time.tick()

    val gdurationMat = dataframeToMatrix(Seq(gmaxduration).toDF("duration"))
    time.tick()
    val gtestsqerr1 = squaredErr(gtestDataSetDurationMat.multiply(gdurationMat), gtestpred.multiply(gdurationMat))
    time.tick()
    println(gtestsqerr1)
    time.tick()

    val gdurationDataMat = dataframeToMatrix(gtripData.join(gtestTripDist, usingColumns=Seq("gdistm")).select("gduration"))
    time.tick()
    val gtestsqerr2 = squaredErr(gtestDataSetDurationMat.multiply(gdurationMat), gdurationDataMat)
    time.tick()
    println(gtestsqerr2)
    time.tick()

    time.log()

  }
}
