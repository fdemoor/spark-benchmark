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

    val gtripData = datasetLoader.loadFromQuery("(select t.duration, g.gdistm, g.gduration from (   select stscode, endscode   from $SCHEMA.tripdata2017   where stscode<>endscode   group by stscode, endscode   having count(*) >= 50 )s, $SCHEMA.tripdata2017 t, $SCHEMA.gmdata2017 g where t.stscode = s.stscode   and t.endscode = s.endscode   and t.stscode = g.stscode   and t.endscode = g.endscode) as g")
      .withColumn("gdistm", col("gdistm").cast("double"))
    gtripData.cache()
    gtripData.foreach(Unit => ())
    time.tick(1)
    gtripData.foreach(Unit => ())
    time.tick(-1)

    val guniqueTripDist = gtripData.select("gdistm").distinct.sort(asc("gdistm"))
    val gsplitTripDist = guniqueTripDist.randomSplit(Array(1, 2), 42)
    val gtestTripDist = gsplitTripDist(0)
    val gtrainTripDist = gsplitTripDist(1)
    var gtrainData = gtripData.select("gdistm", "duration")
      .join(gtrainTripDist, usingColumns=Seq("gdistm"))

    val gmaxdist = guniqueTripDist.agg(max(col("gdistm"))).head().getDouble(0)
    val gmaxduration = gtripData.agg(max(col("duration"))).head().getInt(0)
    gtrainData = gtrainData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")

    val gtrainDataSet = gtrainData.select("gdistm").withColumn("x0", lit(1)).select("x0", "gdistm")
    val gtrainDataSetDuration = gtrainData.select("duration")
    var gparams = Seq(1.0).toDF("a").withColumn("b", lit(1.0))

    def dataframeToMatrix(df: Dataset[Row]) : BlockMatrix = {
      val assembler = new VectorAssembler().setInputCols(df.columns).setOutputCol("vector")
      val df2 = assembler.transform(df)
      return new IndexedRowMatrix(df2.select("vector").rdd.map{
        case Row(v: Vector) => Vectors.fromML(v)
      }.zipWithIndex.map { case (v, i) => IndexedRow(i, v) }).toBlockMatrix()
    }

    time.tick(0)
    val gtrainDataSetMat = dataframeToMatrix(gtrainDataSet)
    gtrainDataSetMat.cache()
    Utils.eval(spark, gtrainDataSetMat)
    time.tick(1)
    Utils.eval(spark, gtrainDataSetMat)
    time.tick(-1)

    var gparamsMat = dataframeToMatrix(gparams)
    var gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)

    def squaredErr(actual: BlockMatrix, predicted: BlockMatrix) : Double = {
      var s: Double = 0
      val it = actual.subtract(predicted).toLocalMatrix().rowIter
      while (it.hasNext) {
        s += scala.math.pow(it.next.apply(0), 2)
      }
      return s / (2 * actual.numRows())
    }

    time.tick(0)
    val gtrainDataSetDurationMat = dataframeToMatrix(gtrainDataSetDuration)
    gtrainDataSetDurationMat.cache()
    Utils.eval(spark, gtrainDataSetDurationMat)
    time.tick(1)
    Utils.eval(spark, gtrainDataSetDurationMat)
    time.tick(-1)

    var gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
    println(gsqerr)

    def gradDesc(actual: BlockMatrix, predicted: BlockMatrix,
                 indata: BlockMatrix) : Seq[Double] = {
      val m = predicted.subtract(actual).transpose.multiply(indata).toLocalMatrix()
      val n = actual.numRows()
      return Seq(m.apply(0, 0) / n, m.apply(0, 1) / n)
    }

    val alpha = 0.1

    var gupdate = gradDesc(gtrainDataSetDurationMat, gpred, gtrainDataSetMat)
    gparams = gparams.select(col("a") - alpha * gupdate(0) as "a",
      col("b") - alpha * gupdate(1) as "b")
    gparams.show(1)

    gparamsMat = dataframeToMatrix(gparams)
    gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
    gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
    println(gsqerr)

    time.tick(0)
    for (i <- 0 to 999) {
      val gparamsMat = dataframeToMatrix(gparams)
      gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
      val gupdate = gradDesc(gtrainDataSetDurationMat, gpred, gtrainDataSetMat)
      gparams = gparams.select(col("a") - alpha * gupdate(0) as "a",
        col("b") - alpha * gupdate(1) as "b")
      if ((i+1)%100 == 0) {
        println(s"Error rate after ${i+1} iterations is ${squaredErr(gtrainDataSetDurationMat, gpred)}")
      }
    }

    gparams.show(1)
    gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
    println(gsqerr)

    time.tick(2)

    var gtestData = gtripData.select("gdistm", "duration")
      .join(gtestTripDist, usingColumns=Seq("gdistm"))
    gtestData = gtestData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")
    val gtestDataSet = gtestData.select("gdistm").withColumn("x0", lit(1)).select("x0", "gdistm")
    val gtestDataSetDuration = gtestData.select("duration")

    time.tick(3)
    val gtestDataSetDurationMat = dataframeToMatrix(gtestDataSetDuration)
    gtestDataSetDurationMat.cache()
    Utils.eval(spark, gtestDataSetDurationMat)
    time.tick(1)
    Utils.eval(spark, gtestDataSetDurationMat)
    time.tick(-1)
    val gtestDataSetMat = dataframeToMatrix(gtestDataSet)
    gtestDataSetMat.cache()
    Utils.eval(spark, gtestDataSetMat)
    time.tick(1)
    Utils.eval(spark, gtestDataSetMat)
    time.tick(-1)
    gparamsMat = dataframeToMatrix(gparams)
    val gtestpred = gtestDataSetMat.multiply(gparamsMat.transpose)

    val gdurationMat = dataframeToMatrix(Seq(gmaxduration).toDF("duration"))
    val gtestsqerr1 = squaredErr(gtestDataSetDurationMat.multiply(gdurationMat), gtestpred.multiply(gdurationMat))
    println(gtestsqerr1)

    val gdurationDataMat = dataframeToMatrix(gtripData.join(gtestTripDist, usingColumns=Seq("gdistm")).select("gduration"))
    val gtestsqerr2 = squaredErr(gtestDataSetDurationMat.multiply(gdurationMat), gdurationDataMat)
    println(gtestsqerr2)
    time.tick(3)

    time.log()

  }
}
