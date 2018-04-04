import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.scala.Logging

import scopt._

case class Config(benchmark: String = "join", ip: String = "localhost",
  port: String = "50000", username: String = "monetdb", password: String = "monetdb",
  database: String = "aida", schema: String = "test01", numIter: Int = 5,
  gmap: Boolean = false, kwargs: Map[String,String] = Map())

object BenchmarkApp extends Logging {

  private final val nbLoadExec = 5
  private final val nbLoadWarmupExec = 2
  private final val nbMultExec = 100
  private final val nbMultWarmupExec = 2
  private var datasetLoader: DatasetLoader = null

  private def loadBenchmark(spark: SparkSession) = {
    logger.info("Starting load benchmark")
    val results = new BenchmarkResult("load")
    val kValues = Seq(1, 10, 100, 1000, 10000, 100000)
    for (k <- kValues) {
      for (i <- 1 to nbLoadWarmupExec + nbLoadExec) {
        val dt = Utils.time {
          val df = datasetLoader.load("trand100x" + k.toString + "r")
          val mat = Utils.dataframeToMatrix(df)
        }
        if (i > nbLoadWarmupExec) {
          results.addResult(k, dt)
        }
      }
    }
    results.log()
    logger.info("Done with load benchmark")
  }

  private def multBenchmark(spark: SparkSession, right: String) = {
    logger.info("Starting mult benchmark")
    val results = new BenchmarkResult("mult" + right)
    val kValues = Seq(1, 10, 100, 1000)

    val dfR = datasetLoader.load("trand100x" + right + "r")
    val matR = Utils.dataframeToMatrix(dfR).transpose

    for (k <- kValues) {
      val dfL = datasetLoader.load("trand100x" + k.toString + "r")
      val matL = Utils.dataframeToMatrix(dfL)
      for (i <- 1 to nbMultWarmupExec) {
        val mat = matL.multiply(matR)
      }
      val dt = Utils.time {
        for (i <- 1 to nbMultExec) {
          matL.multiply(matR)
        }
      }
      results.addResult(k, dt)
    }
    results.log()
    logger.info("Done with mult benchmark")
  }

  private def joinBenchmark(spark: SparkSession) = {
    logger.info("Starting join benchmark")
    val results = new BenchmarkResult("join")
    val resultsCard = new BenchmarkResult("joinCard")
    val jValues = 1 to 11 toSeq

    val df1 = datasetLoader.load("tnrand10x1000000r")
    df1.createOrReplaceTempView("df1")
    var df2 = datasetLoader.load("t2nrand10x1000000r")
    df2.createOrReplaceTempView("df2")

    // Warm up
    spark.sql("SELECT * FROM df1")
    spark.sql("SELECT * FROM df2")

    for (i <- 0 to 10) {
      val joinExprs = (0 to i toSeq).map(j
        => df1("c" + j.toString) === df2("b" + j.toString)).reduce(_ && _)

      val dt = Utils.time {
        df1.join(df2, joinExprs)
      }
      results.addResult(i + 1, dt)

      val df = df1.join(df2, joinExprs)
      resultsCard.addResult(i + 1, df.count())
    }
    results.log()
    resultsCard.log()
    logger.info("Done with join benchmark")
  }

  private def joinSQLBenchmark(spark: SparkSession) = {
    logger.info("Starting joinSQL benchmark")
    val results = new BenchmarkResult("joinSQL")
    val resultsCard = new BenchmarkResult("joinSQLCard")
    val jValues = 1 to 11 toSeq

    val df1 = datasetLoader.load("tnrand10x1000000r")
    df1.createOrReplaceTempView("df1")
    val df2 = datasetLoader.load("t2nrand10x1000000r")
    df2.createOrReplaceTempView("df2")

    // Warm up
    spark.sql("SELECT * FROM df1")
    spark.sql("SELECT * FROM df2")

    var query = "SELECT * FROM df1, df2 WHERE "

    for (i <- 0 to 10) {
      if (i != 0) {
        query = query + " AND "
      }
      query = query + "c" + i.toString + " = b" + i.toString

      val dt = Utils.time {
        spark.sql(query)
      }
      results.addResult(i + 1, dt)

      val df = spark.sql(query)
      resultsCard.addResult(i + 1, df.count())
    }
    results.log()
    resultsCard.log()
    logger.info("Done with joinSQL benchmark")
  }

  private def lrBenchmark(spark: SparkSession, numIter: Int, gmap: Boolean) = {
    logger.info(s"Starting linear regression benchmark (${numIter} iteration(s))")
    BenchmarkLR.run(spark, datasetLoader, numIter, gmap)
    logger.info("Done with linear regression benchmark")
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Benchmark Application").getOrCreate()
    import spark.implicits._

    val parser = new scopt.OptionParser[Config]("BenchmarkApp") {
      head("Spark Benchmark", "1.0")

      opt[String]('b', "benchmark").required().action( (x, c) =>
        c.copy(benchmark = x) ).text("required: load | matmult | vecmult | join | joinSQL | lr")

      opt[String]('i', "ip").valueName("<value>").
        action( (x, c) => c.copy(ip = x) ).
        text("ip of the MonetDB database")

      opt[String]('p', "port").valueName("<value>").
        action( (x, c) => c.copy(port = x) ).
        text("port of the MonetDB database")

      opt[String]('u', "user").valueName("<name>").
        action( (x, c) => c.copy(username = x) ).
        text("username to use to connect to the MonetDB database")

      opt[String]('w', "pwd").valueName("<password>").
        action( (x, c) => c.copy(password = x) ).
        text("password to use to connect to the MonetDB database")

      opt[String]('d', "database").valueName("<name>").
        action( (x, c) => c.copy(database = x) ).
        text("name of the MonetDB database where to find the tables")

      opt[String]('s', "schema").valueName("<name>").
        action( (x, c) => c.copy(schema = x) ).
        text("name of the database schema where to find the tables")

      opt[Int]('n', "numiter").valueName("<value>").
        action( (x, c) => c.copy(numIter = x) ).
        text("number of iterations for the linear regression")

      opt[Unit]("gmap").
        action( (_, c) => c.copy(gmap = true) ).
        text("use google maps data for distance in linear regression")

      help("help").text("prints this message")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        logger.info(s"Getting tables from MonetDB: ${config.ip}:${config.port}/${config.database}")
        logger.info(s"Using username ${config.username} and pwd ${config.password} to connect")
        logger.info(s"Getting tables from schema ${config.schema}")
        logger.info(s"Benchmark to run is ${config.benchmark}")
        datasetLoader = new DatasetLoaderFromMonetDB(spark, config.ip, config.port,
          config.username, config.password, config.database, config.schema)
        config.benchmark match {
          case "load" => loadBenchmark(spark)
          case "vecmult" => multBenchmark(spark, "1")
          case "matmult" => multBenchmark(spark, "100")
          case "join" => joinBenchmark(spark)
          case "joinSQL" => joinSQLBenchmark(spark)
          case "lr" => lrBenchmark(spark, config.numIter, config.gmap)
          case _ => throw new RuntimeException("invalid benchmark")
        }

      case None =>
        // arguments are bad, error message will have been displayed
    }

    spark.stop()

  }
}
