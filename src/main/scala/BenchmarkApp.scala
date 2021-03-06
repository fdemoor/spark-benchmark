import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed.BlockMatrix

import org.apache.logging.log4j.scala.Logging

import scopt._

case class Config(benchmark: String = "join", ip: String = "localhost",
  port: String = "50000", username: String = "monetdb", password: String = "monetdb",
  database: String = "database", schema: String = "sys", queries: Seq[Int] = Seq.empty[Int],
  sf: Double = 1.0, kwargs: Map[String,String] = Map())

// Because of Spark lazy evaluation, we proceed as follows to measure time
// performance in the micro-benchmarks:
//   - f: set of operations that returns a dataframe or a matrix
//   - eval: function that iterates over all the rows of the dataframe or matrix
//           in input and does nothing (basically foreach(Unit => ()))
//   - measure the time values t1 and t2 as shown below
//
//       val x = f(_); x.cache(); eval(x)         eval(x)
//     <----------------v----------------->     <----v---->
//                      t1                           t2
//
//   - compute t1 - t2 and log the result
// When calling f, a logical plan is built, but the data is not physically
// accessed. An action is actually required for that, hence the call to eval.
// The call to the .cache() method tells to keep the data in memory when it is
// first computed. At the second call to eval, the computations of f are not
// done again, but we thus measure the time t2 the iterations take and substract
// from t1 to get the actual time of the computations in f.

object BenchmarkApp extends Logging {

  private final val nbLoadExec = 10
  private final val nbLoadWarmupExec = 2
  private final val nbMultExec = 100
  private final val nbMultWarmupExec = 2
  private var datasetLoader: DatasetLoader = null

  // Load a table into a dataframe and convert it to a matrix format
  private def loadBenchmark(spark: SparkSession) = {
    logger.info("Starting load benchmark")
    val results = new BenchmarkResult("load")
    val resultsMat = new BenchmarkResult("matrix")
    val kValues = Seq(1, 10, 100, 1000, 10000, 100000, 1000000)
    for (k <- kValues) {
      for (i <- 1 to nbLoadWarmupExec + nbLoadExec) {
        var df: Dataset[Row] = null
        val dt = Utils.time {
          df = datasetLoader.load("trand100x" + k.toString + "r")
        }
        var mat: BlockMatrix = null
        val dt2 = Utils.time {
          mat = Utils.dataframeToMatrix(df)
          mat.cache()
          Utils.eval(spark, mat)
        }
        val dt3 = Utils.time {
          Utils.eval(spark, mat)
        }
        if (i > nbLoadWarmupExec) {
          results.addResult(k, dt)
          resultsMat.addResult(k, dt2 - dt3)
        }
      }
    }
    results.log()
    resultsMat.log()
    logger.info("Done with load benchmark")
  }

  // Perform a matrix multiplication between two tables
  private def multBenchmark(spark: SparkSession, right: String) = {
    logger.info("Starting mult benchmark")
    val results = new BenchmarkResult("mult" + right)
    val kValues = Seq(1, 10, 100, 1000, 10000, 100000, 1000000)

    val dfR = datasetLoader.load("trand100x" + right + "r")
    val matR = Utils.dataframeToMatrix(dfR).transpose
    matR.cache()

    for (k <- kValues) {
      val dfL = datasetLoader.load("trand100x" + k.toString + "r")
      val matL = Utils.dataframeToMatrix(dfL)
      matL.cache()
      for (i <- 1 to nbMultWarmupExec) {
        val mat = matL.multiply(matR)
      }
      var dt: Long = 0
      for (i <- 1 to nbMultExec) {
        var mat: BlockMatrix = null
        val dt2 = Utils.time {
          mat = matL.multiply(matR)
          mat.cache()
          Utils.eval(spark, mat)
        }
        val dt3 = Utils.time {
          Utils.eval(spark, mat)
        }
        dt += (dt2 - dt3)
      }
      results.addResult(k, dt)
    }
    results.log()
    logger.info("Done with mult benchmark")
  }

  // Join two tables with Spark API join method
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

    df1.cache()
    df1.foreach(Unit => ())
    df2.cache()
    df2.foreach(Unit => ())

    for (i <- 0 to 10) {
      val joinExprs = (0 to i toSeq).map(j
        => df1("c" + j.toString) === df2("b" + j.toString)).reduce(_ && _)

      var df: Dataset[Row] = null
      val dt = Utils.time {
        df = df1.join(df2, joinExprs)
        df.cache()
        df.foreach(Unit => ())
      }
      val dt2 = Utils.time {
        df.foreach(Unit => ())
      }
      resultsCard.addResult(i + 1, df.count())
      results.addResult(i + 1, dt - dt2)

    }
    results.log()
    resultsCard.log()
    logger.info("Done with join benchmark")
  }

  // Join two tables with a SQL query
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

      var df: Dataset[Row] = null
      val dt = Utils.time {
        df = spark.sql(query)
        df.cache()
        df.foreach(Unit => ())
      }
      val dt2 = Utils.time {
        df.foreach(Unit => ())
      }
      resultsCard.addResult(i + 1, df.count())
      results.addResult(i + 1, dt - dt2)

    }
    results.log()
    resultsCard.log()
    logger.info("Done with joinSQL benchmark")
  }

  // Linear regression example, basic or full workflow
  private def lrBenchmark(spark: SparkSession, basic: Boolean) = {
    logger.info(s"Starting linear regression benchmark")
    if (basic) {
      logger.info(s"Basic workflow")
      BenchmarkLRBasic.run(spark, datasetLoader)
    } else {
      logger.info(s"Full workflow")
      BenchmarkLR.run(spark, datasetLoader)
    }
    logger.info("Done with linear regression benchmark")
  }

  // TPCH benchmark
  private def tpchBenchmark(spark: SparkSession, queries: Seq[Int], isDfApi: Boolean, sf: Double) = {
    logger.info(s"Starting tpch benchmark")
    var logname = ""
    if (isDfApi) {
      logger.info(s"Using Spark dataframe API")
      logname = "tpch"
    } else {
      logger.info(s"Using SQL in Spark")
      logname = "tpchSQL"
    }
    var tpchSchemaProvider: TpchSchemaProvider = null;
    if (datasetLoader.isInstanceOf[DatasetLoaderFromMonetDB]) {
      tpchSchemaProvider = new TpchSchemaProviderMonetDB(spark, datasetLoader, sf)
    } else {
      throw new RuntimeException("Can't match datasetLoader and tpchSchemaProvider")
    }
    val results = new BenchmarkResult(logname)
    for (i <- queries) {
      logger.info(s"Query ${i}")
      val query = Class.forName(f"Q${i}%02d").newInstance.asInstanceOf[TpchQuery]
      var df: Dataset[Row] = null
      val dt = Utils.time {
        df = query.execute(spark, tpchSchemaProvider, isDfApi)
        df.cache()
        df.foreach(Unit => ())
      }
      val dt2 = Utils.time {
        df.foreach(Unit => ())
      }
      results.addResult(i, dt - dt2)
      df.show(df.count().asInstanceOf[Int])
    }
    results.log()
    logger.info("Done with tpch benchmark")
  }

  def main(args: Array[String]) {

    // Get the spark session
    val spark = SparkSession.builder.appName("Benchmark Application").getOrCreate()
    import spark.implicits._

    // Setup the CLI
    val parser = new scopt.OptionParser[Config]("BenchmarkApp") {
      head("Spark Benchmark", "1.0")

      opt[String]('b', "benchmark").required().action( (x, c) =>
        c.copy(benchmark = x) ).text("required: load | matmult | vecmult | join | joinSQL | lr | lr-basic | tpch | tpchSQL")

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

      opt[Seq[Int]]('q', "queries").valueName("<q1>,<q2>...").
        action( (x, c) => c.copy(queries = x) ).
        text("list of tpch queries to execute")

      opt[Double]('f', "scalefactor").valueName("<value>").
        action( (x, c) => c.copy(sf = x) ).
        text("scale factor of tpch workload (WIP, not used yet)")

      help("help").text("prints this message")

    }

    // Parse the command line to retrieve option values
    parser.parse(args, Config()) match {
      case Some(config) =>
        logger.info(s"Getting tables from MonetDB: ${config.ip}:${config.port}/${config.database}")
        logger.info(s"Using username ${config.username} and pwd ${config.password} to connect")
        logger.info(s"Getting tables from schema ${config.schema}")
        logger.info(s"Benchmark to run is ${config.benchmark}")
        datasetLoader = new DatasetLoaderFromMonetDB(spark, config.ip, config.port,
          config.username, config.password, config.database, config.schema)
        config.benchmark match {
          // Micro-benchmarks
          case "load" => loadBenchmark(spark)
          case "vecmult" => multBenchmark(spark, "1")
          case "matmult" => multBenchmark(spark, "100")
          case "join" => joinBenchmark(spark)
          case "joinSQL" => joinSQLBenchmark(spark)
          // Linear regression benchmark
          case "lr" => lrBenchmark(spark, false)
          case "lr-basic" => lrBenchmark(spark, true)
          case "tpch" => tpchBenchmark(spark, config.queries, true, config.sf)
          case "tpchSQL" => tpchBenchmark(spark, config.queries, false, config.sf)
          case _ => throw new RuntimeException("invalid benchmark")
        }

      case None =>
        // arguments are bad, error message will have been displayed
    }

    spark.stop()

  }
}
