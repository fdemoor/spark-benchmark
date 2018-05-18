import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

abstract class TpchQuery {

  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkSession, tpchSchemaProvider: TpchSchemaProvider): Dataset[Row]
}

// object TpchQuery {
//
//   def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
//
//     if (outputDir == null || outputDir == "")
//       df.collect().foreach(println)
//     else
//       //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
//       df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
//   }
//
//   def executeQueries(sc: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: Int): ListBuffer[(String, Float)] = {
//
//     // if set write results to hdfs, if null write to stdout
//     // val OUTPUT_DIR: String = "/tpch"
//     //val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/dbgen/output"
//     val OUTPUT_DIR: String = null
//
//     val results = new ListBuffer[(String, Float)]
//
//     var fromNum = 1;
//     var toNum = 22;
//     if (queryNum != 0) {
//       fromNum = queryNum;
//       toNum = queryNum;
//     }
//
//     for (queryNo <- fromNum to toNum) {
//       val t0 = System.nanoTime()
//
//       val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]
//
//       outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())
//
//       val t1 = System.nanoTime()
//
//       val elapsed = (t1 - t0) / 1000000000.0f // second
//       results += new Tuple2(query.getName(), elapsed)
//
//     }
//
//     return results
//   }
// }
