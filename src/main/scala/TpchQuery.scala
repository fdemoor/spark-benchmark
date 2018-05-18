import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

abstract class TpchQuery {

  protected var sf: Double = 1.0

  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  protected def executeDfApi(sc: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    import sc.implicits._
    return Seq.empty[Int].toDF
  }
  protected def executeSQL(sc: SparkSession): DataFrame = {
    import sc.implicits._
    return Seq.empty[Int].toDF
  }

  def execute(sc: SparkSession, tpchSchemaProvider: TpchSchemaProvider, isDfApi: Boolean): DataFrame = {
    sf = tpchSchemaProvider.sf
    if (isDfApi) {
      return executeDfApi(sc, tpchSchemaProvider)
    } else {
      return executeSQL(sc)
    }
  }

}
