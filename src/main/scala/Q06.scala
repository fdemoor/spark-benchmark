import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 6
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q06 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	sum(l_extendedprice * l_discount) as revenue
      from
      	lineitem
      where
      	l_shipdate >= date '1994-01-01'
      	and l_shipdate < date '1995-01-01'
      	and l_discount between .06 - 0.01 and .06 + 0.01
      	and l_quantity < 24
    """
    return sc.sql(q)
  }

}
