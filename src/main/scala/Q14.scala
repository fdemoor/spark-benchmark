import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 14
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q14 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	100.00 * sum(case
      		when p_type like 'PROMO%'
      			then l_extendedprice * (1 - l_discount)
      		else 0
      	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
      from
      	lineitem,
      	part
      where
      	l_partkey = p_partkey
      	and l_shipdate >= date '1995-09-01'
      	and l_shipdate < date '1995-10-01'
    """
    return sc.sql(q)
  }

}
