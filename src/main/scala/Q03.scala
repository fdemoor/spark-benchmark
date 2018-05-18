import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 3
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q03 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val fcust = customer.filter($"c_mktsegment" === "BUILDING")
    val forders = order.filter($"o_orderdate" < "1995-03-15")
    val flineitems = lineitem.filter($"l_shipdate" > "1995-03-15")

    fcust.join(forders, $"c_custkey" === forders("o_custkey"))
      .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
      .select($"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"o_orderdate", $"o_shippriority")
      .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc, $"o_orderdate")
      .limit(10)
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
      from
        customer,
        orders,
        lineitem
      where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '1995-03-15'
        and l_shipdate > date '1995-03-15'
      group by
        l_orderkey,
        o_orderdate,
        o_shippriority
      order by
        revenue desc,
        o_orderdate
      limit 10
    """
    return sc.sql(q)
  }

}
