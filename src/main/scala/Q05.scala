import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 5
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q05 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val forders = order.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	n_name,
      	sum(l_extendedprice * (1 - l_discount)) as revenue
      from
      	customer,
      	orders,
      	lineitem,
      	supplier,
      	nation,
      	region
      where
      	c_custkey = o_custkey
      	and l_orderkey = o_orderkey
      	and l_suppkey = s_suppkey
      	and c_nationkey = s_nationkey
      	and s_nationkey = n_nationkey
      	and n_regionkey = r_regionkey
      	and r_name = 'ASIA'
      	and o_orderdate >= date '1994-01-01'
      	and o_orderdate < date '1995-01-01'
      group by
      	n_name
      order by
      	revenue desc
    """
    return sc.sql(q)
  }

}
