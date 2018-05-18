import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 15
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q15 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val revenue = lineitem.filter($"l_shipdate" >= "1996-01-01" &&
      $"l_shipdate" < "1996-04-01")
      .select($"l_suppkey", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"l_suppkey")
      .agg(sum($"value").as("total"))
    // .cache

    revenue.agg(max($"total").as("max_total"))
      .join(revenue, $"max_total" === revenue("total"))
      .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
      .sort($"s_suppkey")
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q1 = """
      create temp view revenue0 (supplier_no, total_revenue) as
        select
          l_suppkey,
          sum(l_extendedprice * (1 - l_discount))
        from
          lineitem
        where
          l_shipdate >= date '1996-01-01'
          and l_shipdate < date '1996-01-01' + interval '3' month
        group by
          l_suppkey
    """
    sc.sql(q1)
    val q2 = """
      select
      	s_suppkey,
      	s_name,
      	s_address,
      	s_phone,
      	total_revenue
      from
      	supplier,
      	revenue0
      where
      	s_suppkey = supplier_no
      	and total_revenue = (
      		select
      			max(total_revenue)
      		from
      			revenue0
      	)
      order by
      	s_suppkey
    """
    val df = sc.sql(q2)
    val q3 = """
      drop view revenue0
    """
    sc.sql(q3)
    return df
  }

}
