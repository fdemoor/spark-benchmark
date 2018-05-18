import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q01 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(decrease($"l_extendedprice", $"l_discount")),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	l_returnflag,
      	l_linestatus,
      	sum(l_quantity) as sum_qty,
      	sum(l_extendedprice) as sum_base_price,
      	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      	avg(l_quantity) as avg_qty,
      	avg(l_extendedprice) as avg_price,
      	avg(l_discount) as avg_disc,
      	count(*) as count_order
      from
      	lineitem
      where
      	l_shipdate <= date '1998-09-02'
      group by
      	l_returnflag,
      	l_linestatus
      order by
      	l_returnflag,
      	l_linestatus
    """
    return sc.sql(q)
  }
}
