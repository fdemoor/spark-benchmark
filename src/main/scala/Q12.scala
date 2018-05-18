import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 12
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q12 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val mul = udf { (x: Double, y: Double) => x * y }
    val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")).as("sum_highorderpriority"),
        sum(lowPriority($"o_orderpriority")).as("sum_loworderpriority"))
      .sort($"l_shipmode")
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	l_shipmode,
      	sum(case
      		when o_orderpriority = '1-URGENT'
      			or o_orderpriority = '2-HIGH'
      			then 1
      		else 0
      	end) as high_line_count,
      	sum(case
      		when o_orderpriority <> '1-URGENT'
      			and o_orderpriority <> '2-HIGH'
      			then 1
      		else 0
      	end) as low_line_count
      from
      	orders,
      	lineitem
      where
      	o_orderkey = l_orderkey
      	and l_shipmode in ('MAIL', 'SHIP')
      	and l_commitdate < l_receiptdate
      	and l_shipdate < l_commitdate
      	and l_receiptdate >= date '1994-01-01'
      	and l_receiptdate < date '1994-01-01' + interval '1' year
      group by
      	l_shipmode
      order by
      	l_shipmode
    """
    return sc.sql(q)
  }

}
