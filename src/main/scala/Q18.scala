import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 18
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q18 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .limit(100)
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	c_name,
      	c_custkey,
      	o_orderkey,
      	o_orderdate,
      	o_totalprice,
      	sum(l_quantity)
      from
      	customer,
      	orders,
      	lineitem
      where
      	o_orderkey in (
      		select
      			l_orderkey
      		from
      			lineitem
      		group by
      			l_orderkey having
      				sum(l_quantity) > 300
      	)
      	and c_custkey = o_custkey
      	and o_orderkey = l_orderkey
      group by
      	c_name,
      	c_custkey,
      	o_orderkey,
      	o_orderdate,
      	o_totalprice
      order by
      	o_totalprice desc,
      	o_orderdate
      limit 100
    """
    return sc.sql(q)
  }

}
