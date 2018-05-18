import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 13
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q13 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	c_count,
      	count(*) as custdist
      from
      	(
      		select
      			c_custkey,
      			count(o_orderkey)
      		from
      			customer left outer join orders on
      				c_custkey = o_custkey
      				and o_comment not like '%special%requests%'
      		group by
      			c_custkey
      	) as c_orders (c_custkey, c_count)
      group by
      	c_count
      order by
      	custdist desc,
      	c_count desc
    """
    return sc.sql(q)
  }

}
