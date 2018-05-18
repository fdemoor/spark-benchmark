import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

/**
 * TPC-H Query 4
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q04 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val forders = order.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey")
      .distinct

    flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	o_orderpriority,
      	count(*) as order_count
      from
      	orders
      where
      	o_orderdate >= date '1993-07-01'
      	and o_orderdate < date '1993-10-01'
      	and exists (
      		select
      			*
      		from
      			lineitem
      		where
      			l_orderkey = o_orderkey
      			and l_commitdate < l_receiptdate
      	)
      group by
      	o_orderpriority
      order by
      	o_orderpriority
    """
    return sc.sql(q)
  }

}
