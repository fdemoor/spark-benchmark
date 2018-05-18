import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 11
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q11 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val c = 0.0001 / this.sf
    
    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * c }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = s"""
      select
      	ps_partkey,
      	sum(ps_supplycost * ps_availqty) as value
      from
      	partsupp,
      	supplier,
      	nation
      where
      	ps_suppkey = s_suppkey
      	and s_nationkey = n_nationkey
      	and n_name = 'GERMANY'
      group by
      	ps_partkey having
      		sum(ps_supplycost * ps_availqty) > (
      			select
      				sum(ps_supplycost * ps_availqty) * ${0.0001000000 / this.sf}
      			from
      				partsupp,
      				supplier,
      				nation
      			where
      				ps_suppkey = s_suppkey
      				and s_nationkey = n_nationkey
      				and n_name = 'GERMANY'
      		)
      order by
      	value desc
    """
    return sc.sql(q)
  }

}
