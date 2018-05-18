import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 17
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q17 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	sum(l_extendedprice) / 7.0 as avg_yearly
      from
      	lineitem,
      	part
      where
      	p_partkey = l_partkey
      	and p_brand = 'Brand#23'
      	and p_container = 'MED BOX'
      	and l_quantity < (
      		select
      			0.2 * avg(l_quantity)
      		from
      			lineitem
      		where
      			l_partkey = p_partkey
      	)
    """
    return sc.sql(q)
  }

}
