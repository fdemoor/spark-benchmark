import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 16
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q16 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { (x: Int) => x.toString().matches("49|14|23|45|19|3|36|9") }

    val fparts = part.filter(($"p_brand" !== "Brand#45") && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	p_brand,
      	p_type,
      	p_size,
      	count(distinct ps_suppkey) as supplier_cnt
      from
      	partsupp,
      	part
      where
      	p_partkey = ps_partkey
      	and p_brand <> 'Brand#45'
      	and p_type not like 'MEDIUM POLISHED%'
      	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
      	and ps_suppkey not in (
      		select
      			s_suppkey
      		from
      			supplier
      		where
      			s_comment like '%Customer%Complaints%'
      	)
      group by
      	p_brand,
      	p_type,
      	p_size
      order by
      	supplier_cnt desc,
      	p_brand,
      	p_type,
      	p_size
    """
    return sc.sql(q)
  }

}
