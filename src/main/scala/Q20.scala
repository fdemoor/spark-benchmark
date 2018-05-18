import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 20
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q20 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val forest = udf { (x: String) => x.startsWith("forest") }

    val flineitem = lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01")
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

    val fnation = nation.filter($"n_name" === "CANADA")
    val nat_supp = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    part.filter(forest($"p_name"))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
      .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey").distinct
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address")
      .sort($"s_name")
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	s_name,
      	s_address
      from
      	supplier,
      	nation
      where
      	s_suppkey in (
      		select
      			ps_suppkey
      		from
      			partsupp
      		where
      			ps_partkey in (
      				select
      					p_partkey
      				from
      					part
      				where
      					p_name like 'forest%'
      			)
      			and ps_availqty > (
      				select
      					0.5 * sum(l_quantity)
      				from
      					lineitem
      				where
      					l_partkey = ps_partkey
      					and l_suppkey = ps_suppkey
      					and l_shipdate >= date '1994-01-01'
      					and l_shipdate < date '1994-01-01' + interval '1' year
      			)
      	)
      	and s_nationkey = n_nationkey
      	and n_name = 'CANADA'
      order by
      	s_name
    """
    return sc.sql(q)
  }

}
