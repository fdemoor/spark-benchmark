import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 19
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q19 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val sm = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#12") &&
          sm($"p_container") &&
          $"l_quantity" >= 1 && $"l_quantity" <= 11 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#23") &&
            md($"p_container") &&
            $"l_quantity" >= 10 && $"l_quantity" <= 20 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
            (($"p_brand" === "Brand#34") &&
              lg($"p_container") &&
              $"l_quantity" >= 20 && $"l_quantity" <= 30 &&
              $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	sum(l_extendedprice* (1 - l_discount)) as revenue
      from
      	lineitem,
      	part
      where
      	(
      		p_partkey = l_partkey
      		and p_brand = 'Brand#12'
      		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      		and l_quantity >= 1 and l_quantity <= 1 + 10
      		and p_size between 1 and 5
      		and l_shipmode in ('AIR', 'AIR REG')
      		and l_shipinstruct = 'DELIVER IN PERSON'
      	)
      	or
      	(
      		p_partkey = l_partkey
      		and p_brand = 'Brand#23'
      		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      		and l_quantity >= 10 and l_quantity <= 10 + 10
      		and p_size between 1 and 10
      		and l_shipmode in ('AIR', 'AIR REG')
      		and l_shipinstruct = 'DELIVER IN PERSON'
      	)
      	or
      	(
      		p_partkey = l_partkey
      		and p_brand = 'Brand#34'
      		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      		and l_quantity >= 20 and l_quantity <= 20 + 10
      		and p_size between 1 and 15
      		and l_shipmode in ('AIR', 'AIR REG')
      		and l_shipinstruct = 'DELIVER IN PERSON'
      	)
    """
    return sc.sql(q)
  }

}
