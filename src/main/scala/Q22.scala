import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 22
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q22 extends TpchQuery {

  override protected def executeDfApi(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import sc.implicits._
    import schemaProvider._

    val sub2 = udf { (x: String) => x.substring(0, 2) }
    val phone = udf { (x: String) => x.matches("13|31|23|29|30|18|17") }
    val isNull = udf { (x: Any) => println(x); true }

    val fcustomer = customer.select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))
      .filter(phone($"cntrycode"))

    val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
      //.filter("o_custkey is null")
      .filter($"o_custkey".isNull)
      .join(avg_customer)
      .filter($"c_acctbal" > $"avg_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"c_acctbal"), sum($"c_acctbal"))
      .sort($"cntrycode")
  }

  override protected def executeSQL(sc: SparkSession): DataFrame = {
    val q = """
      select
      	cntrycode,
      	count(*) as numcust,
      	sum(c_acctbal) as totacctbal
      from
      	(
      		select
      			substring(c_phone, 1, 2) as cntrycode,
      			c_acctbal
      		from
      			customer
      		where
      			substring(c_phone, 1, 2) in
      				('13', '31', '23', '29', '30', '18', '17')
      			and c_acctbal > (
      				select
      					avg(c_acctbal)
      				from
      					customer
      				where
      					c_acctbal > 0.00
      					and substring(c_phone, 1, 2) in
      						('13', '31', '23', '29', '30', '18', '17')
      			)
      			and not exists (
      				select
      					*
      				from
      					orders
      				where
      					o_custkey = c_custkey
      			)
      	) as custsale
      group by
      	cntrycode
      order by
      	cntrycode
    """
    return sc.sql(q)
  }

}
