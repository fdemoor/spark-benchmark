import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

trait TpchSchemaProvider {
  val customer: Dataset[Row]
  val lineitem: Dataset[Row]
  val nation: Dataset[Row]
  val region: Dataset[Row]
  val order: Dataset[Row]
  val part: Dataset[Row]
  val partsupp: Dataset[Row]
  val supplier: Dataset[Row]
}
