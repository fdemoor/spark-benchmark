import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

trait DatasetLoader {

  def load(name: String) : Dataset[Row]

}
