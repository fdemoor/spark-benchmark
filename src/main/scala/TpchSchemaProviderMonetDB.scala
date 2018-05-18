import org.apache.spark.sql.SparkSession

class TpchSchemaProviderMonetDB(spark: SparkSession, datasetLoader: DatasetLoader)
    extends TpchSchemaProvider {

  assert(datasetLoader.isInstanceOf[DatasetLoaderFromMonetDB])
  val loader = datasetLoader.asInstanceOf[DatasetLoaderFromMonetDB]

  val customer = loader.load("customer")
  val lineitem = loader.load("lineitem")
  val nation = loader.load("nation")
  val region = loader.load("region")
  val order = loader.load("orders")
  val part = loader.load("part")
  val partsupp = loader.load("partsupp")
  val supplier = loader.load("supplier")

  customer.cache()
  customer.foreach(Unit => ())
  lineitem.cache()
  lineitem.foreach(Unit => ())
  nation.cache()
  nation.foreach(Unit => ())
  region.cache()
  region.foreach(Unit => ())
  order.cache()
  order.foreach(Unit => ())
  part.cache()
  part.foreach(Unit => ())
  partsupp.cache()
  partsupp.foreach(Unit => ())
  supplier.cache()
  supplier.foreach(Unit => ())

}
