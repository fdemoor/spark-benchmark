import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

class DatasetLoaderFromMonetDB(spark: SparkSession, ip: String, port: String,
                               user: String, pwd: String, dbname: String,
                               sname: String)
  extends DatasetLoader {

  override def load(name: String) : Dataset[Row] = {
    val options = new HashMap[String, String]()
    options.put("url", "jdbc:monetdb://" + ip + ":" + port + "/" + dbname)
    options.put("user", user)
    options.put("password", pwd)
    options.put("driver", "nl.cwi.monetdb.jdbc.MonetDriver")
    options.put("fetchsize", "1000000")
    options.put("batchsize", "1000000")
    return spark.read.format("jdbc").option("dbtable", sname + "." + name)
      .options(options).load()
  }

  /**
   * Allows to load a dataframe directly as the result of a SQL query executed
   * in the database.
   *
   * The token '$SCHEMA' can be used in the string passed as argument, and
   * will be replaced by the schema name provided by the user when starting
   * the application.
   */
  override def loadFromQuery(query: String) : Dataset[Row] = {
    val options = new HashMap[String, String]()
    options.put("url", "jdbc:monetdb://" + ip + ":" + port + "/" + dbname)
    options.put("user", user)
    options.put("password", pwd)
    options.put("driver", "nl.cwi.monetdb.jdbc.MonetDriver")
    options.put("fetchsize", "1000000")
    options.put("batchsize", "1000000")
    return spark.read.format("jdbc").option("dbtable", query.replace("$SCHEMA", sname))
      .options(options).load()
  }

}
