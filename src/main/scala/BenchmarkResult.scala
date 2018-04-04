import scala.collection.mutable.HashMap

import org.apache.logging.log4j.LogManager

class BenchmarkResult(name: String) {

  val logger = LogManager.getLogger(name)
  val values = new HashMap[Int, List[Long]]()

  def addResult(key: Int, value: Long) = {
    if (!values.contains(key)) {
      values.put(key, List(value))
    } else {
      values.put(key, values.get(key).get :+ value)
    }
  }

  def printResult() = {
    values.foreach(x => println(x._1 + "-->" + x._2.mkString(",")))
  }

  def log() = {
    values.foreach(x => logger.trace(x._1.toString + "," + x._2.mkString(",")))
  }

}
