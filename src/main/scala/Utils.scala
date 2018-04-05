import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix

object Utils {

  def time[R](block: => R) : Long = {
    val t0 = System.nanoTime()
    block
    val t1 = System.nanoTime()
    return t1 - t0
  }

  def evalMatrix(mat: BlockMatrix) = {
    val it = mat.toLocalMatrix().rowIter
    while (it.hasNext) {
      val v = it.next
    }
  }

  def dataframeToMatrix(df: Dataset[Row]) : BlockMatrix = {
    val assembler = new VectorAssembler().setInputCols(df.columns).setOutputCol("vector")
    val df2 = assembler.transform(df)
    return new IndexedRowMatrix(df2.select("vector").rdd.map{
      case Row(v: Vector) => Vectors.fromML(v)
    }.zipWithIndex.map { case (v, i) => IndexedRow(i, v) }).toBlockMatrix()
  }

  def squaredErr(actual: BlockMatrix, predicted: BlockMatrix) : Double = {
    var s: Double = 0
    val it = actual.subtract(predicted).toLocalMatrix().rowIter
    while (it.hasNext) {
      s += scala.math.pow(it.next.apply(0), 2)
    }
    return s / (2 * actual.numRows())
  }

  def gradDesc(actual: BlockMatrix, predicted: BlockMatrix,
               indata: BlockMatrix) : Seq[Double] = {
    val m = predicted.subtract(actual).transpose.multiply(indata).toLocalMatrix()
    val n = actual.numRows()
    return Seq(m.apply(0, 0) / n, m.apply(0, 1) / n)
  }

}
