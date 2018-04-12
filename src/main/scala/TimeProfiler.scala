class TimeProfiler(name: String) {

  val res = new BenchmarkResult(name)
  var t0 = System.nanoTime()
  var t1 = System.nanoTime()

  def start() = {
    t0 = System.nanoTime()
  }

  def tick(label: Int) = {
    t1 = System.nanoTime()
    res.addResult(label, t1 - t0)
    t0 = System.nanoTime()
  }

  def log() = {
    res.log()
  }

}
