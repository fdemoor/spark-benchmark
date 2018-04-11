class TimeProfiler(name: String) {

  val res = new BenchmarkResult(name)
  var counter = 0
  var t0 = System.nanoTime()
  var t1 = System.nanoTime()

  def start() = {
    t0 = System.nanoTime()
  }

  def tick() = {
    t1 = System.nanoTime()
    res.addResult(counter, t1 - t0)
    counter += 1
    t0 = System.nanoTime()
  }

  def log() = {
    res.log()
  }

}
