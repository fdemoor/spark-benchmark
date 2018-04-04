name := "Benchmark Application"

version := "1.0"

scalaVersion := "2.11.8"

unmanagedBase := baseDirectory.value / "lib"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"

// https://mvnrepository.com/artifact/net.sf.geographiclib/GeographicLib-Java
libraryDependencies += "net.sf.geographiclib" % "GeographicLib-Java" % "1.49"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api-scala_2.11" % "11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"

