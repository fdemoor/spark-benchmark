#! /bin/bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

typeset -A config
config=(
    [sparkDir]=""
		[sparkNThreads]="1"
		[sparkDriverMemory]="6G"
    [sparkEventLogDir]="/tmp/spark-events"
    [resultsOutputDir]="output"
)

while read line
do
    if echo $line | grep -F = &>/dev/null
    then
        varname=$(echo "$line" | cut -d '=' -f 1)
        config[$varname]=$(echo "$line" | cut -d '=' -f 2-)
    fi
done < submit.conf

BIN="${config[sparkDir]}/bin/spark-submit"
if [ ! -f "$BIN" ]; then
	echo "Spark submit binary not found, specify the Spark basedir path in submit.conf"
	echo "Path given was ${config[sparkDir]}"
	exit 1
fi

APP="BenchmarkApp"
TARGET="$BASEDIR/target/scala-2.11/benchmark-application_2.11-1.0.jar"
if [ ! -f "$TARGET" ]; then
	echo "Application jar not found, let us compile"
	cd $BASEDIR && sbt package
	rc=$?; if [[ $rc != 0 ]]; then echo "Detected sbt error, exiting"; exit $rc; fi
fi

mkdir -p "$BASEDIR/lib"

MONETDB_JDBC_DL="https://dev.monetdb.org/downloads/Java/monetdb-jdbc-2.27.jar"
MONETDB_JDBC_JAR="$BASEDIR/lib/monetdb-jdbc-2.27.jar"
if [ ! -f "$MONETDB_JDBC_JAR" ]; then
	echo "MonetDB JDBC driver jar not found, downloading it now"
	cd "$BASEDIR/lib" && wget $MONETDB_JDBC_DL
fi

SCOPT_DL="http://central.maven.org/maven2/com/github/scopt/scopt_2.11/3.7.0/scopt_2.11-3.7.0.jar"
SCOPT_JAR="$BASEDIR/lib/scopt_2.11-3.7.0.jar"
if [ ! -f "$SCOPT_JAR" ]; then
	echo "Scopt jar not found, downloading it now"
	cd "$BASEDIR/lib" && wget $SCOPT_DL
fi

GEOGRAPHICLIB="net.sf.geographiclib":"GeographicLib-Java":"1.49"
LOG4J_SCALA="org.apache.logging.log4j":"log4j-api-scala_2.11":"11.0"
LOG4J_API="org.apache.logging.log4j":"log4j-api":"2.11.0"
LOG4J_CORE="org.apache.logging.log4j":"log4j-core":"2.11.0"

mkdir -p ${config[resultsOutputDir]}
export LOGDIR=${config[resultsOutputDir]}
mkdir -p ${config[sparkEventLogDir]}

$BIN \
--class $APP --jars $MONETDB_JDBC_JAR,$SCOPT_JAR \
--master local[${config[sparkNThreads]}] \
--packages $LOG4J_SCALA,$LOG4J_API,$LOG4J_CORE,$GEOGRAPHICLIB \
--conf spark.executor.extraJavaOptions="-Dlog4j.configurationFile=log4j2.xml" \
--conf spark.driver.extraJavaOptions="-Dlog4j.configurationFile=log4j2.xml" \
--conf spark.driver.memory="${config[sparkDriverMemory]}" \
--conf spark.eventLog.enabled="true" \
--conf spark.eventLog.dir="${config[sparkEventLogDir]}" \
$TARGET $@
