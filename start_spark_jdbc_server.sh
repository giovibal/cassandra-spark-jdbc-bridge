#!/bin/bash

export SPARK_HOME=/storm/spark-1.4.1-bin-hadoop2.6
export INADCO_CSJ_HOME=/storm/lib/inadco-csjb

$SPARK_HOME/bin/spark-submit --class com.inadco.cassandra.spark.jdbc.InadcoCSJServer --master spark://bd9.bigdata:7077 $INADCO_CSJ_HOME/inadco-csjb-assembly-1.1.jar

