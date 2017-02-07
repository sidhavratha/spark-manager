#!/bin/sh

if [ ! -d "$SPARK_HOME" ]; then

    echo mkdir -p $SPARK_HOME
    mkdir -p $SPARK_HOME

    echo curl -o $SPARK_HOME/spark.tar.gz -L http://archive.apache.org/dist/spark/spark-1.6.2/spark-1.6.2-bin-hadoop2.6.tgz
    curl -o $SPARK_HOME/spark.tar.gz -L http://archive.apache.org/dist/spark/spark-1.6.2/spark-1.6.2-bin-hadoop2.6.tgz

    echo tar zxf $SPARK_HOME/spark.tar.gz --strip-components 1 -C $SPARK_HOME
    tar zxf $SPARK_HOME/spark.tar.gz --strip-components 1 -C $SPARK_HOME

    echo ls $SPARK_HOME/
    ls $SPARK_HOME/

    echo ls $SPARK_HOME/lib
    ls $SPARK_HOME/lib

    echo ls $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar
    ls $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar

    echo cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar $SPARK_HOME/lib/spark-assembly.jar
    cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar $SPARK_HOME/lib/spark-assembly.jar
fi