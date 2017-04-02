package com.exercise

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

import com.exercise.AnalysisParquet._

/**
  * Created by a613274 on 02.04.2017.
  */
object ParquetExerciseOnSpark {

  def doAnalysisOperations(parqfile : DataFrame): Unit = {
    val myData = parqfile.cache()
    myData.showChannelAndBrowser.show()
  }

  def main(args : Array[String]) = {
    val conf = new SparkConf().setAppName("ParquetExerciseOnSpark")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val parqfile = sqlContext.read.parquet("hdfs:///user/shong/nzztest.parquet") //HDFS
    parqfile.show(20)

    doAnalysisOperations(parqfile)
  }
}
