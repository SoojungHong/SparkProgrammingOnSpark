package com.exercise

import org.apache.spark.sql.DataFrame

/**
  * Created by a613274 on 02.04.2017.
  */
object AnalysisParquet {
  implicit class AnalysisOperations(df : DataFrame) {

    def countColumnNum : Long = {
      df.count()
    }

    def showChannelAndBrowser(): DataFrame = {
      df.filter(df.col("channel") !== "prod-nzz")
        .select("browser")
    }

  }

}
