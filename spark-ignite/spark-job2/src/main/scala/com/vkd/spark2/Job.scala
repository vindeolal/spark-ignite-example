package com.vkd.spark2

import com.vkd.base.{Config, SparkIgniteBase}

/*
 * second spark job to load rdd from ignite cache and only operate on even lines
 */
object Job extends SparkIgniteBase {

  def main(args: Array[String]): Unit = {

    //create spark-context and ignite-rdd with cache name
    val sc = getSparkContext(Config.SPARK_JOB2_NAME)
    val sharedRdd = getSharedRdd(Config.IGNITE_CACHE_NAME, sc)

    //filter only even index from the file
    val evenLines = sharedRdd.filter(_._1 % 2 == 0)

    //print the filtered lines
    evenLines.foreach(x => println(x._2))
  }

}
