package com.vkd.ignite

import com.vkd.base.{Config, SparkIgniteBase}

/*
 * job to load the rdd data into ignite cache
 */
object Job extends SparkIgniteBase {

  def main(args: Array[String]): Unit = {

    //create spark-context and ignite-rdd with cache name
    val sc = getSparkContext(Config.IGNITE_JOB_NAME)
    val sharedRdd = getSharedRdd(Config.IGNITE_CACHE_NAME, sc)

    //load sample data into rdd to be saved into ignite memory
    val file = sc.textFile(Config.TEXT_FILE_PATH)
    val indexedRdd = file.zipWithIndex().map(_.swap)

    //save key-value pairs
    sharedRdd.savePairs(indexedRdd)
  }

}
