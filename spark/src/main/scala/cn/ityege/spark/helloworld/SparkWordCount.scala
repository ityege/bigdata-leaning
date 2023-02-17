package cn.ityege.spark.helloworld

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Scala语言使用SparkCore编程实现词频统计：WordCount
 * 从HDFS上读取数据，统计WordCount，将结果保存到HDFS上
 */
object SparkWordCount {
  // TODO: 当应用运行在集群上的时候，MAIN函数就是Driver Program，必须创建SparkContext对象
  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象，设置应用的配置信息，比如应用名称和应用运行模式
    val sparkConf: SparkConf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName("SparkWordCount")
    // TODO: 构建SparkContext上下文实例对象，读取数据和调度Job执行
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    // 第一步、读取数据
    // 封装到RDD集合，认为列表List
    val inputRDD: RDD[String] = sparkContext.textFile("/input/wordcount.txt")
    // 第二步、处理数据
    // 调用RDD中函数，认为调用列表中的函数
    // a. 每行数据分割为单词
    val wordsRDD:RDD[String] = inputRDD.flatMap(line => line.split("\\s+"))
    // b. 转换为二元组，表示每个单词出现一次
    val tuplesRDD: RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
    // c. 按照Key分组聚合
    val wordCountsRDD: RDD[(String, Int)] = tuplesRDD.reduceByKey((tmp, item) => tmp + item)
    // 第三步、输出数据
    wordCountsRDD.foreach(println)
    // 保存到为存储系统，比如HDFS
    wordCountsRDD.saveAsTextFile(s"/output/wordcount/swc-output-${System.currentTimeMillis()}")
    // 为了测试，线程休眠，查看WEB UI界面
//    Thread.sleep(10000000)
    // TODO：应用程序运行接收，关闭资源
    sparkContext.stop()
  }
}