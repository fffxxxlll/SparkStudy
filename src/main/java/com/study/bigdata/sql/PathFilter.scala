package com.study.bigdata.sql

import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author feng xl
 * @date 2021/6/15 0015 9:25
 */

/**
 * 用于提取访问js|png|jpg|css|ico|json|html|jsp|dwr|action|appcache资源的访问路径
 *
 * */
object PathFilter {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("path-filter")
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        //读取本机orc资源，转化为DataFrame
        val dataFrame: DataFrame = sparkSession.read.orc("D:\\download\\resource\\test2\\2021.05.27")
        //使用DSL查询出url
        val urlFrame: DataFrame = dataFrame.select("url")
        getClearPath(urlFrame).collect().foreach(println)
        sparkSession.stop()
    }

    /**
     * 传入url的DataFrame，然后处理后返回访问路径的RDD
     * */
    def getClearPath(urlFrame: DataFrame): RDD[String] = {
        //过滤掉空且含有js|png|jpg|css|ico|json|html|jsp|dwr|action|appcache的url
        val filterDs: Dataset[Row] = urlFrame.filter(r => {
            !r.isNullAt(0) &&
              !Pattern.compile(".*\\.(js|png|jpg|gif|ttf|woff|woff2|css|ico|json|html|xml|jsp|dwr|action|appcache).*", Pattern.CASE_INSENSITIVE)
                .matcher(r.getString(0)).matches()
        })
        //将url分割，先使用空格分离掉请求方法、协议，保留第二片，再使用?分离请求参数，取第一片
        val pathRdd: RDD[String] = filterDs.rdd.map(r => {
            r.getString(0).split(" ")
        }).filter(_.length>1).map(_.apply(1).split("\\?").apply(0))
        //返回RDD
        pathRdd
    }
}
