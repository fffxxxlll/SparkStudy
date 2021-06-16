package com.study.bigdata.sql

import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author feng xl
 * @date 2021/6/15 0015 9:25
 */

/**
 * 用于得到访问量前十的client_ip和url
 *
 * */
object GetTopTen {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("get-top-ten")
        val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        val dataFrame: DataFrame = session.read.orc("D:\\download\\resource\\test2\\2021.05.27")
        println("前10客户端:")
        getTopTenClient(dataFrame).foreach(println)
        println("前十url:")
        getTopTenUrl(dataFrame).foreach(println)
        session.stop()
    }

    /**
     * 得到访问前十的客户端IP
     * */
    def getTopTenClient(dataFrame: DataFrame): Array[((Any, Any), Int)] = {
        val notNullRdd: RDD[Row] = dataFrame.select("client_ip", "dt").rdd.filter(!_.anyNull)
        val groupByDateAndIpRdd: RDD[((Any, Any), Iterable[Row])] = notNullRdd.groupBy(r => (r.apply(1), r.apply(0)))
        val groupCountMap: RDD[((Any, Any), Int)] = groupByDateAndIpRdd.map(r => {
            (r._1, r._2.size)
        })
        groupCountMap.sortBy(_._2, false).take(10)


    }
    /**
     * 得到前十的URL
     * */
    def getTopTenUrl(dataFrame: DataFrame): Array[((String, String), Int)] = {

        val urlAndDateFrame: DataFrame = dataFrame.select("url", "dt")
        //过滤掉空的以及资源访问的url
        val filterDs: Dataset[Row] = urlAndDateFrame.filter(r =>
            !r.anyNull &&
              !Pattern.compile(".*\\.(js|png|jpg|gif|ttf|woff|woff2|css|ico|json|html|xml|jsp|dwr|action|appcache).*", Pattern.CASE_INSENSITIVE)
                .matcher(r.getString(0)).matches()
        )
        //导入隐式转换
        import dataFrame.sparkSession.implicits._
        val urlAndDateDs: Dataset[UrlAndDate] = filterDs.filter(_.size > 1).map(r => {
            UrlAndDate(clearUrl(r.apply(0).asInstanceOf[String]), r.apply(1).asInstanceOf[String])
        })
        //返回前十的url，将url和访问日期作为排序依据，然后分组的大小即访问量，再降序排列，取前十
        urlAndDateDs.rdd.groupBy(r => (r.url, r.date))
          .map(r => (r._1, r._2.size))
          .sortBy(r => r._2, false)
          .collect().take(10)
    }
    /**
     * 处理url的参数、方法、协议
     * */
    def clearUrl(url: String): String = {
        var sp = url.split(" ")
        if(sp.length>1)
            sp.apply(1).split("\\?").apply(0)
        else
            url.split("\\?").apply(0)
    }
    /**
     * 包装url信息类
     *
     * */
    case class UrlAndDate(url:String, date:String)
}
