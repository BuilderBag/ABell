package invert

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * InvertTextDriver 3 stages, 3 steps each stage.
 * @author ZHOUSAI
 * @version 2019年10月18日 下午7:07:07
 */
object InvertTextDriver {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("inverted")
    val sc = new SparkContext(conf)

    val data = sc.wholeTextFiles("T://sparkWork/inverted/*", 2)
    data.foreach { println }

    /**
     * step1:fileName-->split-->last-->dropRight
     */
    val r1 = data.map {
      case (filePath, fileText) =>
        val fileName = filePath.split("/").last.dropRight(4)
        (fileName, fileText)
    }
    r1.foreach { println }

    /**
     * step2:fileText-->split-->flatMap-->map(word,fileName)
     */
    val r2 = r1.flatMap {
      case (fileName, fileText) =>
        fileText.split("\r\n").flatMap { line => line.split(" ") }
          .map { word => (word, fileName) }
    }
    r2.foreach { println }

    /**
     * step3:group-->distinct-->mkString-->save
     */
    val r3 = r2.groupByKey.map { case (word, buffer) => (word, buffer.toList.distinct.mkString(",")) }
    r3.foreach { println }
    r3.saveAsTextFile("T://sparkWork/invertedResult.txt")
  }
}