import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/**
  * Created by akash on 31/7/16.
  */

object GlobalData {

  val spark = SparkSession.builder().appName("Fire Call-On-Service").master("local").getOrCreate()

  def getDataFrame: DataFrame = {

    spark.read.option("header", "true").csv("/home/akash/Documents/Hadoop/Fire_Department_Calls_for_Service.csv")
  }

  val formatter = new SimpleDateFormat("MM/dd/yyyy")
  val dateUdf = udf((date: String) => new Date(formatter.parse(date).getTime))

  def getSingleColumnData(columnName: String): Dataset[String] = {

    implicit val stringEncoder = Encoders.STRING
    getDataFrame.select(columnName).as[String]
  }

}

class FireDepartment {

  import GlobalData._

  def getCallType: List[String] = {

    import collection.JavaConversions._
    implicit val stringEncoder = Encoders.STRING
    getSingleColumnData("Call Type").as[String].distinct().collectAsList().toList
  }

  def getYears: Long = {

    import spark.implicits._
    implicit val intEncoder = Encoders.INT
    val stringYear = getSingleColumnData("Call Date").map { date => date.split("/")(2).toInt }.distinct()
    val maxYear = stringYear.reduce((valueOne, valueTwo) => if (valueOne > valueTwo) valueOne else valueTwo)
    val minYear = stringYear.reduce((valueOne, valueTwo) => if (valueOne < valueTwo) valueOne else valueTwo)
    maxYear.toLong - minYear.toLong
  }

  def getIncidentWithCallType: Map[String, Long] = {

    import spark.implicits._

    import collection.JavaConversions._
    getDataFrame.select("Call Type", "Incident Number").as[(String, String)].groupBy("Call Type").count().as[(String, Long)].collectAsList().toList.toMap
  }

  def getLastSevenDayCalls: Long = {

    import spark.implicits._
    implicit val dateEncoders = Encoders.DATE

    val dateDataset = getSingleColumnData("Call Date")
    val orderedDateDataset = dateDataset.withColumn("Call Date", dateUdf(dateDataset("Call Date"))).as[Date].orderBy($"Call Date".desc)
    val latestDate = orderedDateDataset.first()
    val previousDate = new Date(latestDate.getTime - (7 * 24 * 3600 * 1000).toLong)
    orderedDateDataset.filter(date => date.getTime > previousDate.getTime).count()
  }

  def maxCallInNeighbour: String = {

    import spark.implicits._
    val districtData = getDataFrame.select("City", "Neighborhood  District", "Call Date").as[(String, String, String)].filter(field => field._1 == "San Francisco" && field._3.split("/")(2).toInt == 2015).map { field => field._2 }
    districtData.groupBy("value").count().sort($"count".desc).as[(String, Long)].first()._1
  }
}

object FireDepartment extends App {

  val obj = new FireDepartment

  println("Q1. Types Of Call \n" + obj.getCallType)
  println("Q2. Incident with Call Type " + obj.getIncidentWithCallType)
  println("Q3. Year Of Service = " + obj.getYears)
  println("Q4 Total Calls in Last Seven Days are " + obj.getLastSevenDayCalls)
  println("Q5. Last Year max Call in Neighborhood  District of SF is " + obj.maxCallInNeighbour)

  GlobalData.spark.stop()

}
