import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/**
  * Created by akash on 31/7/16.
  */

object GlobalData {

  val spark = SparkSession.builder().appName("Fire Call-On-Service").master("local").getOrCreate()

  def getDataFrame: DataFrame = {

    spark.read.option("header", "true").csv("/home/akash/Documents/Hadoop/Fire_Department_Calls_for_Service.csv")
  }

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

  def maxCallInNeighbour: String = {

    import spark.implicits._
    val districtData = getDataFrame.select("City", "Neighborhood  District", "Call Date").as[(String, String, String)].filter(field => field._1 == "San Francisco" && field._3.split("/")(2).toInt == 2016).map { field => field._2 }
    districtData.groupBy("value").count().sort("value").as[(String, Long)].first()._1
  }
}

object FireDepartment extends App {

  val obj = new FireDepartment

  println("Q1. Types Of Call \n" + obj.getCallType)
  println("Q2. Incident with Call Type " + obj.getIncidentWithCallType)
  println("Q3. Year Of Service = " + obj.getYears)
  println("Q5. Last Year max Call in Neighborhood  District of SF is " + obj.maxCallInNeighbour)

}
