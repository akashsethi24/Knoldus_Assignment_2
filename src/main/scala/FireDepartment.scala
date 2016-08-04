import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/**
  * Created by akash on 31/7/16.
  */

object GlobalData {

  val spark = SparkSession.builder().appName("Fire Call-On-Service").master("local").enableHiveSupport().getOrCreate()

  def getDataSet: Dataset[String] = {

    spark.read.option("header", "true").csv("/home/akash/Documents/Hadoop/Fire_Department_Calls_for_Service.csv").as[String]
  }

  implicit val encoder = Encoders.STRING

  def getSingleColumnData(columnName: String): Dataset[String] = {

    getDataSet.select(columnName).as[String]
  }

}

class FireDepartment {

  import GlobalData._

  def getCallType: List[String] = {

    import collection.JavaConversions._
    getSingleColumnData("Call Type").as[String].distinct().collectAsList().toList
  }

  def incidenceWithCall: Map[String, Long] = {

    val dataset = getDataSet
    dataset
  }

  def getYears: Long = {

    implicit val encoder = Encoders.STRING
    implicit val dateEncoder = Encoders.DATE

    val strDataSet = getSingleColumnData("Call Date")
    val formatter = new SimpleDateFormat("MM/dd/yyyy")

    val myUdf = org.apache.spark.sql.functions.udf((date: String) => new java.sql.Date(formatter.parse(date).getTime))

    val dateDataSet = strDataSet.withColumn("Call Date", myUdf(strDataSet("Call Date"))).as[Date]

    val dataset = dateDataSet.toDF("call_date").as[Date]
    dataset.createOrReplaceTempView("fire_call_date")

    val minDateDataset = dataset.sqlContext.sql("SELECT MIN(call_date) as maxDate FROM fire_call_date").as[Date]
    val maxDateDataset = dataset.sqlContext.sql("SELECT MAX(call_date) as minDate FROM fire_call_date").as[Date]

    val minDate = minDateDataset.first()
    val maxDate = maxDateDataset.first()

    val calender = Calendar.getInstance()
    calender.setTime(maxDate)
    val maxYear = calender.get(Calendar.YEAR).toLong
    calender.setTime(minDate)
    val minYear = calender.get(Calendar.YEAR).toLong
    maxYear - minYear
  }

}

object FireDepartment extends App {

  val obj = new FireDepartment

  println("Q1. Types Of Call \n" + obj.getCallType)
  println("Q3. Year Of Service = " + obj.getYears)

}
