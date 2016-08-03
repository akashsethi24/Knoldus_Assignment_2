import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/**
  * Created by akash on 31/7/16.
  */

object GlobalData {

  val spark = SparkSession.builder().appName("Fire Call-On-Service").master("local").getOrCreate()

  implicit val myOrdering = new Ordering[Date] {
    override def compare(dateOne: Date, dateTwo: Date): Int = {
      dateOne.getTime.compareTo(dateTwo.getTime)
    }
  }

  def getDataSet: DataFrame = {

    spark.read.option("header", "true").csv("/home/akash/Documents/Hadoop/Fire_Department_Calls_for_Service.csv")
  }

  import collection.JavaConversions._

  implicit val encoder = Encoders.STRING
  implicit val dateEncoder = Encoders.DATE

  lazy val maxDate: Date = getDateList.max(myOrdering)

  def getSingleColumnData(columnName: String): Dataset[String] = {

    getDataSet.select(columnName).as[String]
  }

  def getDateList: List[Date] = {
    val dateDataSet = getSingleColumnData("Call Date")
    val formatter = new SimpleDateFormat("MM/dd/yyyy")
    dateDataSet.collectAsList().toList.map { date => formatter.parse(date) }
  }

}

class FireDepartment {

  import GlobalData._

  def getCallType: List[String] = {

    import collection.JavaConversions._
    getSingleColumnData("Call Type").as[String].distinct().collectAsList().toList
  }

  def getYears: Long = {

    import collection.JavaConversions._
    val dateDataSet = getSingleColumnData("Call Date")
    val formatter = new SimpleDateFormat("MM/dd/yyyy")
    val dateList = dateDataSet.collectAsList().toList.map { date => formatter.parse(date) }
    val minDate = dateList.min(myOrdering)
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
