//EnronEmailCountDF.scala

//usage spark-shell -i CodingChallenge/EmailReceiverCountDF.scala --conf spark.driver.args="/home/mujeeb/Enron/Out/*/part-*"
import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object EnronEmailCountDF {     
 
  case class Email(emailId:String, factor:Double) //inout file contains emailId, weight eg. abc,1.0

  def main(args: Array[String]): Unit = {
 
   val sparkConf = new SparkConf().setAppName("Enron Coding Challenge")
 
   val sc = new SparkContext(sparkConf)
  
   import org.apache.spark.sql.SparkSession

   val inputFile = args(0)  //Path of file containing emailId and weight

   val spark = SparkSession.
       builder().
       appName("Enron email coding challenge").
       config("spark.some.config.option", "some-value").
       getOrCreate()

   import spark.implicits._
   import org.apache.spark.sql.DataFrame

   val emailIdCountDf =  spark.sparkContext.
       textFile(inputFile).
       map(_.split(",")).
       map(attributes => Email(attributes(0), attributes(1).toDouble)).
       toDF()

   emailIdCountDf.createOrReplaceTempView("emailIdCount")

   val totEmailIdCountDF = spark.sql("SELECT emailId, count(factor) as emailCount FROM emailIdCount GROUP BY emailId")

   totEmailIdCountDF.createOrReplaceTempView("SortedEmailCount")

   val SortedEmailCountDF = spark.sql("SELECT emailId, emailCount FROM SortedEmailCount ORDER BY emailCount DESC")

   SortedEmailCountDF.show(100)

   sc.stop()
 }
}

