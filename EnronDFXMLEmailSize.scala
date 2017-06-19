// usage spark-shell -i CodingChallenge/EnronDFXML.scala --conf spark.driver.args="/home/mujeeb/Enron/*.xml" --packages com.databricks:spark-xml_2.10:0.4.1
// spark-submit --packages com.databricks:spark-xml_2.10:0.4.1 --class "EnronDFXMLEmailSize"  target/scala-2.11/email_avg_2.11-0.1-SNAPSHOT.jar "/home/mujeeb/Enron/*.xml"


//EnronDFXMLEmailSize.scala
import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SQLImplicits


object EnronDFXMLEmailSize {
 
  def main(args: Array[String]): Unit = {
 
   val sparkConf = new SparkConf().setAppName("Enron Coding Challenge")
 
   val sc = new SparkContext(sparkConf)
     
   val emailSizeAccum = sc.doubleAccumulator("Total Email Size")
   
   val emailCountAccum = sc.longAccumulator("Total Number of Emails") 

   val inputFile = args(0)


   val sqlContext = new SQLContext(sc)
   val df = sqlContext.read.
       format("com.databricks.spark.xml").
       option("rowTag", "File").
       load(inputFile)

   import sqlContext.implicits._
   val textdf = df.filter($"_FileType".contains("Text") )
   val filesizecol = textdf("ExternalFile")("_FileSize")

   val sumSteps =  textdf.agg(sum(filesizecol.cast(DoubleType))).first.getDouble(0)

   val countSteps = textdf.agg(count(filesizecol.cast(LongType))).first.getLong(0)

   emailSizeAccum.add(sumSteps)

   emailCountAccum.add(countSteps)

   val avg = emailSizeAccum.value / emailCountAccum.value

   println("----- Enron Email Info --------")
   println(s"Total Size of Emails   : ${emailSizeAccum.value}")
   println(s"Total Number of Emails : ${emailCountAccum.value}")
   println("-------------------------------")
   println("Average email size      : " + avg)
   println("-------------------------------")
 
   sc.stop()
  }
}

