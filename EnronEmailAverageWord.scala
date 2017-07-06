//spark-submit --class "EnronEmailAverageWord"  target/scala-2.11/email_avg_2.11-0.1-SNAPSHOT.jar  "/home/mujeeb/Enron/edrm-enron-v2_arnold-j_xml/text_000/*.txt" >> avgemailTot.out

//EnronEmailAverageWord.scala
import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object EnronEmailAverageWord {
 
  def main(args: Array[String]): Unit = {
 
   val sparkConf = new SparkConf().setAppName("Enron Coding Challenge")
 
   val sc = new SparkContext(sparkConf)
     
   val inputFile = args(0)

   val pattern = raw"(\p{Alnum})+".r    //only words, eliminate email address, website etc
  
   val spark = SparkSession.
      builder().
      appName("Enron email coding challenge").
      config("spark.some.config.option", "some-value").
      getOrCreate()

   import spark.implicits._

   val etext =  spark.sparkContext.
       textFile(inputFile).
       flatMap(_.split(" ")).
       map(x => pattern.findFirstIn(x).getOrElse(x)).
       map(s => (s,s.length)).
       filter({case (x,y) => y > 0}).
       distinct().
       toDF("word", "length")

   etext.createOrReplaceTempView("emailWordCount")

   val totWordCount = spark.sql("SELECT sum(length) as totWordLen FROM emailWordCount").first().getLong(0) * 1.0

   val uniqueWords = etext.count
   val avgWordLength = totWordCount / uniqueWords


   println("----- Enron Email Info --------")
   println(s"Total no. of Words        : ${uniqueWords}")
   println(s"Total average words       : ${avgWordLength}")
   println("-------------------------------")
 
   sc.stop()
  }
}

