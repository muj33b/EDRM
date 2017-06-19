//EnronEmailToCC.scala

import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object EnronEmailToCC {     
 
  def main(args: Array[String]): Unit = {
 
   val sparkConf = new SparkConf().setAppName("Enron Coding Challenge")
 
   val sc = new SparkContext(sparkConf)
  
   val inputFile = args(0)
   val outputDir = args(1)  //Output dir parent location
   val saveToEmailPath = outputDir + "/ToEmail"
   val saveCCEmailPath = outputDir + "/CCEmail"

   val sqlContext = new SQLContext(sc)
   import sqlContext.implicits._

   import org.apache.spark.sql.SparkSession

 
   val spark = SparkSession.
      builder().
      appName("Enron email coding challenge").
      config("spark.some.config.option", "some-value").
      getOrCreate()

   val combinedEmailIds = sqlContext.read.
       format("com.databricks.spark.xml").
       option("rowTag", "Tags").
       option("valueTag", "_TagName").  // to eliminate nulls _VALUE for tags with no child
       load(inputFile)

      //At this stage we have nested WrrapedArray of <Tag> elements which look like
      //[WrappedArray([Text,#From,Harry Arora], [Text,#Subject,Jay Webb], [DateTime,#DateSent,2001-02-06T22:13:00.0+00:00],.... 

      //We are only interested in #To and #CC tags

   import org.apache.spark.sql.Row

   val content = combinedEmailIds.select($"Tag").rdd.map(_.getSeq[Row](0))   //get Seq of Tag rows
   //res10: Array[Seq[org.apache.spark.sql.Row]] = Array(WrappedArray([Text,#From,Harry Arora], [Text,#Subject,Jay Webb],.... 

   // explode rows - from WrappedArray to Array(ArrayBuffer((Text,#From,Harry Arora), (Text,#Subject,Jay Webb),...
   val splitrows = content.map(_.map {
       case Row(tagDataType: String, tagName: String, tagValue: String) => 
            (tagDataType, tagName, tagValue)
   })

   import org.apache.spark.rdd.RDD
   def createNsaveDF (path: String, df: RDD[Seq[(String, String, String)]] , filterTag: String, weightage: String) : Unit =
   {
     import sqlContext.implicits._
     df.
       flatMap(x => x).  //unpack the array
       filter(_._2.contains(filterTag)).  // select only the required filter tag eg. #To or #CC
       map(x => x._3).   //Get the values (comma separated) from 3rd element
       flatMap(_.split(",")).    //split comma separated emailIds into individual record to assign weightage
       map(_.concat(weightage)).  //assign weightage 1.0 for 'To' emailIds and 0.5 for 'CC' emails
       saveAsTextFile(path)  // save the output as we can find top 100 recipients only once we processed all the records
   }
   
   createNsaveDF(saveToEmailPath, splitrows, "#To", ",1.0")    //Note: we are appending "," in the weightage to make it csv
   createNsaveDF(saveCCEmailPath, splitrows, "#CC", ",0.5")

  sc.stop()
 }
}

