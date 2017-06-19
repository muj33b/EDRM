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
      //[WrappedArray([Text,#From,Harry Arora], [Text,#Subject,Jay Webb], [DateTime,#DateSent,2001-02-06T22:13:00.0+00:00], [Boolean,#HasAttachments,false], [Text,X-
      //SDOC,528064], [Text,X-ZLID,zl-edrm-enron-v2-arora-h-915.eml])]
      //[WrappedArray([Text,#From,Harry Arora], [Text,#Subject,Allan Sommer], [DateTime,#DateSent,2001-02-07T22:11:00.0+00:00], [Boolean,#HasAttachments,false], [Text,X-
      //SDOC,528063], [Text,X-ZLID,zl-edrm-enron-v2-arora-h-914.eml])]
      //[WrappedArray([Text,#From,Harry Arora], [Text,#Subject,DealBench Auction Control Room Meeting], [DateTime,#DateSent,2001-01-16T15:03:00.0+00:00], 
      //[Boolean,#HasAttachments,false], [Text,X-SDOC,528065], [Text,X-ZLID,zl-edrm-enron-v2-arora-h-916.eml])]
      //We are only interested in #To and #CC tags
      //Doing it the dirty way for now!!!

   def createNsaveDF (path: String, df: DataFrame , filterTag: String, weightage: String) : Unit =
   {
     import sqlContext.implicits._
     val filteredIdDF = df.
       flatMap(_.mkString.split("]").  //each array is contained in [ ], so split by ]
       filter(_.contains(filterTag)).  // select only the required filter tag eg. #To or #CC
       map(x => x.substring(x.indexOf(filterTag)+filterTag.length)).   //At this stage it looks like  , [Text,#To,Harry Arora so grabbing everything after #To,
       flatMap(_.split(",")).    //split comma separated emailIds into individual record to assign weightage
       map(_.concat(weightage))  //assign weightage 1.0 for 'To' emailIds and 0.5 for 'CC' emails
     )
     filteredIdDF.write.format("text").save(path)  // save the output as we can find top 100 recipients only once we processed all the records in cluster
   }
   
   createNsaveDF(saveToEmailPath, combinedEmailIds, "#To,", ",1.0")
   createNsaveDF(saveCCEmailPath, combinedEmailIds, "#CC,", ",0.5")

  sc.stop()
 }
}

