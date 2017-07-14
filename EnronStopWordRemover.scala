//spark-submit --class "EnronStopWordRemover"  target/scala-2.11/email_avg_2.11-0.1-SNAPSHOT.jar  "/home/mujeeb/Enron/edrm-enron-v2_arnold-j_xml/In/*.txt" "/home/mujeeb/Enron/edrm-enron-v2_arnold-j_xml/Out/"

//Removes stop words from the input email text files in the input directory and saves them with the same name in the output directory


//EnronStopWordRemover.scala

import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._

object EnronStopWordRemover {

  case class Sentence(value: String)

  def main(args: Array[String]): Unit = {

      val sparkConf = new SparkConf().setAppName("Enron Coding Challenge")
 
      val sc = new SparkContext(sparkConf)
      
      val spark = SparkSession.
          builder().
          appName("Enron email coding challenge").
          config("spark.some.config.option", "some-value").
          getOrCreate()

      val inPath = args(0)  //Input Path of file containing email text
      val outPath = args(1)    //Output Path of file containing email text
      val tempOutPath = outPath + "/tmp/"

      //val inPath = "/home/mujeeb/Enron/edrm-enron-v2_arnold-j_xml/In/"
      //val outPath = "/home/mujeeb/Enron/edrm-enron-v2_arnold-j_xml/Out/"

      val data = sc.wholeTextFiles(inPath)
      val files = data.map { case (filename, content) => filename}
      val fs = FileSystem.get(sc.hadoopConfiguration)

      files.collect.zipWithIndex.foreach { case(filename, idx) => {
          removeStopWords(filename, idx) }
      }

      def removeStopWords(file: String, index: Int) = { 

          println("********************   " + file)
          val sqlContext = new SQLContext(sc)
          import sqlContext.implicits._

          val tokenizer: RegexTokenizer = new RegexTokenizer()
              .setInputCol("raw")
              .setOutputCol("tokens_raw")
              .setToLowercase(false)

          val remover = new StopWordsRemover()
                        .setInputCol("tokens_raw")
                        .setOutputCol("tokens")

          val emailData = spark.read.textFile(file).as[Sentence].toDF("raw")  

          val tokenized = tokenizer.transform(emailData)
          val filtered = remover.transform(tokenized)

          val x = filtered.select("tokens").as[Array[String]].map(r => r.mkString(" "))

          x.rdd.saveAsTextFile(tempOutPath+index)

          val fname = file.substring(file.lastIndexOf("/")+1)

          fs.rename(new Path(tempOutPath+index+"/part-00000"), new Path(outPath+fname))

          fs.delete(new Path(tempOutPath), true)
      }


  sc.stop()
  }
}
