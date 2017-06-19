//build.sbt

name := "Enron Coding Challenge"
 
name := "email_avg"
 
scalaVersion := "2.11.0"
 
libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "com.databricks"   %% "spark-xml"  % "0.4.1",
  "org.apache.spark" %% "spark-sql"  % "2.0.0"
)

