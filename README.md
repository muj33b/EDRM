README.md

Spark (Scala) based solution for Enron email (EDRM) analysis. 

This solution was designed and tested with Enron EDRM XML files at https://archive.org/download/edrm.enron.email.data.set.v2.xml

It calculates average size of email text and calculates top 100 email recipients. This solution supports distributed XML files.

**Requirements:**

##### Spark 2.0+ 

##### Scala 2.11

##### Databricks XML Data Source for Apache Spark
    groupId: com.databricks
    artifactId: spark-xml_2.11
    version: 0.4.1

#### Enron EDRM XML data files 
    https://archive.org/download/edrm.enron.email.data.set.v2.xml

**Solution:**

There are 3 scala objects: 

###### EnronDFXMLEmailSize.scala	=> for Average email size 

###### EnronEmailToCC.scala			=> for extracting To and CC email Ids and 

###### EnronEmailCountDF.scala		=> for counting and finding top 100 email recipients from extracted Ids (EnronEmailToCC).



###### **1. EnronDFXMLEmailSize.scala**

    This class reads XML files in local or distributed filesystem with Spark DataFrame. It uses 2 accumulators
"Total Email Size" and "Total Number of Emails". Email sizes are extracted from "FileSize" attribute of FilePath="text_000" of ExternalFile tag
This object requires 1 input parameter which should specify the XML file path.

Average size of all the emails is printed on the stdout. eg. sample output
Enron Email Info
Total Size of Emails   : 4.7106064E7
Total Number of Emails : 17110
Average email size      : 2753.13056691993


###### **2. EnronEmailToCC.scala**

This object extracts 'To' and 'CC' email ids from the XML files from <Tags> xml tags
				<Tags>
					<Tag TagName="#From" TagDataType="Text" TagValue="Suresh Raghavan"/>
					<Tag TagName="#To" TagDataType="Text" TagValue="Brad Richter"/>
					<Tag TagName="#CC" TagDataType="Text" TagValue="Harry Arora"/>
                    ...
 
It assigns weightage 1.0 for 'To' emailIds and 0.5 for 'CC' email ids before writing it to disk to be picked up by counting job. 

This object requires 2 parameters:
    - Input XML file location
    - Output directory location
'To' email data is saved under 'ToEmail' subfolder and 'CC' email data is saved under 'CCEmail' folder


###### **3. EnronEmailCountDF.scala**

This object reads extracted email id data from the input parameter file location.
It uses Spark sql to calculate the count for each email ids and then sorts the data by count for top 100 recipients.

sample output

            emailId | emailCount 
--------------------|-------------
Arnold  John <Joh...|         290
          Ina Rangel|         277
slafontaine@globa...|         213
Arora  Harry <Har...|         190
     John J Lavorato|         172
         Harry Arora|         170
     Jennifer Fraser|         159
         John Arnold|         134
    Jennifer Medcalf|         126
      Matthew Arnold|         126
Jennifer White <j...|         118
      Jennifer Burns|         109
harry.arora@enron...|         102
       Brian Hoskins|         102
 John </O=ENRON/O...|         101
harry.arora@enron...|         100








