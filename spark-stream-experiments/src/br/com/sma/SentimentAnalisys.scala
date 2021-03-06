package br.com.sma



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// $example on$
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import java.text.Normalizer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.types.FloatType


object SentimentAnalisys {
  
 val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[*]")
 val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 

 def removerAcentos(s: String):  String = {

 return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
 
 } 
 
 
 def load_vocabulary(file_name: String): Unit ={
  
  
   val parquetFile = sqlContext.parquetFile(file_name)

   //Parquet files can also be registered as tables and then used in SQL statements.
   parquetFile.registerTempTable("parquetFile")
   
   val teenagers = sqlContext.sql("SELECT * FROM parquetFile")
teenagers.map(t => "Name: " + t(1)).collect().foreach(println)
       

   
 }
   
 
  
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");  
     
    
   val parquetFile = sqlContext.parquetFile("data/parquet/vocabulario_expandido")

   //Parquet files can also be registered as tables and then used in SQL statements.
   parquetFile.registerTempTable("dicionario")
   
    val parquetFileAnewbrT = sqlContext.parquetFile("data/parquet/ANEWbrT")

   //Parquet files can also be registered as tables and then used in SQL statements.
   parquetFileAnewbrT.registerTempTable("dicionario_anew")
   
    

   // usage exampe -- a tpc-ds table called catalog_page
  def schemaClippings= StructType(Array(
          StructField("date",        StringType,false),
          StructField("doc_id",        IntegerType,false),
          StructField("cat",          StringType,false),
          StructField("sub_cat",          StringType,false),
          StructField("text",          StringType,false)
       )) 


  val dfClippings = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(schemaClippings)        
        .option("header", "true")
        .option("charset", "UTF8")
        .option("delimiter",",")
        .option("nullValue","")
        .option("treatEmptyValuesAsNulls","true")
        .load("data/clippings_newformat.csv")
        
     // usage exampe -- a tpc-ds table called catalog_page
  def schemaAnew= StructType(Array(
          StructField("palavra",        StringType,false),
          StructField("valencia_media", FloatType,false),
          StructField("valencia_dp", FloatType,false),
          StructField("alerta_media", FloatType,false),
          StructField("alerta_dp", FloatType,false)
       )) 


  val dfAnew = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(schemaAnew)        
        .option("header", "true")
        .option("charset", "UTF8")
        .option("delimiter",",")
        .option("nullValue","")
        .option("treatEmptyValuesAsNulls","true")
        .load("data/ANEWbrT")
             
        
//dfAnew.registerTempTable("dicionario_anew")
           
dfAnew.show()        

val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

val tokenizedClippings = tokenizer.transform(dfClippings)

tokenizedClippings.registerTempTable("clipping")
 

val rdd1 = sqlContext.sql("SELECT date,doc_id,cat,sub_cat,text, explode(words) as palavra  FROM clipping").registerTempTable("sentencas")

val resultset_int1 = sqlContext.sql("SELECT * FROM sentencas  JOIN dicionario_anew ON  sentencas.palavra = dicionario_anew.palavra")

resultset_int1.show()

val resultset_int = sqlContext.sql("SELECT date,doc_id,cat,sub_cat,text,sentencas.palavra as palavra,negativo,positivo  FROM sentencas LEFT OUTER JOIN dicionario ON  sentencas.palavra = dicionario.palavra")

resultset_int.registerTempTable("passo1")


resultset_int.show()
//
//val resultset = sqlContext.sql("SELECT *  FROM passo1 LEFT OUTER JOIN dicionario_anew ON passo1.palavra = dicionario_anew.palavra")
//
//
//resultset.show()



    sc.stop()
  }
}