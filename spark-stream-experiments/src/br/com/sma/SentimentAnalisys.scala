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
   
   
    
    
val sentenceDataFrame = sqlContext.createDataFrame(Seq(
  (0, "Este governo esta muito ruim"),
  (1, "Temos um bom resultado nas contas publicas este semestre"),
  (2, "A saca do cafe esta em otima")
)).toDF("label", "sentence")

   // usage exampe -- a tpc-ds table called catalog_page
  def schema= StructType(Array(
          StructField("date",        StringType,false),
          StructField("doc_id",        IntegerType,false),
          StructField("cat",          StringType,false),
          StructField("sub_cat",          StringType,false),
          StructField("text",          StringType,false)
       )) 


 val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(schema)        
        .option("header", "true")
        .option("charset", "UTF8")
        .option("delimiter",",")
        .option("nullValue","")
        .option("treatEmptyValuesAsNulls","true")
        .load("data/clippings_newformat.csv")

val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")




val tokenized = tokenizer.transform(df).registerTempTable("tmp")



val rdd1 = sqlContext.sql("SELECT date,doc_id,cat,sub_cat, explode(words) as palavra FROM tmp").registerTempTable("sentencas")

//val rdd1 = sqlContext.sql("SELECT label, explode(words) as palavra FROM tmp").registerTempTable("sentencas")


val resultset = sqlContext.sql("SELECT *  FROM sentencas,dicionario WHERE sentencas.palavra = dicionario.palavra")

resultset.show()


    sc.stop()
  }
}