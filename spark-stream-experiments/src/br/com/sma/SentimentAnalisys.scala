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
          StructField("date",        DateStringType,false),
          StructField("negativo",        FloatType,false),
          StructField("positivo",          FloatType,false)
       )) 

       
  date,doc_id,cat,sub_cat,text     
 val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(schema)        
        .option("header", "true")
        .option("charset", "UTF8")
        .option("delimiter",",")
        .option("nullValue","")
        .option("treatEmptyValuesAsNulls","true")
        .load(filename)



val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

val regexTokenizer = new RegexTokenizer()
  .setInputCol("sentence")
  .setOutputCol("words")
  .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

val tokenized = tokenizer.transform(sentenceDataFrame).registerTempTable("tmp")


val rdd1 = sqlContext.sql("SELECT label, explode(words) as palavra FROM tmp").registerTempTable("sentencas")


val resultset = sqlContext.sql("SELECT *  FROM sentencas,dicionario WHERE sentencas.palavra = dicionario.palavra")

resultset.show()


    sc.stop()
  }
}