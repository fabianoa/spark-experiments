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
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.types.TimestampType


object SentimentAnalisysWithParquet {

	val conf = new SparkConf().setAppName("SentimentAnalisysWithParquet").setMaster("local[*]")
	val sc = new SparkContext(conf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)


      def loadDictionariesParquet{
	  
	      	val parquetFile = sqlContext.parquetFile("data/parquet/vocabulario_expandido")
					parquetFile.registerTempTable("dicionario_expandido")

					val parquetFileAnewbrT = sqlContext.parquetFile("data/parquet/ANEWbrT")
					parquetFileAnewbrT.registerTempTable("dicionario_anew")
	  
	   }
	     
	   def loadDictionariesCSV{
	        
	      
        def schema= StructType(Array(
            StructField("palavra",        StringType,false),
            StructField("negativo",        FloatType,false),
            StructField("positivo",          FloatType,false)
           )) 

       val dfExpandido = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(schema)        
        .option("header", "true")
        .option("charset", "UTF8")
        .option("delimiter",",")
        .option("nullValue","")
        .option("treatEmptyValuesAsNulls","true")
        .load("data/clippings_expandido.csv")
         
        dfExpandido.registerTempTable("dicionario_expandido") 
           
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
             
         dfAnew.registerTempTable("dicionario_anew")
    
	   }
	

			def main(args: Array[String]): Unit = {

					System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");  

          loadDictionariesParquet

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

           val newDf = dfClippings.withColumn("uniqueIdColumn", monotonicallyIncreasingId())		
						
      
					val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
					val tokenizedClippings = tokenizer.transform(newDf)
		    			tokenizedClippings.registerTempTable("clipping")

					val tokenizedClippingsExploded = sqlContext.sql("SELECT uniqueIdColumn,date,doc_id,cat,sub_cat,text, explode(words) as palavra  FROM clipping").registerTempTable("clippings_explodidos")

					val resultset = sqlContext.sql("SELECT uniqueIdColumn as doc_id , date,cat,sub_cat,text,clippings_explodidos.palavra as palavra,negativo,positivo,valencia_media,valencia_dp,alerta_media,alerta_dp  FROM clippings_explodidos LEFT OUTER JOIN dicionario_expandido ON  clippings_explodidos.palavra = dicionario_expandido.palavra LEFT OUTER JOIN dicionario_anew ON  clippings_explodidos.palavra = dicionario_anew.palavra")

					resultset.show(50)
					
					//resultset.select("palavra","valencia_media").distinct().filter("valencia_media is not null").orderBy("palavra").groupBy("palavra","valencia_media").count().show(1000)
					
					
					//resultset.select("palavra","valencia_media").filter("valencia_media is not null").orderBy("palavra").groupBy("palavra","valencia_media").count().orderBy("count").show(1000)
					
					 resultset.write.parquet("data/parquet/sentimento")

				 sc.stop()
	}
}