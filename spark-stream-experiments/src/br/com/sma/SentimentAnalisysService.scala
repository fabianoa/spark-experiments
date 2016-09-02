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
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext


object SentimentAnalisysService {

//	val conf = new SparkConf().setAppName("SentimentAnalisysWithParquet").setMaster("local[*]")
//	val sc = new SparkContext(conf)
//	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
	

    def loadDictionariesParquet(sqlContext: SQLContext){
	  
	      	val parquetFile = sqlContext.parquetFile("data/parquet/vocabulario_expandido")
					parquetFile.registerTempTable("dicionario_expandido")

					val parquetFileAnewbrT = sqlContext.parquetFile("data/parquet/ANEWbrT")
					parquetFileAnewbrT.registerTempTable("dicionario_anew")
					
        
	}      
          
          
//	   def loadDictionariesCSV{
//	        
//	      
//        def schema= StructType(Array(
//            StructField("palavra",        StringType,false),
//            StructField("negativo",        FloatType,false),
//            StructField("positivo",          FloatType,false)
//           )) 
//
//       val dfExpandido = sqlContext.read
//        .format("com.databricks.spark.csv")
//        .schema(schema)        
//        .option("header", "true")
//        .option("charset", "UTF8")
//        .option("delimiter",",")
//        .option("nullValue","")
//        .option("treatEmptyValuesAsNulls","true")
//        .load("data/clippings_expandido.csv")
//         
//        dfExpandido.registerTempTable("dicionario_expandido") 
//           
//	            // usage exampe -- a tpc-ds table called catalog_page
//          def schemaAnew= StructType(Array(
//                  StructField("palavra",        StringType,false),
//                  StructField("valencia_media", FloatType,false),
//                  StructField("valencia_dp", FloatType,false),
//                  StructField("alerta_media", FloatType,false),
//                  StructField("alerta_dp", FloatType,false)
//               )) 
//        
//          val dfAnew = sqlContext.read
//                .format("com.databricks.spark.csv")
//                .schema(schemaAnew)        
//                .option("header", "true")
//                .option("charset", "UTF8")
//                .option("delimiter",",")
//                .option("nullValue","")
//                .option("treatEmptyValuesAsNulls","true")
//                .load("data/ANEWbrT")
//             
//         dfAnew.registerTempTable("dicionario_anew")
//    
//	   }
	

			def ProcessaAnalise(dataFrame: DataFrame, nomeColunaTexto: String, sqlContext: SQLContext ): DataFrame = {

					System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");  

          loadDictionariesParquet(sqlContext)

								
				val regex = new RegexTokenizer().setInputCol(nomeColunaTexto).setOutputCol("words").setMinTokenLength(3).setPattern("[a-zA-Z']+")
        .setGaps(false) // alternatively .setPattern("\\w+").setGaps(false)
        val regexed = regex.transform(dataFrame)
       
        val ColunasDataFrame = dataFrame.columns.mkString(",")  
      
        val stopWords = sqlContext.sparkContext.textFile("data/stopwords-br.txt").cache().toArray()
       	val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("words").setOutputCol("filtered")
		    val stopwordRemoved = remover.transform(regexed).registerTempTable("clipping")
    
		    
        val pipeline = new Pipeline().setStages(Array( regex, remover))
        val model = pipeline.fit(dataFrame).transform(dataFrame)
      
      
                      
        

				val tokenizedClippingsExploded = sqlContext.sql("SELECT *,explode(filtered) as palavra  FROM clipping")
				tokenizedClippingsExploded.registerTempTable("clippings_explodidos")
        tokenizedClippingsExploded.show()
	          
          
				val resultset = sqlContext.sql("SELECT "++ColunasDataFrame++",clippings_explodidos.palavra as palavra,negativo,positivo,valencia_media,alerta_media   FROM clippings_explodidos LEFT OUTER JOIN dicionario_expandido ON  clippings_explodidos.palavra = dicionario_expandido.palavra LEFT OUTER JOIN dicionario_anew ON  clippings_explodidos.palavra = dicionario_anew.palavra")

					resultset.show(50)
					
					if(resultset.count()>0)
			     resultset.write.parquet("data/parquet/sentimento/"+System.currentTimeMillis())
//        
				
					return resultset

					
	}
}