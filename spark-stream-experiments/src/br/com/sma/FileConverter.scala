package br.com.sma

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.Normalizer
import org.apache.spark.ml._
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.rdd.RDD

object FileConverter {
  
   
 def convert(sqlContext: SQLContext, filename: String, schema: StructType, tablename: String) {
      // import text-based table first into a data frame.
      // make sure to use com.databricks:spark-csv version 1.3+ 
      // which has consistent treatment of empty strings as nulls.
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .schema(schema)        
        .option("header", "true")
        .option("charset", "UTF8")
        .option("delimiter",",")
        .option("nullValue","")
        .option("treatEmptyValuesAsNulls","true")
        .load(filename)
      // now simply write to a parquet file
      df.write.parquet("data/parquet/"+tablename)
  }
 
  
  def main(args: Array[String]): Unit = {
    
  System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");  
  
 
  val conf = new SparkConf().setAppName("FileConverter").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val input = sc.textFile("data/stopwords.txt").cache().map(line => TextUtils.removerAcentos(line).toLowerCase().split(" ").mkString)

 
 // val header: RDD[String] = sc.parallelize(Array("palavra,valencia_media,valencia_dp,alerta_media,alerta_dp"))
 
  // header.union(input).coalesce(1, true).saveAsTextFile("data/ANEW-Br-tratado.txt") 
    input.coalesce(1, true).saveAsTextFile("data/stopwords-br") 

   
     // usage exampe -- a tpc-ds table called catalog_page
  def schemaAnew= StructType(Array(
          StructField("palavra",        StringType,false),
          StructField("valencia_media", FloatType,false),
          StructField("valencia_dp", FloatType,false),
          StructField("alerta_media", FloatType,false),
          StructField("alerta_dp", FloatType,false)
       )) 
       
       
//  convert(sqlContext,
//          "data/ANEWbrT",  schemaAnew,        
//          "ANEWbrT")
//  
   
    sc.stop()
  }
}