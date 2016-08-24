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
    
  
  
  val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
    // usage exampe -- a tpc-ds table called catalog_page
  def schema= StructType(Array(
          StructField("palavra",        StringType,false),
          StructField("negativo",        FloatType,false),
          StructField("positivo",          FloatType,false)
       )) 

       
       
  convert(sqlContext,
          "data/clippings_expandido.csv",  schema,        
          "vocabulario_expandido")
  
   

    sc.stop()
  }
}