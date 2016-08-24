package br.com.sma


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// $example on$
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import java.text.Normalizer


object Word2VecExample {
  
 val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[*]")
 val sc = new SparkContext(conf)

 def removerAcentos(s: String):  String = {

 return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
 
 } 
 
 
 def load_vocabulary(file_name: String): Unit ={
  
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
   val parquetFile = sqlContext.parquetFile(file_name)

//Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerTempTable("parquetFile")
val teenagers = sqlContext.sql("SELECT * FROM parquetFile")
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
       

   
 }
   
 
  
  def main(args: Array[String]): Unit = {
    
      System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");  
    
    load_vocabulary("data/parquet/vocabulario_expandido")
  
    // $example on$
    val input = sc.textFile("data/b.txt").map(line => removerAcentos(line).toLowerCase().split(" ").toSeq)
   
//    input.take(10).foreach { 
//         print 
//      }
//     
    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms(removerAcentos("salvação"), 10)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sc, "myModelPath")
   // val sameModel = Word2VecModel.load(sc, "myModelPath")
    // $example off$

    sc.stop()
  }
}