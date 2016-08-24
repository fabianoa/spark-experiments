package br.com.sma


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// $example on$
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

object Word2VecExample {
  
 def processLine(s: String, stopWords: Set[String]): Seq[String] = {

    s.replaceAll("[^a-zA-Z ]", " ")
      .toLowerCase()
      .split(" ")
      .filter(!stopWords.contains(_)).toSeq
}
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // $example on$
    val input = sc.textFile("data/b.txt").map(line => line.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split(" ").toSeq)
   
    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("salvação", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = Word2VecModel.load(sc, "myModelPath")
    // $example off$

    sc.stop()
  }
}