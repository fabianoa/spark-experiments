package br.com.sma
// Import all the libraries required
import org.apache.nifi._
import java.nio.charset._
import org.apache.nifi.spark._
import org.apache.nifi.remote.client._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.nifi.remote._
import org.apache.nifi.remote.client._
import org.apache.nifi.remote.protocol._
import org.apache.spark.storage._
import org.apache.spark.streaming.receiver._
import java.io._
import org.apache.spark.serializer._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.record.Record
import org.apache.avro.generic.GenericData.Record
import org.apache.spark.sql._
import java.util.Calendar

object SparkNiFiAttribute {
  
  val WINDOW_LENGTH = Minutes(4)
  val SLIDE_INTERVAL = Minutes(10)
  
    case class RSS(nomePeriodico:String,secaoPeriodico:String,estadoPeridodico:String,titulo:String,descricao:String,dataPublicacao:String)
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created

def main(args: Array[String]) {
    
    			System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");  
    
        // Build a Site-to-site client config with NiFi web url and output port name[spark created in step 6c]
val conf = new SiteToSiteClient.Builder().url("http://localhost:8090/nifi").portName("spark2").buildConfig()
// Set an App Name
val config = new SparkConf().setAppName("Nifi_Spark_Data").setMaster("local[*]")
val sc = new SparkContext(config)


val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._



// Create a  StreamingContext
val ssc = new StreamingContext(sc,SLIDE_INTERVAL)



        // Create a DStream using a NiFi receiver so that we can pull data from specified Port
val lines = ssc.receiverStream(new NiFiReceiver(conf, StorageLevel.MEMORY_ONLY))
     
//lines.window(WINDOW_LENGTH,SLIDE_INTERVAL)

lines.foreachRDD { (rdd: RDD[(NiFiDataPacket)], time: Time) => 
  
             import sqlContext.implicits._
              
              
             val myDataset=rdd.map( t => RSS(t.getAttributes.get("nomePeriodico"),t.getAttributes.get("secaoPeriodico"),t.getAttributes.get("estadoPeridodico"),TextUtils.removerAcentos(t.getAttributes.get("titulo")),TextUtils.removerAcentos(t.getAttributes.get("descricao")),t.getAttributes.get("dataPublicacao")) )
                                                                        .toDF()
                                                               
             SentimentAnalisysService.ProcessaAnalise(myDataset,"descricao",sqlContext) 
                                                                                                    
}


ssc.start()
ssc.awaitTermination()
}
}