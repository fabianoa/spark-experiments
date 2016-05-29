package br.com.sma

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object LogAnalizer {
  

  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Log Analyzer in Scala").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    
    	//	Load	the	text	file	into	Spark.
		if	(args.length	==	0)	{
						System.out.println("Must	specify	an	access	logs	file.");
						System.exit(-1);
		}
		
    
    val logFile = args(0)
    
				
		val	logLines	=	sc.textFile(logFile);

    val accessLogs = logLines.map(ApacheAccessLog.parseLogLine).cache()

    val contentSizes = accessLogs.map(log => log.contentSize).cache()
    
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizes.reduce(_ + _) / contentSizes.count,
      contentSizes.min,
      contentSizes.max))
      
    sc.stop()
    
    
    // Calculate statistics based on the content size.
    //val contentSizes = accessLogs.map(log => log.contentSize).cache()
    
    
  }
}