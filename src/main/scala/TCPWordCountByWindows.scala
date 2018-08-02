import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object TCPWordCountByWindows {
  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  //setMaster("local[2]")本地执行2个线程，一个用来接收消息，一个用来计算
  val conf: SparkConf = new SparkConf().setAppName("spark-streaming-wordcount").setMaster("local[2]")
  //创建spark的streaming,传入间隔多长时间处理一次，间隔在5秒左右，否则打印控制台信息会被冲掉
  val ssc = new StreamingContext(conf, Seconds(6))
  ssc.checkpoint("./checkpointStreaming")

  def main(args: Array[String]): Unit = {
    //读取数据的地址：从某个ip和端口收集数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("mini1", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pair: DStream[(String, Int)] = words.map(x => (x, 1))
    val wordCounts: DStream[(String, Int)] = pair.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(12),Seconds(6))
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }
}
