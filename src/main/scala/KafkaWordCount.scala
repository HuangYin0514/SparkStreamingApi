import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {

  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  //setMaster("local[2]")本地执行2个线程，一个用来接收消息，一个用来计算
  val conf: SparkConf = new SparkConf().setAppName("spark-streaming-wordcount").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))
  ssc.checkpoint("./checkpointStreaming")

  val updateByKey: Iterator[(String, Seq[Int], Option[Int])] => Iterator[(String, Int)] = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map {
      case (x, y, z) => {
        val count: Int = y.sum + z.getOrElse(0)
        (x, count)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, groupId, topics, numThread) = Array[String]("mini2:2181,mini3:2181", "m1", "kafka_sparkStreaming_test", "2")
    val topicMap: Map[String, Int] = topics.split(" ").map((_, numThread.toInt)).toMap
    val receiverMsg: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)
    val lines: DStream[String] = receiverMsg.map(_._2)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateByKey, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), true)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
