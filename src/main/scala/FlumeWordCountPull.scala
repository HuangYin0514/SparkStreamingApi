import org.apache.hadoop.util.bloom.HashFunction
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumeWordCountPull {
  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  //setMaster("local[2]")本地执行2个线程，一个用来接收消息，一个用来计算
  val conf: SparkConf = new SparkConf().setAppName("spark-streaming-wordcount").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))
  ssc.checkpoint("./checkpointStreaming")

  val updataFunction: Iterator[(String, Seq[Int], Option[Int])] => Iterator[(String, Int)] = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.flatMap {
      case (x, y, z) =>
        val count: Iterable[Int] = Some(y.sum + z.getOrElse(0))
        count.map(v => (x, v))
    }
  }


  def main(args: Array[String]): Unit = {
    //去服务器上拉取数据
    //    val fileContext: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "mini1", 9999)
    //flume 发送到其他机器中,  该例子为发送到windows电脑上
    val fileContext: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc,"192.168.25.1",9999)
    val StringContext: DStream[String] = fileContext.map(x => new String(x.event.getBody.array()))
    val wordAndOne: DStream[(String, Int)] = StringContext.flatMap(_.split(" ")).map((_, 1))
    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updataFunction, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), rememberPartitioner = true)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
