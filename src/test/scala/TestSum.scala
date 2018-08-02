import org.apache.commons.lang.ArrayUtils

import scala.collection.mutable.ArrayBuffer

object TestSum {
  def main(args: Array[String]): Unit = {

    val arr = Array(1, 2, 3, 4)
    val sum: Int = arr.sum
    //    println(sum)
    val chars = ArrayBuffer('a', 'b')

    val s = "abcd"
    val bytes: Array[Byte] = s.getBytes()
    val str = new String(bytes)
    //    println(bytes(1))

    val tuples = Array(("a", 1), ("b", 1))
   val tuples2: Array[(String, Int)] = tuples.flatMap(x => Some(x._1,x._2))
    val someTuples: Array[Some[(String, Int)]] = tuples.map(x => Some(x._1,x._2))
    println(tuples2.toList)



  }

}
