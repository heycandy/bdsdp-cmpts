import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017.1.19.
 */
object TestScala {

  def main(args: Array[String]) {

    val arr = ArrayBuffer[Int]()

    val str = new StringBuffer()
    str.append("a")
    str.append("b")

    val str2 = "a"
    str2 = "b"

    var str3 = "a"
    str3 = "b"



    arr += 1
    arr +=(2, 5, 4)

    for (elem <- arr) {
      println(elem)
    }
  }

}
