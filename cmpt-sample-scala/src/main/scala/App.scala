/**
 * Created by White on 2016/11/07.
 */
object App {

  func()
  println(func("Scala"))
  println(func1().apply())
  println(func1()())
  println(func1(() => "Hello"))
  println(func(99))

  def func() {
    println("Hello")
  }

  def func(string: String) = string

  def func1(): () => String = {
    () => "!"
  }

  def func1(func: () => String) = func()

  implicit def int2String(int: Int): String = int toString

  def main(args: Array[String]): Unit = {
    println(new Simpling with Incrementing with Doubling put 1 get)
    println(new Simpling with Doubling with Incrementing put 1 get)
  }

  trait Incrementing extends Register {

    abstract override def put(value: Int): Register = {
      super.put(value + 1)
      this
    }
  }

  trait Doubling extends Register {

    abstract override def put(value: Int): Register = {
      super.put(value * 2)
      this
    }
  }

  abstract class Register {

    def get: Int

    def put(value: Int): Register
  }

  class Simpling extends Register {

    private var value = 0

    override def get: Int = value

    override def put(value: Int): Register = {
      this.value = value
      this
    }
  }

}
