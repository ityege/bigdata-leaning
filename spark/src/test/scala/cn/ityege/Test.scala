package cn.ityege

object Test {
  def main(args: Array[String]): Unit = {
    val a = 1 to 100 by 2
    for (elem <- a if elem < 50) {
      print(elem + "\t")
    }
    val b = for (i <- 1 to 100 if i % 2 == 0) yield i * i
    println(b)
  }
}
