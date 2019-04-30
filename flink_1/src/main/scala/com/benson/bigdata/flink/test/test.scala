package com.benson.bigdata.flink.test
import java.text.SimpleDateFormat
import java.util.Date

object test {
  def main(args: Array[String]): Unit = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date( "1502036122000".toLong))
    println(tim)
  }
}
