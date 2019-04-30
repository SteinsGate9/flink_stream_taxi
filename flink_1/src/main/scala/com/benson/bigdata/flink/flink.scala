package com.benson.bigdata.flink

// alreadt
import java.io._
import java.util.zip.{GZIPInputStream, ZipEntry, ZipFile, ZipInputStream}

import com.benson.bigdata.flink.sources._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector
import java.text.SimpleDateFormat

import org.joda.time.DateTime
import java.util.Date

import scala.collection.mutable
import scala.io.Source
import scala.collection.mutable.{ListBuffer, Set}
// add
import utils.GeoUtils
import sources.TaxiRideSource
import org.apache.flink.streaming.api.TimeCharacteristic
import datatypes.TaxiRide

object flink {


  class MyTimestampsAndWatermarks()

  class MyWindowFunction extends WindowFunction[(String, Long), String, String, TimeWindow] {
    def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
      var count = 0L
      for (in <- input) {
        count = count + 1
      }
      out.collect(s"Window $window count: $count")
    }
  }

  class MyWindowFunction2 extends WindowFunction[(String, Int), String, String, TimeWindow] {
    def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[String]): Unit = {
      var count = ""
      for (in <- input) {
        count = count + in._2
      }
      out.collect(s"Window:$window| Taxi:$key| count:$count")
    }
  }

  class Assigner extends AssignerWithPeriodicWatermarks[TaxiRide] {
    val format = new SimpleDateFormat("yyyy-mm-dd  HH:mm:ss")
    var currentMaxTimestamp = 0L
    val maxOutOfOrderness = 10000L
    var a: Watermark = null

    override def getCurrentWatermark: Watermark = {
      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      a
    }

    override def extractTimestamp(t: TaxiRide, l: Long): Long = {
      val timestamp = t.startTime.getMillis
      currentMaxTimestamp = timestamp.compareTo(currentMaxTimestamp) match {
        case 1 => timestamp
        case _ => currentMaxTimestamp
      }
//      println("timestamp:" + timestamp + "|" + t.startLon + t.startLat)
      timestamp
    }
  }

  def isInSing(a: Float, b: Float): Boolean = {
    val LON_WEST: Double = 103.542195
    val LON_EAST: Double = 104.128511
    val LAT_NORTH: Double = 1.480820
    val LAT_SOUTH: Double = 1.199148
    !(a > LON_EAST || a < LON_WEST) && !(b > LAT_NORTH || b < LAT_SOUTH)
  }

  def mapToGridCell(lon: Float, lat: Float): Int = {
    val LON_WEST: Double = 103.542195
    val LON_EAST: Double = 104.128511
    val LAT_NORTH: Double = 1.480820
    val LAT_SOUTH: Double = 1.199148
    val step: Int = 20
    val DELTA_LON = (LON_EAST - LON_WEST) / step
    val DELTA_LAT = (LAT_NORTH - LAT_SOUTH) / step
    val xIndex = Math.floor((Math.abs(lon) - Math.abs(LON_WEST)) / DELTA_LON).asInstanceOf[Int]
    val yIndex = Math.floor((LAT_NORTH - lat) / DELTA_LAT).asInstanceOf[Int]
    xIndex + (yIndex * step)
  }

  def count_hot(key: String, window: TimeWindow, in: Iterable[Tuple2[String, Int]], out: Collector[Tuple2[ListBuffer[Int], Long]]): Unit = {
    var lis:ListBuffer[Int] = ListBuffer()
    var pre = -100
    for (v <- in) {
      if (v._2 != pre)
      {
        lis = lis :+ v._2
        pre = v._2
      }
    }
    out.collect(lis, window.getStart)
  }

  def count_hot2(window: TimeWindow, in: Iterable[(ListBuffer[Int], Long)], out: Collector[(String, mutable.Map[Int, mutable.Map[Long, Int]])] ): Unit = {
    val map :mutable.Map[Int, mutable.Map[Long, Int]] = mutable.Map()
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for ( timeList <- in)
      for( gg <- timeList._1){
        val time = (timeList._2-window.getStart)/600000 + 1
        if (map.contains(gg))
          if(map(gg).contains(time))
            map(gg)(time) += 1
          else
            map(gg) += time->1
        else
        { map += (gg-> mutable.HashMap(time->1))}
    }
    out.collect((fm.format(window.getStart), map))
  }

  def count_route_first(key: String, window: TimeWindow, in: Iterable[Tuple2[String, Int]], out: Collector[(String, ListBuffer[Int])]): Unit = {
    var lis:ListBuffer[Int] = ListBuffer()
    var pre = -100
    for (v <- in) {
      if (v._2 != pre)
        {
          lis = lis :+ v._2
          pre = v._2
        }
    }
//    println(s"key$key"+"  "+lis)
    out.collect((key, lis))
  }

  def count_route_2first (window: TimeWindow, in: Iterable[(String, ListBuffer[Int])], out: Collector[(String, mutable.Map[ ListBuffer[Int] ,Int])]): Unit = {
    val sorting = false
    val minCount = 5
    val minLengh = 3
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    // read into map
    val con:mutable.Map[String, ListBuffer[Int]] = mutable.Map()
    in.map( gg => {
      con.contains(gg._1) match {
        case true => con(gg._1) +: gg._2
        case false => con += (gg._1 -> gg._2)
      }
    })
    // input process
    val ne = con.map(_._2).filter(_.length >=minLengh )
    val map:mutable.Map[ ListBuffer[Int] ,Int ] = mutable.Map()
    for(in <- ne)
      for(i <- minLengh to in.size)
        for(j <- 0 to in.size-i) {
             val gg = in.slice(j, j + i)
             if (map.contains(gg))
               map(gg) += 1
             else
               map += (gg -> 1)
          }
    // output process
    println(s"Window:"+fm.format(new Date(window.getStart)))
    val newmap = map.filter( _._2 >= minCount )
    for (i <- newmap.keys)
      println("    "+ i,newmap(i))
    out.collect((fm.format(window.getStart), newmap))
  }

  def count_route_second(key: String, window: TimeWindow, in: Iterable[Tuple2[String, Int]], out: Collector[(String, ListBuffer[Item])]): Unit = {
    var lis:ListBuffer[Item] = ListBuffer()
    var pre:Int = -100
    for (v <- in) {
//      lis = lis :+ new Item(v._2.toString, 0)
      if(v._2 != pre) {
        lis = lis :+ new Item(v._2.toString, 0)
        pre = v._2
      }
    }
//    println(s"key$key"+lis.map(_.name))
    out.collect((key, lis))
  }

  def count_route_2second (window: TimeWindow, in: Iterable[(String ,ListBuffer[Item])], out: Collector[(String, mutable.Map[ListBuffer[Item], Int])]): Unit = {
    //This is for local FPGrowth if someone wants to run FPGrowth in local
    var con:mutable.Map[String, ListBuffer[Item]] = mutable.Map()
    val sorting = true
    val minCount = 5
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    // generate fp tree
    in.map( gg => {
      con.contains(gg._1) match {
        case true => con(gg._1) +: gg._2
        case false => con += (gg._1 -> gg._2)
      }
    })
    val in2 = con.toList.map( _._2 ).filter( _.length >= 3 )
    val fpGrowthLocal: FPGrowth = new FPGrowth(in2, minCount, sorting)
    //Result result
    val result = fpGrowthLocal.getFrequentItemsets()

    println(s"Window:"+fm.format(new Date(window.getStart)))
    for (i <- result)
      {
      println("   Route:"+i._1.map(_.name) +"    Times:"+i._2)
      }
    out.collect((fm.format(new Date(window.getStart)), result))
  }

//  def count_route_2third (window: TimeWindow, in: Iterable[ListBuffer[Item]], out: Collector[(String, mutable.Map[ListBuffer[Item], Int])]): Unit = {
//    var lis:ListBuffer[Item] = ListBuffer()
//    var pre:Int = -100
//    for (v <- in) {
//      if(v._2 != pre) {
//        lis = lis :+ new Item(v._2.toString, 0)
//        pre = v._2
//      }
//    }
//    out.collect((window.getStart.toString, lis))
//  }

  def main(args: Array[String]): Unit = {
    // env
    val input = args(0)
    val output = args(1)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)
    println("LOADED ENV")

    /***************task0***************************************************************************/
//    val zip_path = "file:///root/logs.zip"
//    val zip = env.addSource(new TaxiRideSource(zip_path, 60, 600))
//    val count = zip.filter { gg => isInSing(gg.startLon, gg.startLat) }
//      .map { gg => (mapToGridCell(gg.startLon, gg.startLat), 1) }
//      .keyBy(t => t._1)
//      .timeWindow(Time.minutes(30),Time.minutes(15))
//      .apply{ count_hot _ }
//    count.print()
//    env.execute()
//    println("PRINTED DATA")
    /******************************************************************************************/
    /*************task1****************************************************************/
//    val txt_path = "F:\\dasanxia\\Bigdata\\flink_1\\data\\das.txt"
//    val txt_path2 = "hdfs://hadoop1:9000/user/root/logs_2.txt"
//    val zip = env.readTextFile(input)
//    val start = System.nanoTime
//    val inputMap = zip.map( TaxiRide.fromString(_) )
//      .filter{ gg => isInSing(gg.startLon, gg.startLat) }
//      .assignTimestampsAndWatermarks( new Assigner())
//      .map{ gg => (gg.taxiId, mapToGridCell(gg.startLon, gg.startLat)) }
//      .keyBy( t => t._1 )
//      .timeWindow( Time.minutes(10),Time.minutes(10))
//      .apply{ count_hot _ }
//      .timeWindowAll( Time.minutes(60),Time.minutes(30))
//      .apply{ count_hot2 _ }
//      .writeAsCsv(output, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
//
//    env.execute()
//    val end = System.nanoTime
//    println("PRINTED DATA IN "+ (end-start))
    /********************************************************************************/
    /*************task2**************************************************************/
//    val txt_path = "F:\\dasanxia\\Bigdata\\flink_1\\data\\logs.txt"
//    val txt_path2 = "hdfs://hadoop1:9000/user/root/logs.txt"
//    val zip = env.readTextFile(txt_path)
//    val start = System.nanoTime
//    val inputMap = zip.map( TaxiRide.fromString(_) )
//      .filter { gg => isInSing(gg.startLon, gg.startLat) }
//      .assignTimestampsAndWatermarks(new Assigner())
//      .map { gg => (gg.taxiId, mapToGridCell(gg.startLon, gg.startLat)) }
//      .keyBy( t => t._1 )
//      .timeWindow( Time.minutes(10),Time.minutes(10) )
//      .apply{ count_route_second _ }
//      .timeWindowAll( Time.minutes(60),Time.minutes(30) )
//      .apply( count_route_2second _)
//      //      .print()
////      .writeAsText("hdfs://hadoop1:9000/user/root/out_2.txt").setParallelism(1)
//    env.execute()
//    val end = System.nanoTime
//    println("PRINTED DATA IN "+ (end-start))
    /***************************************************************************/
    /***********************tesk3*******************************************/
        val txt_path = "F:\\dasanxia\\Bigdata\\flink_1\\data\\das.txt"
        val txt_path2 = "hdfs://hadoop1:9000/user/root/logs.txt"
        val zip = env.readTextFile(input)
        val start = System.nanoTime
        val inputMap = zip.map( TaxiRide.fromString(_) )
          .filter { gg => isInSing(gg.startLon, gg.startLat) }
          .assignTimestampsAndWatermarks(new Assigner())
          .map { gg => (gg.taxiId, mapToGridCell(gg.startLon, gg.startLat)) }
          .keyBy( t => t._1 )
          .timeWindow( Time.minutes(10),Time.minutes(10) )
          .apply{ count_route_first _ }
          .timeWindowAll( Time.minutes(60),Time.minutes(30) )
          .apply( count_route_2first _)
//          .print()
          .writeAsCsv(output, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
        env.execute()
        val end = System.nanoTime
        println("PRINTED DATA IN "+ (end-start))


  }
}
