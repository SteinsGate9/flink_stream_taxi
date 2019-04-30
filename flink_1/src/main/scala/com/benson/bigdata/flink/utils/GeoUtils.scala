package com.benson.bigdata.flink.utils

class GeoUtils {
  // are
  val LON_WEST:Double  = 103.542195
  val LON_EAST:Double  = 104.128511
  val LAT_NORTH:Double  = 1.480820
  val LAT_SOUTH:Double  = 1.199148

  // width
  val LON_WIDH:Double = 0.00
  val LAT_HEIGHT:Double = 0.00

  // Delta
  val step:Int = 20

  // delta step to create artificial grid overlay of NYC
  val DELTA_LON = (LON_EAST-LON_WEST)/step
  val DELTA_LAT = (LAT_NORTH-LAT_SOUTH)/step

//  1033G08081300313, 008130030, 13/08/2008 00:31:00, 13/08/2008 00:54:00, 103.98989, 1.36118, 103.83672, 1.41517, SH1033G
  def isInSing( a:Float, b:Float ) : Boolean = {
     !(a>LON_EAST || a<LON_WEST) && !(b>LAT_NORTH || b<LAT_SOUTH)
  }

  def mapToGridCell(lon:Float, lat:Float) : Int = {
    val xIndex = Math.floor((Math.abs(lon) - Math.abs(LON_WEST)) / DELTA_LON).asInstanceOf[Int]
    val yIndex = Math.floor((LAT_NORTH - lat) / DELTA_LAT).asInstanceOf[Int]
    xIndex + (yIndex * step)
  }
}
