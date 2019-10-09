package org.apache.spark.sql.simba.examples

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * Created by dongx on 3/7/2017.
  */
object RTreeIndex {
  case class PointData(x: Double, y: Double, z: Double, other: String)
  case class Trajectory(id: Long, oid: Long, x: Double, y: Double)
  case class PartitionMBR(minX: Double, minY: Double, maxX: Double, maxY: Double)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("IndexExample")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    val mbrs = buildIndex(simbaSession)
    writeMBRS(mbrs)
    simbaSession.stop()
  }

  private def buildIndex(simba: SimbaSession): Seq[PartitionMBR] = {
    import simba.implicits._
    val data = simba.read
      .format("csv")
      .option("header", "false")
      .option("mode", "DROPMALFORMED")
      .load("trajectories.csv")
      .sample(true, 0.1)
      .toDF("id", "oid", "x", "y", "time")
    val ds = data.map(row => Trajectory(
        row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble, row.getString(3).toDouble))

    import simba.simbaImplicits._

    //build R tree index
    ds.index(RTreeType, "indexForTable", Array("x", "y"))

    //Loop through MBRs
    val mbrs: Seq[PartitionMBR] = ds.mapPartitions(iter => {
      var minX = Double.MaxValue
      var minY = Double.MaxValue
      var maxX = Double.MinValue
      var maxY = Double.MinValue

      while (iter.hasNext) {
        val poi = iter.next()
        if (minX > poi.x) minX = poi.x
        if (maxX < poi.x) maxX = poi.x
        if (minY > poi.y) minY = poi.y
        if (maxY < poi.y) maxY = poi.y
      }
      List(PartitionMBR(minX, minY, maxX, maxY)).iterator
    }).collect()

    //Write points to file
      val pointBW = new BufferedWriter(new FileWriter(new File("points.csv")))
      ds.collect().foreach(poi => {
         // pointBW.write(((poi.x - minX) / (maxX - minX) * 500).floor.toInt + "," + ((poi.y - minY) / (maxY - minY) * (500 * (maxY - minY)/(maxX - minX))).floor.toInt + "\n")
          pointBW.write(poi.x + "," + poi.y + "\n")
        }
      )
      pointBW.close()
    return mbrs
  }

  private def writeMBRS(mbrs: Seq[PartitionMBR]): Unit = {
    import java.awt.image.BufferedImage
    import java.awt.{Graphics2D,Color,Font,BasicStroke}
    import java.awt.geom._

    //Write MBRs to file
    val mbrBW = new BufferedWriter(new FileWriter(new File("mbrs.csv")))
    mbrs.foreach(
      mbr => {
        mbrBW.write(mbr.minX + "," +  mbr.maxX + "," +  mbr.minY + "," + mbr.maxY + "\n")
      }
    )
    mbrBW.close()

  }
}
