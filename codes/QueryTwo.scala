package org.apache.spark.sql.simba.examples

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.functions.{unix_timestamp, date_format, hour, avg, countDistinct, asc}
import org.apache.spark.sql.types._

/**
  * Created by dongx on 3/7/2017.
  */
object QueryTwo {
  case class PointData(x: Double, y: Double, z: Double, other: String)
  case class Trajectory(id: Long, oid: Long, lon: Double, lat: Double, time: String, day:String, date:String, hour: Int)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("QueryOne")
      .config("simba.join.partitions", "64")
      .getOrCreate()

    query(simbaSession)
    simbaSession.stop()
  }

  private def query(simba: SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    val raw = simba.read
      .option("header", false)
      .format("csv")
      .load("trajectories.csv")
      .toDF("id", "oid", "lon", "lat", "time")
      .filter($"lon".isNotNull)
      .filter($"lat".isNotNull)

    //build data frame
    val df = raw.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4)))
      .toDF("id", "oid", "lon", "lat", "time")
      .select($"id", $"oid", $"lon", $"lat", $"time",
        date_format(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType), "EEEE").as("day"),
        date_format(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType), "dd-MM-yyyy").as("date"),
        hour(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType)).as("hour")
      )

    //Filter out non-workdays
    val workDays = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")
    val result = df.filter($"day".isin(workDays:_*))

    result.show(100)
    val ds = result.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble, row.getString(3).toDouble, row.getString(4), row.getString(5), row.getString(6), row.getInt(7)))
    ds.show()

    //Circle range query and count
    val count = ds.circleRange(Array("lon", "lat"), Array(-322357.0, 4463408.0), 2000.0).groupBy("hour").agg(countDistinct("id", "oid").as("count"))
    val cresult = count.sort(asc("hour"))
    cresult.show(24)
    val avgCount = cresult.select(avg($"count"))
    avgCount.show()

  }
}
