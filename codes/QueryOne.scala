package org.apache.spark.sql.simba.examples

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.simba.SimbaSession

/**
  * Created by dongx on 3/7/2017.
  */
object QueryOne {
  case class PointData(x: Double, y: Double, z: Double, other: String)

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
      .load("POIs.csv")
      .toDF("id", "name", "lon", "lat")
      .filter($"lon".isNotNull)
      .filter($"lat".isNotNull)

    val df = raw.map(row => (row.getString(0), row.getString(1), row.getString(2).toDouble, row.getString(3).toDouble))
      .toDF("id", "name", "lon", "lat")

    //range query and filter out non-restaurant
    val result = df.range(Array("lon", "lat"), Array(-339220.0, 4444725.0), Array(-309375.0, 4478070.0))
      .filter($"name".contains("restaurant"))

    result.show()

    val resultBW = new BufferedWriter(new FileWriter(new File("queryOne.csv")))

    result.collect().foreach(poi => resultBW.write(poi.getString(0) + "," + poi.getString(1) + "," + poi.getDouble(2) + "," +
      poi.getDouble(3) + "\n"
    ))
    resultBW.close()
  }
}
