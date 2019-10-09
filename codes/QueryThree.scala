package org.apache.spark.sql.simba.examples

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.Timestamp

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.functions.{avg, countDistinct, date_format, hour, max, min, unix_timestamp, when}
import org.apache.spark.sql.types._

/**
  * Created by dongx on 3/7/2017.
  */
object QueryThree {

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

    val l = -339220.0
    val r = -309375.0
    val u = 4444725
    val d = 4478070
    val lonMid = (l + r) / 2
    val latMid = (u + d) /2

    val raw = simba.read
      .option("header", false)
      .format("csv")
      .load("trajectories.csv")
      .toDF("id", "oid", "lon", "lat", "time")
      .filter($"lon".isNotNull)
      .filter($"lat".isNotNull)

    val df = raw.map(row => (row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble, row.getString(3).toDouble, row.getString(4)))
      .toDF("id", "oid", "lon", "lat", "timeRaw")
      .range(Array("lon", "lat"), Array(l, u), Array(r, d))
      .select($"id", $"oid", $"lon", $"lat", $"timeRaw", unix_timestamp($"timeRaw", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("time")
      )
      //add numeric quadrant number
      .withColumn("quadrant",
        (when($"lon" <= lonMid && $"lat" <= latMid, 0)
          .otherwise(when($"lon" <= lonMid && $"lat" > latMid, 1)
              .otherwise(when($"lon" > lonMid && $"lat" <= latMid, 2)
                .otherwise(3)
        ))))

    //Find start of trajectories and end of trajectories
    val t1 = df.groupBy($"id").agg(min("time").alias("min"), max("time").alias("max"))
    val t2 = t1.as("t1").join(df.as("df"), $"t1.id" === $"df.id" && $"t1.min" === $"df.time")
        .select($"t1.id".as("id"), $"t1.min".as("min"), $"t1.max".as("max"), $"df.quadrant".as("startQuadrant"))
    val t3 = t2.as("t2").join(df.as("df"), $"t2.id" === $"df.id" && $"t2.max" === $"df.time")
        .select($"t2.id".as("id"), $"t2.min".as("min"), $"t2.max".as("max"), $"t2.startQuadrant".as("startQuadrant"),
          $"df.quadrant".as("endQuadrant")
        )
    t3.show()

    //Find all trajectories that start and end in the same quadrant
    val t4 = t3.withColumn("sameQuadrant", (when($"startQuadrant" === $"endQuadrant", true).otherwise(false)))
    val result = t4.groupBy("sameQuadrant").count()
    result.show()

  }
}
