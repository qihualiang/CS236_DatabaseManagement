import java.sql.Timestamp

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{date_format, desc, hour, unix_timestamp}
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.{StructType, TimestampType}

object Part5 {

  case class POI(piad: Long, tags: String, poi_lon: Double, poi_lat: Double)
  case class TRAJ(tid: Long, oid: Long, t_lon: Double, t_lat:Double, time: Timestamp)
  case class TRAJ1(tid: Long, oid: Long, t_lon1: Double, t_lat1:Double, time: Timestamp)
  case class Trajectory(id: Long, oid: Long, lon: Double, lat: Double, time: String, day:String, date:String, month:String, year:String, hour: Int)
  case class TrajPOI(piad: Long, tags: String, poi_lon: Double, poi_lat: Double,id: Long, oid: Long, lon: Double, lat: Double, time: String, day:String, date:String, month:String, year:String, hour: Int)

  def main(args: Array[String]): Unit = {
    val start_time=System.currentTimeMillis()
    val simba = SimbaSession
      .builder()
      .master("local[*]") //Change to 1, 2, 3, 4 for number of cores
      .appName("Project")
      .config("simba.index.partitions", "16")
      .getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._

    val schema = ScalaReflection.schemaFor[TRAJ].dataType.asInstanceOf[StructType]

    val trajectory = simba.read.
      option("header", "false").
//      schema(schema).
      format("com.databricks.spark.csv").
      //Input file path for trajectories
      load("trajectories.csv").
      toDF("id", "oid", "t_lon", "t_lat", "time")

    val ds = trajectory.map(row => (row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble, row.getString(3).toDouble, row.getString(4)))
  .toDF("id", "oid", "t_lon", "t_lat", "time")
  .select($"id", $"oid", $"t_lon", $"t_lat", $"time",
    date_format(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType), "EEEE").as("day"),
    date_format(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType), "dd-MM-yyyy").as("date"),
    date_format(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType), "MM").as("month"),
    date_format(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType), "yyyy").as("year"),
    hour(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType)).as("hour")
  )
    val trajIndex = ds.index(RTreeType,"tpRTree",Array("t_lon","t_lat"))


    val schema2 = ScalaReflection.schemaFor[POI].dataType.asInstanceOf[StructType]

    val pois = simba.read.
      option("header", "false").
//      schema(schema2).
      format("com.databricks.spark.csv").
      //Input file path for POI file
      load("POIs.csv").
      toDF("piad", "tags", "poi_lon", "poi_lat")

    val pds = pois.map(row => (row.getString(0).toLong, row.getString(1), row.getString(2).toDouble, row.getString(3).toDouble)).toDF("piad","tags","poi_lon","poi_lat")
    val poisIndex = pds.index(RTreeType,"pointsRTree",Array("poi_lon","poi_lat"))


    //Perform distance join between POI and Trajectory datasets with distance 100
    //Change radius for 100, 200, 300, 400, 500
    val joinTrajPOI = poisIndex.distanceJoin(trajIndex,Array("poi_lon","poi_lat"),Array("t_lon","t_lat"),100.00).distinct()

    val workDays = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")

    //Filter only for workdays
    val result = joinTrajPOI.filter($"day".isin(workDays:_*))

    //Filter for the year 2009
    val yresult2 = result.filter($"year".contains("2009"))

    //Groupby POI lat and long as well as month to get count of the interesting points sorted in descending order
    val gp2 = yresult2.groupBy("poi_lon","poi_lat","month").count().orderBy(desc("count"))

    val yresult1 = result.filter($"year".contains("2008"))

    val gp1 = yresult1.groupBy("poi_lon","poi_lat","month").count().orderBy(desc("count"))

    println("Time Taken to execute operations : ")
    println(System.currentTimeMillis()-start_time)

    gp2.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header","true")
      .save("./output//part5-2009") //Output File path

    //Uncomment to perform the same for the year 2008



    gp1.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header","true")
      .save("./output//part5-2008") //Output File path
//    gp1.show(10)
    simba.stop()
  }
}
