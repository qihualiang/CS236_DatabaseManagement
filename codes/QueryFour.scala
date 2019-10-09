import java.sql.Timestamp

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType

object Part4 {
  case class POI(piad: Long, tags: String, poi_lon: Double, poi_lat: Double)
  case class TRAJ(tid: Long, oid: Long, t_lon: Double, t_lat:Double, time: Timestamp)
  case class TRAJ1(tid: Long, oid: Long, t_lon1: Double, t_lat1:Double, time: Timestamp)
  def main(args: Array[String]): Unit = {

        val start_time = System.currentTimeMillis()
        val simba = SimbaSession
          .builder()
          .master("local[*]") //Change to 1, 2, 3, 4 for number of cores
          .appName("Project")
          .config("simba.index.partitions", "32")
          .getOrCreate()

        import simba.implicits._
        import simba.simbaImplicits._

        val schema = ScalaReflection.schemaFor[TRAJ].dataType.asInstanceOf[StructType]

        val trajectory = simba.read.
          option("header", "false").
          schema(schema).
          format("com.databricks.spark.csv").
          //Input file path for trajectories
          load("trajectories.csv").
          //      sample(false, 0.1, System.nanoTime().toLong).
          //      as[TRAJ]
          toDF("tid", "oid", "t_lon", "t_lat", "time")

        //Create copy of same dataset
        val trajectory2 = trajectory.toDF("tid", "oid", "t_lon1", "t_lat1", "time")

        //Map both datasets
        val ds = trajectory.map(row => TRAJ(row.getLong(0), row.getLong(1), row.getDouble(2), row.getDouble(3), row.getTimestamp(4))).sample(false, 0.1, System.nanoTime().toLong)

        val ds2 = trajectory2.map(row => TRAJ1(row.getLong(0), row.getLong(1), row.getDouble(2), row.getDouble(3), row.getTimestamp(4))).sample(false, 0.1, System.nanoTime().toLong)

        //Index both datasets
        val trajIndex = ds.index(RTreeType, "tpRTree", Array("t_lon", "t_lat"))

        val trajIndex2 = ds2.index(RTreeType, "tpRTree1", Array("t_lon1", "t_lat1"))

        //Extract date from data and store in "time" column
        val trajDate = trajIndex.withColumn("date", to_date($"time"))
        val trajDate2 = trajIndex2.withColumn("date", to_date($"time"))
        //    trajDate.show(10)

        //Filter results for 1 year b/w Feb and June for both datasets
        //Uncomment the two lines in order to get all rows from various years
        val trajFebtoJune = trajDate.filter(
          $"date".between("2007-02-01", "2007-06-30") ||
          $"date".between("2008-02-01","2008-06-30")  ||
          $"date".between("2009-02-01", "2009-06-30") ||
          $"date".between("2010-02-01","2010-06-30")  ||
          $"date".between("2011-02-01","2011-06-30")
        )

        val trajFebtoJune2 = trajDate.filter(
          $"date".between("2007-02-01", "2007-06-30") ||
          $"date".between("2008-02-01","2008-06-30")  ||
          $"date".between("2009-02-01", "2009-06-30") ||
          $"date".between("2010-02-01","2010-06-30")  ||
          $"date".between("2011-02-01","2011-06-30")
        )

        //Perform distance join on the two datasets with radius 100
        //Change radius for 100, 200, 300, 400, 500
        val dJoin = trajFebtoJune.distanceJoin(trajIndex2, Array("t_lon", "t_lat"), Array("t_lon1", "t_lat1"), 100.00).distinct()

        //Perform groupby operation on lat and long to get count and sort in descending order
        val dgroup = dJoin.groupBy("t_lon", "t_lat").count().orderBy(desc("count"))

        //    dJoin.show(20)


        val t = (System.currentTimeMillis() - start_time)
        //Write results to file
        dgroup.coalesce(1).write.format("com.databricks.spark.csv")
          .option("header", "true")
          .save("./output/part4") //Output File path
//        j=j+100
        dgroup.show(20)
        println("Time Taken to execute operations : ")
        println(t  + " milliseconds")

        simba.stop()
      }

  }
