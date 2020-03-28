package objects

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, sql}

import com.datastax.spark.connector.cql.CassandraConnector

object SparkSessionObj {

  def getDataCassandra(sppName:String): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .appName(sppName)
      .getOrCreate()

    val sparkContext = spark.sparkContext

    sparkContext.setLogLevel("WARN")
    val connector = CassandraConnector(sparkContext.getConf)

    connector.withSessionDo(session => {
      session.execute("USE test_space")
    })

    val df = spark.read
      .format("org.apache.sql.cassandra")
      .options(Map("table" -> "employees", "keyspace" -> "test_space"))
      .load()

    println("Los empleados son: ")
    df.collect().foreach(println)
  }

  def getSparkContext(appName:String, master:String): SparkContext = {
    val conf:SparkConf = new SparkConf().setAppName(appName).setMaster(master)
    new SparkContext(conf)
  }

}
