/**
  * Created by volodymyrmiz on 31/08/18.
  */

import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession

object AvroTest extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Avro Test")
    .getOrCreate()

  val df = spark.createDataFrame(
    Seq(
      (2012, 8, "Batman", 9.8),
      (2012, 8, "Hero", 8.7),
      (2012, 7, "Robot", 5.5),
      (2011, 7, "Git", 2.0))
  ).toDF("year", "month", "title", "rating")

  df.show()

  val FILE_PATH = "/mnt/data/git/avro-playground/resources/AvroDF/"

//  df.write.partitionBy("year", "month").avro(FILE_PATH)

  // configuration to use deflate compression
  spark.conf.set("spark.sql.avro.compression.codec", "deflate")
  spark.conf.set("spark.sql.avro.deflate.level", "5")

  val dfRead = spark.read.avro(FILE_PATH)

  dfRead.show()

}