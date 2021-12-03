
package za.co.absa.abris.sample

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.AbrisConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils

object SparkApp {
  private val schemaRegistryUrl = "http://localhost:8081"
  private val topic = "hero.topic"
  private val kafkaBrokers = "http://localhost:9092"
  private val checkpointLocation = "/tmp/checkpoint-location"

  private def createStreamingDataframe(spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", LongType, nullable = true),
      StructField("nicknames", ArrayType(StringType), nullable = true),
      StructField("magic_powers", StructType(Seq(
        StructField("has_invisibility", BooleanType),
        StructField("is_alchemist", BooleanType),
      )))
    ))
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schema))
    val rows = Seq(
      Row(1, "A-Bomb", 441, List("Smith", "bla"), Row(true, false)),
    )
    memoryStream.addData(rows)
    memoryStream.toDF()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("abris-sample-app").getOrCreate()
    val dataFrame = createStreamingDataframe(spark)
    val schema = AvroSchemaUtils.toAvroSchema(dataFrame)
    val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> "http://localhost:8081")
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    val subject = SchemaSubject.usingTopicNameStrategy("topic")
    val schemaId = schemaManager.register(subject, schema)

    val toAvroConfig = AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(schemaRegistryUrl)

    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
    val avroDataFrame = dataFrame.select(to_avro(allColumns, toAvroConfig) as 'value)

    avroDataFrame
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.Once)
      .option("topic", topic)
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .format("kafka")
      .start()
  }
}
