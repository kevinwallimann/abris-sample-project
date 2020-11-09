
package za.co.absa.abris.sample
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.AbrisConfig

object App {
  private val SchemaRegistryUrl = "http://localhost:8081"
  private def getSchemaString(name: String, namespace: String) = {
    raw"""{
     "type": "record",
     "name": "$name",
     "namespace": "$namespace",
     "fields":[
         {"name": "column", "type": ["int", "null"] }
     ]
    }"""
  }

  def main(args: Array[String]): Unit = {
    val schemaManager = SchemaManagerFactory.create(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> SchemaRegistryUrl))
    val dummyTopicNameSchema = getSchemaString("topLevelRecord", "")
    val subject = SchemaSubject.usingTopicNameStrategy("topic")
    schemaManager.register(subject, dummyTopicNameSchema)
  }
}
