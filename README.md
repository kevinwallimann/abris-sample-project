# abris-sample-project
Toy project to reproduce https://github.com/AbsaOSS/ABRiS/issues/165
```
Exception in thread "main" java.lang.NoSuchFieldError: FACTORY
	at org.apache.avro.Schemas.toString(Schemas.java:36)
	at org.apache.avro.Schemas.toString(Schemas.java:30)
	at io.confluent.kafka.schemaregistry.avro.AvroSchema.canonicalString(AvroSchema.java:140)
	at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.registerAndGetId(CachedSchemaRegistryClient.java:206)
	at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.register(CachedSchemaRegistryClient.java:268)
	at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.register(CachedSchemaRegistryClient.java:244)
	at io.confluent.kafka.schemaregistry.client.SchemaRegistryClient.register(SchemaRegistryClient.java:42)
	at za.co.absa.abris.avro.read.confluent.SchemaManager.register(SchemaManager.scala:77)
	at za.co.absa.abris.avro.read.confluent.SchemaManager.register(SchemaManager.scala:67)
	at za.co.absa.abris.sample.App$.main(App.scala:25)
	at za.co.absa.abris.sample.App.main(App.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:849)
	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:167)
	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:195)
	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:86)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:924)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:933)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
```

- I'm running a local confluent cluster with schema registry at `http://localhost:8081`
- Build code with `mvn clean package`
- Execute with `spark-submit target/abris-sample-project-0.1.0-SNAPSHOT.jar`
- Tried with Spark 2.4.3/Scala 2.11 and Spark 3.0.1/Scala 2.12
