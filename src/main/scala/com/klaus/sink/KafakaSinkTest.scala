package com.klaus.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, FlinkKafkaProducer011}

object KafakaSinkTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //kafka连接properties
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //kafka source
    val kafkaStream = environment.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
    //    val kafkaStream = environment.addSource(new MySensorSource)

    //kafka sink
    kafkaStream.addSink(new FlinkKafkaProducer[String]("sinkTest",new SimpleStringSchema(),properties))
    kafkaStream.print("kafkaStream")
    environment.execute()


  }


}
