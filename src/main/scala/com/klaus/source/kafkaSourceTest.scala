package com.klaus.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 测试flink消费kafka数据流
 */

object kafkaSourceTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafkaStream = environment.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    val kafkaStream = environment.addSource(new MySensorSource)

    kafkaStream.print("kafkaStream")
    environment.execute()


  }
}
