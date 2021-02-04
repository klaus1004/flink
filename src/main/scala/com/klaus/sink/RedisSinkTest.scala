package com.klaus.sink

import java.util.Properties

import com.klaus.source.{MySensorSource, Sensor}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val sensor = environment.addSource(new MySensorSource)

    //redis sink
    sensor.addSink(new RedisSink[Sensor](new FlinkJedisPoolConfig.Builder().setHost("wangyu.online").setPassword("wangyu123").setPort(6379).build(),new MyRedisMapper))
    sensor.print("kafkaStream")
    environment.execute()


  }

}

 class MyRedisMapper extends RedisMapper[Sensor] {
  override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")

  override def getKeyFromData(t: Sensor): String = t.id

  override def getValueFromData(t: Sensor): String = t.temp.toString
}
