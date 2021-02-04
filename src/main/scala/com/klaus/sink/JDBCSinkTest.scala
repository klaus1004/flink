package com.klaus.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.klaus.source.{MySensorSource, Sensor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

object JDBCSinkTest {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    val sensor = environment.addSource(new MySensorSource)

    //自定义JDBC sink 的测试
    sensor.addSink(new MyJdbcSink)
    sensor.print("kafkaStream")
    environment.execute()


  }
  //自定义JDBC Sink
class MyJdbcSink() extends RichSinkFunction[Sensor]{
      //初始化连接所需对象
  var connection: Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "admin")
    insertStmt = connection.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = connection.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    connection.close()
  }

  override def invoke(value: Sensor, context: SinkFunction.Context[_]): Unit ={
    updateStmt.setDouble(1,value.temp)
    updateStmt.setString(2,value.id)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temp)
      insertStmt.execute()
    }
  }
}

}
