package com.klaus.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //使用flink提供的工具获取参数内容 例如: --host localhost
    val tool = ParameterTool.fromArgs(args)
    val host = tool.get("host")
    val port = tool.getInt("port")
    //创建流处理执行环境
   val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接收socket文本流
    val inputDataStream = environment.socketTextStream(host, port)
    //定义转换操作
    val result = inputDataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    result.print()
    environment.execute("TextStreamWordCount")
  }

}
