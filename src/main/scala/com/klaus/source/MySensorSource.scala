package com.klaus.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

case class Sensor(id:String,curTime:Long,temp:Double)

/**
 * 自定义source,自动生成温度传感器sensor数据
 */
class MySensorSource extends SourceFunction[Sensor]{

  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {
    //定义一个随机数生成器
    val random = new Random()
    //生成10个传感器温度初始值
    var curTemps = (1 to 10).map(
      i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
    )
    while (running){
      //获取当前时间
      val time = System.currentTimeMillis()
      //在当前温度的基础上生成温度波动
      curTemps = curTemps.map(
        data => {
          (data._1, data._2 + random.nextGaussian())
        }
      )
      //对波动之后的十个温度进行遍历发出给,(使用sourceContext)
      curTemps.foreach(
        data=>sourceContext.collect(new Sensor(data._1,time,data._2))
      )
      //传感器更新间隔
      Thread.sleep(1000)
    }


  }

  override def cancel(): Unit = running = false


}
