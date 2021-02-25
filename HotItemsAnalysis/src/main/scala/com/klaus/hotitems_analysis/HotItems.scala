package com.klaus.hotitems_analysis

import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
//定义窗口聚合结果的样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {
  def main(args: Array[String]): Unit = {
      //创建一个流处理执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = environment.readTextFile("C:\\Users\\Administrator\\Desktop\\idea_workspace\\flink1\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dateStream = inputStream.map(
      date => {
        val dataArray = date.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //对数据进行转换,过滤出pv行为,开窗统计个数

    val aggStream = dateStream
      .filter(_.behavior == "pv")//过滤pv行为
      .keyBy("itemId")//按照itemId分组
      .timeWindow(Time.hours(1), Time.minutes(5))//定义滑动窗口
      .aggregate(new CountAgg, new ItemCountWindowResult)

    //对窗口聚合结果按照窗口进行分组,并做排序取TopN输出
    val resultStream = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(5))

    resultStream.print()

    environment.execute("hot item job")
  }
}
//自定义预聚合函数,来一条数据就+1
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
//自定义窗口函数,结合window信息包装成样例类
class ItemCountWindowResult() extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId,windowEnd,count))
    util.BitSet
  }
}
//自定义KeyedProcessFunction
class TopNHotItems(n:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String] {
  //定义一个listState,用来保存当前窗口所有的count结果
  lazy val itemCountListState:ListState[ItemViewCount]=getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemcount-list",classOf[ItemViewCount]))


  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

    //每来一条数据就保存到状态中
    itemCountListState.add(i)
    //注册定时器,在windowEnd + 100 触发(重复注册不会受影响,一个时间点只可以定义一个定时器)
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)
  }
  //定时器触发,从状态中取数据,然后排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //先把状态中的数据提取到一个ListBuffer中
    val allItemCountList: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(itemCount <- itemCountListState.get()){
      allItemCountList += itemCount
    }
    //按照count值大小排序,取TopN
    val sortedItemCountList = allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

    //清除状态
    itemCountListState.clear()

    //将排名信息格式化成String,方便监控显示
    val result = new StringBuilder
    result.append("时间: ").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历sorted列表,输出topN信息
    for(i <- sortedItemCountList.indices){
      //获取当前商品的count信息
      val currentItemCount = sortedItemCountList(i)
      result.append("top").append(i+1).append(":")
        .append(" 商品id=").append(currentItemCount.itemId)
        .append(" 访问量=").append(currentItemCount.count)
        .append("\n")
    }
    result.append("==============================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString())
  }
}
