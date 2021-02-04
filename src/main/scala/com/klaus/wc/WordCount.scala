package com.klaus.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment,_}


object WordCount {
  def main(args: Array[String]): Unit = {
     //创建一个批处理的执行环境
     val environment = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inputDateSet = environment.readTextFile("C:\\Users\\Administrator\\Desktop\\idea_workspace\\flink1\\src\\main\\resources\\wordcountText.txt")
    //基于dateSet做转换,首先按空格分词打散,然后按照word作为key做group by

    val resultValue: DataSet[(String,Int)] = inputDateSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    resultValue.print()



  }

}
