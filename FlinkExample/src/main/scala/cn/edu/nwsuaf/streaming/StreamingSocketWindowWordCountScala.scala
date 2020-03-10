package cn.edu.nwsuaf.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName: StreamingSocketWindowWordCountScala
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/9 10:30 下午
 *
 */

object StreamingSocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {

    //获取socket端口号
    val port: Int =
      try {
        ParameterTool.fromArgs(args).getInt("port")
      } catch {
        case e: Exception => {
          System.err.println("没有传入端口号，使用默认的端口port 9999 --Java")
        }
          9999
      }


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketData = env.socketTextStream("localhost", port)


    //解析数据(把数据打平)，分组，窗口计算，并且聚合求sum

    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._

    val windowCount = socketData.flatMap(line => line.split("\\s"))
      .map(w => WordCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .reduce((a, b) => WordCount(a.word, a.count + b.count))
    //.sum("count")

    windowCount.print().setParallelism(1)

    env.execute("StreamingSocketWindowWordCountScala")
  }

  case class WordCount(word: String, count: Int)

}


