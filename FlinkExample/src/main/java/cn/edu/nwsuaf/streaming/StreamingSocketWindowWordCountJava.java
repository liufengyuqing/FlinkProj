package cn.edu.nwsuaf.streaming;

import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Streaming
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/9 9:13 下午
 * <p>
 * <p>
 * 滑动窗口计算
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 */

public class StreamingSocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {
        int port = 0;

        try {
            //通过命令行动态传入参数
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("没有传入端口号，使用默认的端口port 9999 -- Java");
            port = 9999;
        }


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "localhost";
        //行换行符 不传 也可以
        String delimiter = "\n";

        //链接socket 获取输入的数据
        DataStreamSource<String> sourceData = env.socketTextStream(hostname, port, delimiter);

        // a a c

        // a 1
        // a 1
        // c 1

        SingleOutputStreamOperator<WordCount> windowCount = sourceData.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override //打平
            public void flatMap(String value, Collector<WordCount> out) {
                String[] splits = value.split("\\s"); // 正则
                for (String word : splits) {
                    out.collect(new WordCount(word, 1));
                }
            } //分组
        })
                .keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1))//指定时间窗口大小为2秒，指定时间间隔为1秒
                .sum("count");//在这里使用sum或者reduce都可以
               /* .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount a, WordCount b) throws Exception {
                        return new WordCount(a.word, a.count + b.count);
                    }
                });*/


        //把数据打印到控制台并且设置并行度
        windowCount.print().setParallelism(1);

        //这一行代码一定要实现，否则程序不执行
        env.execute("StreamingSocketWindowWordCountJava");

    }


   /* public static class WordCount {
        public String word;
        public long count;

        public WordCount() {
        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }*/

}
