package cn.edu.nwsuaf.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Streaming
 * @Description: window 增量聚合
 * @Create by: liuzhiwei
 * @Date: 2020/3/9 9:13 下午
 * <p>
 * <p>
 * 滑动窗口计算
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 */

public class StreamingSocketFullCount {

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

        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port);

        DataStream<Tuple2<Integer, Integer>> intData = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1, Integer.parseInt(value));
            }
        });

        intData.keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple key, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("执行process。。。");
                        long count = 0;
                        for (Tuple2<Integer, Integer> element : elements) {
                            count++;
                        }

                        out.collect("window:" + context.window() + ",count:" + count);
                    }
                }).print();

        //这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");


    }
}
