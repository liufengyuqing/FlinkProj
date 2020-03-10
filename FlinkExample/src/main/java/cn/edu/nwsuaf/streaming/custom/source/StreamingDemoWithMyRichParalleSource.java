package cn.edu.nwsuaf.streaming.custom.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: StreamingDemoWithMyNoParalleSource
 * @Description: 使用多并行度的source
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 1:55 下午
 */

public class StreamingDemoWithMyRichParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.addSource(new MyRichParalleSource()).setParallelism(2);

        SingleOutputStreamOperator<Long> map = source.map(new MapFunction<Long, Long>() {

            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据：" + value);
                return value;
            }
        });

        //每2秒钟处理一次数据
        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);

        String jonName = StreamingDemoWithMyRichParalleSource.class.getName();
        env.execute(jonName);


    }
}
