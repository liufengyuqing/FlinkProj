package cn.edu.nwsuaf.streaming.stramAPI;

import cn.edu.nwsuaf.streaming.custom.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: StreamingDemoWithMyNoParalleSource
 * @Description: broadcast分区规则
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 1:55 下午
 */

public class StreamingDemoWithMyNoParalleSourceBroadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<Long> source = env.addSource(new MyNoParalleSource());

        //broadcast 广播分区 和广播变量不太一样
        SingleOutputStreamOperator<Long> map = source.broadcast().map(new MapFunction<Long, Long>() {

            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据：" + value);
                return value;
            }
        });

        //每2秒钟处理一次数据
        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0).setParallelism(1); //注意：只支持并行度为1 不设置，默认为1

        //打印结果
        sum.print().setParallelism(1);

        String jonName = StreamingDemoWithMyNoParalleSourceBroadcast.class.getName();
        env.execute(jonName);


    }
}
