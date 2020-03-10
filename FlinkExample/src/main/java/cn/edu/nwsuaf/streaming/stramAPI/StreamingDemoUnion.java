package cn.edu.nwsuaf.streaming.stramAPI;

import cn.edu.nwsuaf.streaming.custom.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: StreamingDemoWithMyNoParalleSource
 * @Description: union 合并多个流，新的流会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致的
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 1:55 下午
 */

public class StreamingDemoUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source1 = env.addSource(new MyNoParalleSource());
        DataStreamSource<Long> source2 = env.addSource(new MyNoParalleSource());

        DataStream<Long> unionStream = source1.union(source2);

        SingleOutputStreamOperator<Long> map = unionStream.map(new MapFunction<Long, Long>() {

            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据：" + value);
                return value;
            }
        });

        //执行filter 过滤 满足条件的数据会被留下
        SingleOutputStreamOperator<Long> filterData = map.filter(new FilterFunction<Long>() {
            //把所有的奇数过滤掉
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });


        SingleOutputStreamOperator<Long> resultData = filterData.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后的数据：" + value);
                return value;
            }
        });


        //每2秒钟处理一次数据
        SingleOutputStreamOperator<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0).setParallelism(1); //注意：只支持并行度为1 不设置，默认为1

        //打印结果
        sum.print().setParallelism(1);

        String jonName = StreamingDemoUnion.class.getName();
        env.execute(jonName);


    }
}
