package cn.edu.nwsuaf.streaming.custom.partition;

import cn.edu.nwsuaf.streaming.custom.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StreamingDemoWithMyPartition
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 4:15 下午
 */

public class StreamingDemoWithMyPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<Long> source = env.addSource(new MyNoParalleSource());

        //对数据进行转换，把long类型转成tuple1类型
        SingleOutputStreamOperator<Tuple1<Long>> tupleData = source.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });

        //分区之后的数据
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new MyPartition(), 0);

        SingleOutputStreamOperator<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id：" + Thread.currentThread().getId() + ",value: " + value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);

        env.execute("StreamingDemoWithMyPartition");
    }
}
