package cn.edu.nwsuaf.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @ClassName: StreamingFromCollection
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 1:36 下午
 */

public class StreamingFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(15);
        data.add(20);

        DataStreamSource<Integer> source = env.fromCollection(data);

        SingleOutputStreamOperator<Integer> map = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        map.print().setParallelism(1);

        env.execute("StreamingFromCollection");


    }
}
