package cn.edu.nwsuaf.streaming.stramAPI;

import cn.edu.nwsuaf.streaming.custom.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: StreamingDemoWithMyNoParalleSource
 * @Description: union 合并多个流，新的流会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致的
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 1:55 下午
 * <p>
 * connect
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 */

public class StreamingDemoConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source1 = env.addSource(new MyNoParalleSource());
        DataStreamSource<Long> source2 = env.addSource(new MyNoParalleSource());

        SingleOutputStreamOperator<String> source2_str = source2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        ConnectedStreams<Long, String> connectStream = source1.connect(source2_str);

        SingleOutputStreamOperator<Object> mapResult = connectStream.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Long map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        //mapResult.shuffle().rebalance().rescale()
        //打印结果
        mapResult.print().setParallelism(1);

        String jonName = StreamingDemoConnect.class.getName();
        env.execute(jonName);


    }
}
