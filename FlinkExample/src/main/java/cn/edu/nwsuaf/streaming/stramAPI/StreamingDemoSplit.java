package cn.edu.nwsuaf.streaming.stramAPI;

import cn.edu.nwsuaf.streaming.custom.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

/**
 * @ClassName: StreamingDemoWithMyNoParalleSource
 * @Description: split根据规则把一个数据流切分为多个流
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 1:55 下午
 * <p>
 * split
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 */

public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.addSource(new MyNoParalleSource());

        SplitStream<Long> split = source.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    outPut.add("even");//偶数
                } else {
                    outPut.add("odd");//奇数
                }
                return outPut;
            }
        });

        //选择一个或者多个切分后的流
        DataStream<Long> even = split.select("even");
        DataStream<Long> odd = split.select("odd");

        DataStream<Long> moreStream = split.select("even", "odd");


        //打印结果
        even.print().setParallelism(1);
        //moreStream.print().setParallelism(1);

        String jonName = StreamingDemoSplit.class.getName();
        env.execute(jonName);


    }
}
