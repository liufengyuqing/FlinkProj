package cn.edu.nwsuaf.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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

public class StreamingSocketWindowWordCountJavaCheckPoint {

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

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend

        //env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
        //env.setStateBackend(new RocksDBStateBackend("hdfs://locahost:9000/flink/checkpoints",true));


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
                .keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))//指定时间窗口大小为2秒，指定时间间隔为1秒
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
