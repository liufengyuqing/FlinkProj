package cn.edu.nwsuaf.streaming;

/**
 * @ClassName: StreamingDemoKafakaSource
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/11 11:02 下午
 */

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Optional;
import java.util.Properties;


public class StreamingDemoKafakaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend

        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        String brokerList = "localhost:9092";
        String topic = "t1";


        //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerList);

        //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
        //设置事务超时时间
        //props.setProperty("transaction.timeout.ms",60000*15+"");

        //第二种解决方案，设置kafka的最大事务超时时间
        // 15分钟和1个小时


        //使用仅一次语义的kafkaProducer
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(topic,
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), props, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        source.addSink(myProducer);

        env.execute("StreamingDemoKafakaSink");

    }
}
