package cn.edu.nwsuaf;

import cn.edu.nwsuaf.source.MyRedisSource;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName: DataClean
 * @Description: 数据清洗需求 组装代码
 * @Create by: liuzhiwei
 * @Date: 2020/3/7 8:02 下午
 * 创建kafka topic命令：
 * ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic allData
 * ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic allDataClean
 * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic allData
 * ./kafka-topics.sh --alter --zookeeper localhost:2181 --topic allData --partitions 1
 * ./kafka-topics.sh --delete  --zookeeper localhost:2181 --topic allData
 * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic allDataClean
 * ./flink run -m yarn-cluster -yn 2 -yjm 1024m -ytm 1024m -c cn.edu.nwsuaf.DataClean /Users/liuzhiwei/Desktop/FlinkPro/DataClean/target/DataClean-1.0-SNAPSHOT.jar
 */

public class DataClean {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //修改并行度
        //并行度根据 kafka topic partition数设定
        env.setParallelism(1);

        //checkpoint配置
        env.enableCheckpointing(60000);  // 设置 1分钟=60秒
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //enableCheckpointing最小间隔时间（一半）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);// 超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        //env.setStateBackend(new RocksDBStateBackend("hdfs://localhost:9000/flink/checkpoints", true));


        //指定kafka数据
        String topic = "allData";
        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "localhost:9092");

        prop.setProperty("group.id", "con1");
        FlinkKafkaConsumer011 flinkKafkaConsumer = new FlinkKafkaConsumer011(topic, new SimpleStringSchema(), prop);

        //  获取 Kafka 中的数据，Kakfa 数据格式如下：
        //  {"dt":"2019-01-01 11:11:11", "countryCode":"US","data":[{"type":"s1","score":0.3},{"type":"s1","score":0.3}]}
        DataStreamSource data = env.addSource(flinkKafkaConsumer);


        //最新的国家码和大区的映射关系
        DataStream<HashMap<String, String>> mapData = env.addSource(new MyRedisSource()).broadcast();//  可以把数据发送到后面算子的所有并行实际例中进行计算，否则处理数据丢失数据

        //  通过 connect 方法将两个数据流连接在一起,然后再flatMap
        //
        SingleOutputStreamOperator resData = data.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {

            //存储国家码和大区的映射关系
            private HashMap<String, String> allMap = new HashMap<String, String>();

            //  flatMap1 处理 Kafka 中的数据
            @Override
            public void flatMap1(String value, Collector<String> collector) throws Exception {
                JSONObject jsonObject = (JSONObject) JSONObject.parse(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");

                //获取大区
                String area = allMap.get(countryCode);

                JSONArray jsonArray = jsonObject.getJSONArray("data");

                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                    jsonObject1.put("area", area);
                    System.out.println("area----" + area);

                    jsonObject1.put("dt", dt);
                    collector.collect(jsonObject1.toJSONString());
                }

            }

            //  flatMap2 处理 Redis 返回的 map 类型的数据
            @Override
            public void flatMap2(HashMap<String, String> value, Collector<String> collector) throws Exception {
                this.allMap = value;

            }
        });

        String outTopic = "allDataClean";


        Properties outprop = new Properties();
        outprop.setProperty("bootstrap.servers", "localhost:9092");
        //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
        //设置事务超时时间
        outprop.setProperty("transaction.timeout.ms", 60000 * 15 + "");
        //第二种解决方案，设置kafka的最大事务超时时间


        FlinkKafkaProducer011<String> stringFlinkKafkaProducer = new FlinkKafkaProducer011<String>(outTopic, new SimpleStringSchema(), outprop);

        //FlinkKafkaProducer011<String> stringFlinkKafkaProducer = new FlinkKafkaProducer011<String>(outTopic, new SimpleStringSchema(), outprop);
        resData.addSink(stringFlinkKafkaProducer);

        env.execute("DataClean");


    }
}
