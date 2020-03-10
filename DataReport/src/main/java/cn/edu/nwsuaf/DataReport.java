package cn.edu.nwsuaf;

import cn.edu.nwsuaf.function.MyAggFunction;
import cn.edu.nwsuaf.watermark.MyWaterMark;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName: DataReport
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/8 5:24 下午
 */

public class DataReport {
    private static Logger logger = LoggerFactory.getLogger(DataReport.class);

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置使用eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * 配置kafkaSource
         */
        String topic = "auditLog";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "con1");

        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        FlinkKafkaConsumer011 flinkKafkaConsumer = new FlinkKafkaConsumer011(topic, new SimpleStringSchema(), props);

        /**
         *    获取到kafka的数据
         *    审核数据的格式：
         *   {"dt":"审核时间{年月日 时分秒}", "type":"审核类型","username":"审核人姓名","area":"大区"}
         *    说明： json 格式占用的存储空间比较大
         */

        DataStreamSource dataStreamSource = env.addSource(flinkKafkaConsumer);


        /**
         * 对数据进行清洗
         */
        SingleOutputStreamOperator mapData = dataStreamSource.map(new MapFunction<String, Tuple3<Long, String, String>>() {

            @Override
            public Tuple3<Long, String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String dt = jsonObject.getString("dt");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                long time = 0;
                try {
                    Date parse = simpleDateFormat.parse(dt);
                    time = parse.getTime();
                } catch (ParseException e) {
                    //也可以把这个日志存储到其他介质中
                    logger.error("时间解析异常 dt：" + dt, e.getCause());
                }

                String type = jsonObject.getString("type");
                String area = jsonObject.getString("area");

                return new Tuple3<Long, String, String>(time, type, area);
            }
        });

        /**
         * 过滤掉异常数据
         */
        SingleOutputStreamOperator filterData = mapData.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> value) throws Exception {
                boolean flag = true;
                if (value.f0 == 0) {
                    flag = false;
                }
                return flag;
            }
        });

        //保存迟到太久的数据
        OutputTag<Tuple3<Long, String, String>> tuple3OutputTag = new OutputTag<Tuple3<Long, String, String>>("late-data");

        SingleOutputStreamOperator resultData = filterData.assignTimestampsAndWatermarks(new MyWaterMark()).keyBy(1, 2)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(30))//允许迟到30s
                .sideOutputLateData(tuple3OutputTag) //记录迟到的数据
                .apply(new MyAggFunction());

        //获取迟到太久的数据
        DataStream<Tuple3<Long, String, String>> sideOutput = resultData.getSideOutput(tuple3OutputTag);

        //把迟到的数据存储到kafka中
        String outTopic = "lateLog";

        Properties outprop = new Properties();
        outprop.setProperty("bootstrap.servers", "localhost:9092");
        //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
        //设置事务超时时间
        outprop.setProperty("transaction.timeout.ms", 60000 * 15 + "");
        FlinkKafkaProducer011<String> stringFlinkKafkaProducer = new FlinkKafkaProducer011<String>(outTopic, new org.apache.flink.api.common.serialization.SimpleStringSchema(), outprop);


        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0 + "\t" + value.f1 + "\t" + value.f2;
            }
        }).addSink(stringFlinkKafkaProducer);


        /**
         * 把计算的结果存储到es中
         */
/*

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"));


        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
*/



        env.execute("DataReport");
    }
}
