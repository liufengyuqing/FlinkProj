package cn.edu.nwsuaf.streaming.tableSQL;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: FlinkTableSinkMySQL
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/12 4:36 下午
 * <p>
 * Flink SQL/Table API 消费Kafka的json格式数据存到MySQL--存入MySQL通过继承RichSinkFunction来实现
 */

public class FlinkTableSinkMySQLRichSinkFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 必须设置checkpoint的间隔时间，不然不会写入jdbc
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        Schema schema = new Schema()
                .field("userId", Types.STRING)
                .field("name", Types.STRING)
                .field("age", Types.STRING)
                .field("sex", Types.STRING)
                .field("createTime", Types.LONG)
                .field("updateTime", Types.LONG);


        Kafka kafka = new Kafka().version("0.11").topic("flink_test").property("bootstrap.servers", "localhost:9092").startFromLatest();

        tableEnv.connect(kafka)
                .withSchema(schema)
                .withFormat(new Json().failOnMissingField(true).deriveSchema())
                .inAppendMode()
                .registerTableSource("Users");

        String sql = "select userId,name,age,sex,createTime,updateTime from Users";
        Table table = tableEnv.sqlQuery(sql);

        DataStream<Info> result = tableEnv.toAppendStream(table, Info.class);
        result.print();

        /*JDBCAppendTableSink sink = new JDBCAppendTableSinkBuilder()
                .setDBUrl("jdbc:mysql://localhost:3306/test?useSSL=false")
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("123456")
                .setBatchSize(1000)
                .setQuery("REPLACE INTO user(userId,name,age,sex,createTime,updateTime) values(?,?,?,?,?,?)")
                .setParameterTypes(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP})
                .build();

        tableEnv.registerTableSink("Result",
                new String[]{"userId", "name", "age", "sex", "createTime", "updateTime"},
                new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP},
                sink);


        tableEnv.insertInto(table, "Result", new StreamQueryConfig());
*/
        result.addSink(new MySQLWriter());
        env.execute("FlinkTableSinkMySQL");


    }
}

// mysql的建表语句
/*
CREATE TABLE `user` (
    `userId` varchar(10) NOT NULL,
    `name` varchar(10) DEFAULT NULL,
    `age` varchar(3) DEFAULT NULL,
    `sex` varchar(10) DEFAULT NULL,
    `createTime` varchar(20) DEFAULT NULL,
    `updateTime` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`userId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
*/
