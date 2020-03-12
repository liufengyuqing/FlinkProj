package cn.edu.nwsuaf.streaming.tableSQL;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * @ClassName: KafkaProducerTest
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/12 5:09 下午
 */

public class KafkaProducerTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("acks", "all");
        //props.put("retries", 0);
        //props.put("batch.size", 16384);
        //props.put("linger.ms", 1);
        //props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
       /* for (int i = 0; i < 100; i++) {
            JSONObject event = new JSONObject();
            event.put("id", (int) (Math.random() * 100 + 1)) .put("name", "mingzi" + 1) .put("sex", i).put("score", i * 1.0);
            //producer.send(new ProducerRecord<String, String>("nima", Integer.toString(i), event.toString()));
            System.out.println(i + " " + event.toString());
            Thread.sleep(5000);
        }*/

        String topic = "flink_test";
        int count = 1;
        while (true) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("userId", (int) (Math.random() * 100 + 1));
            map.put("name", "zhiwei" + count);
            map.put("age", getRandomAge());
            map.put("sex", getRandomSex());
            map.put("createTime", System.currentTimeMillis());
            map.put("updateTime", System.currentTimeMillis());
            count++;
            String producerData = JSON.toJSONString(map);
            System.out.println(producerData);
            producer.send(new ProducerRecord<>(topic, producerData));

            Thread.sleep(5000);
        }
    }

    private static int getRandomAge() {
        int[] ages = {20, 22, 24, 25, 26, 27};
        Random random = new Random();
        int i = random.nextInt(ages.length);
        return ages[i];
    }

    private static String getRandomSex() {
        String[] types = {"F", "M"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
