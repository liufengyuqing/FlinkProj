package cn.edu.nwsuaf.utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName: KafkaProducer
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/7 10:37 下午
 */

public class MyKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "allData";
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //  生成消息数据格式：
        //{"dt":"2018-01-01 10:11:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.2,"level":"B"}]}
        try {
            while (true) {
                Thread.sleep(2000);

                HashMap<String, Object> map = new HashMap<>();
                map.put("dt", getCurrentTime());
                map.put("countryCode", getCountryCode());

                LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String, String>>();
                HashMap<String, String> map1 = new HashMap<>();
                map1.put("type", getRandomType());
                map1.put("score", getRandomScore());
                map1.put("level", getRandomLevel());
                list.add(map1);

                HashMap<String, String> map2 = new HashMap<>();
                map2.put("type", getRandomType());
                map2.put("score", getRandomScore());
                map2.put("level", getRandomLevel());
                list.add(map2);

                map.put("data", list);
                String producerData = JSON.toJSONString(map);

                System.out.println(producerData);
                producer.send(new ProducerRecord<>(topic, producerData));

                //关闭链接
                //producer.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static String getRandomLevel() {
        String[] types = {"A", "A+", "B", "C", "D"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getRandomScore() {
        double[] types = {0.3, 0.2, 0.1, 0.5, 0.8};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return String.valueOf(types[i]);
    }

    private static String getRandomType() {
        String[] types = {"s1", "s2", "s3", "s4", "s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getCountryCode() {
        String[] types = {"US", "TW", "HK", "PK", "KW", "SA", "IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }
}
