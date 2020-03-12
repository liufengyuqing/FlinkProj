package cn.edu.nwsuaf.utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName: KafkaProducerDataReport
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/12 11:06 上午
 */

public class KafkaProducerDataReport {

    public static void main(String[] args) {
        Properties props = new Properties();
        //指定kafka broker地址
        props.setProperty("bootstrap.servers", "localhost:9092");
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定key value的序列化方式
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        //指定topic名称
        String topic = "auditLog";
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //  生成消息数据格式：
        //{"dt":"2018-01-01 10:11:22","type":"shelf","username":"shenhe1","area":"AREA_US"}
        try {
            while (true) {
                Thread.sleep(2000);

                HashMap<String, Object> map = new HashMap<>();
                map.put("dt", getCurrentTime());
                map.put("type",getRandomType());
                map.put("username",getUserName());
                map.put("area",getCountryArea());


                /*LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String, String>>();
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
*/
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



    private static String getRandomType() {
        String[] types = {"s1", "s2", "s3", "s4", "s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getCountryArea() {
        String[] types = {"AREA_US","AREA_CT","AREA_AR","AREA_IN","AREA_ID"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getUserName() {
        String[] types = {"shenhe1","shenhe2","shenhe3","shenhe4","shenhe5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    private static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }
}
