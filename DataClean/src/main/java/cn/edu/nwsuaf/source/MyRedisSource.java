package cn.edu.nwsuaf.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: MyRedisSource
 * @Description: redis 中进行数据初始化
 * @Create by: liuzhiwei
 * @Date: 2020/3/7 8:16 下午
 * <p>
 * 在 Redis 中保存的国家和大区的关系
 * <p>
 * Redis中进行数据的初始化，数据格式：
 * Hash        大区       国家
 * hset areas  AREA_US   US
 * hset areas  AREA_CT   TW,HK
 * hset areas  AREA_AR   PK,SA,KW
 * hset areas  AREA_IN   IN
 * <p>
 * 需要把大区和国家的对应关系组装成 java 的 hashmap
 */


public class MyRedisSource implements SourceFunction<HashMap<String, String>> {


    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);

    private final long SLEEP_MILLION = 5000;
    private boolean isRunning = true;
    private Jedis jedis = null;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {


        jedis = new Jedis("localhost", 6379);

        //  存储所有国家和大区的对应关系
        HashMap<String, String> keyValuesMap = new HashMap<>();
        while (isRunning) {
            try {
                //每次获取数据前clear 老数据清除
                keyValuesMap.clear();
                Map<String, String> areas = jedis.hgetAll("areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        keyValuesMap.put(split, key);
                    }
                }
                //写出去
                if (keyValuesMap.size() > 0) {
                    // map >0 数据发送出去
                    sourceContext.collect(keyValuesMap);
                } else {
                    logger.warn("从redis中获取的数据为空");
                }
                // 歇5秒
                Thread.sleep(SLEEP_MILLION);
            } catch (JedisConnectionException e) {
                logger.error("redis 链接异常，重新获取链接", e.getCause());
                jedis = new Jedis("localhost", 6379);

            } catch (Exception e) {
                logger.error("Source", e.getCause());
                e.printStackTrace();
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;

    }
}
