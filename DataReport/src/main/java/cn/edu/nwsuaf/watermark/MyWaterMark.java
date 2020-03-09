package cn.edu.nwsuaf.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import redis.clients.jedis.Tuple;
import scala.Tuple3;

import javax.annotation.Nullable;

/**
 * @ClassName: MyWaterMark
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/8 6:31 下午
 */

public class MyWaterMark implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {

    Long currentMaxTimestamp = 0L;
    final Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10s


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {

        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        Long timestamp = element._1();
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }
}
