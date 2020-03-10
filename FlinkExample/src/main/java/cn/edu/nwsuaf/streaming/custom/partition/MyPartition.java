package cn.edu.nwsuaf.streaming.custom.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @ClassName: MyPartition
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 4:11 下午
 */

public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数" + numPartitions);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
