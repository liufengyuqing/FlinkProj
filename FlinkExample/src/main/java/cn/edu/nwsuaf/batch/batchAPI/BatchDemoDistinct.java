package cn.edu.nwsuaf.batch.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @ClassName: BatchDemoMapPartition
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 4:56 下午
 */

public class BatchDemoDistinct {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);

        FlatMapOperator<String, String> flatMap = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.toLowerCase().split("\\s");
                for (String word : split) {
                    System.out.println("单词：" + word);
                    out.collect(word);
                }
            }
        });

        flatMap.distinct().print();// 对数据进行整体去重

    }


}
