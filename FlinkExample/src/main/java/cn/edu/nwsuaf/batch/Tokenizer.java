package cn.edu.nwsuaf.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Tokenizer
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/9 10:54 下午
 */

public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] splits = value.toLowerCase().split("\\W+");
        for (String split : splits) {
            if (split.length() > 0) {
                out.collect(new Tuple2<String, Integer>(split, 1));
            }
        }
    }
}
