package cn.edu.nwsuaf.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @ClassName: BatchWordCountJava
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/9 10:52 下午
 */

public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "data/test.txt";

        DataSource<String> stringDataSource = env.readTextFile(inputPath);

        AggregateOperator<Tuple2<String, Integer>> wordCount = stringDataSource.flatMap(new Tokenizer()).groupBy(0).sum(1);

        String outPath = "data/res.txt";
        wordCount.writeAsCsv(outPath);
        wordCount.print();

    }
}
