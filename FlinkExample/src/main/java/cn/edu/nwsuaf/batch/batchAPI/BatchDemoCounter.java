package cn.edu.nwsuaf.batch.batchAPI;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ClassName: BatchDemoMapPartition
 * @Description: 计数器
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 4:56 下午
 */

/**
 * 全局累加器
 * counter 计数器
 * 需求：
 * 计算map函数中处理了多少数据
 * <p>
 * 注意：只有在任务执行结束后，才能获取到累加器的值
 */

public class BatchDemoCounter {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> map = source.map(new RichMapFunction<String, String>() {
            //1:创建累加器
            private IntCounter numLines = new IntCounter();
            //int sum = 0;


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2:注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String value) throws Exception {
                //如果并行度为1，使用普通的累加求和即可，但是设置多个并行度，则普通的累加求和结果就不准了
                //sum++;
                //System.out.println("sum: " + sum);
                numLines.add(1);
                return value;
            }
        }).setParallelism(4);

        //map.print();
        map.writeAsText("data/counterResult.txt");


        JobExecutionResult jobResult = env.execute("BatchDemoCounter");

        int num = jobResult.getAccumulatorResult("num-lines");


        System.out.println("num:" + num);


    }
}
