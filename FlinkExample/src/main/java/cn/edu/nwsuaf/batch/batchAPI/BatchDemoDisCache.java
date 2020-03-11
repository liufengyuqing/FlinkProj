package cn.edu.nwsuaf.batch.batchAPI;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ClassName: BatchDemoMapPartition
 * @Description: 广播变量
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 4:56 下午
 */

/**
 * Distributed Cache
 */

public class BatchDemoDisCache {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：注册一个文件,可以使用hdfs或者s3上的文件
        env.registerCachedFile("data/a.txt", "a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2. 使用文件
                File file = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.out.println("line:" + line);
                }

            }

            @Override
            public String map(String value) throws Exception {
                //3.在这里就可以使用dataList
                return value;
            }
        });

        result.print();


    }
}
