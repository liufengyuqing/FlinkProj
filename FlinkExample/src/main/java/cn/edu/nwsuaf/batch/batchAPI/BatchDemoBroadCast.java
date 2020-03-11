package cn.edu.nwsuaf.batch.batchAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

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
 * broadcast广播变量
 * 需求：
 * flink会从数据源中获取到用户的姓名
 * <p>
 * 最终需要把用户的姓名和年龄信息打印出来
 * <p>
 * 分析：
 * 所以就需要在中间的map处理的时候获取用户的年龄信息
 * <p>
 * 建议吧用户的关系数据集使用广播变量进行处理
 * <p>
 * 注意：如果多个算子需要使用同一份数据集，那么需要在对应的多个算子后面分别注册广播变量
 */

public class BatchDemoBroadCast {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs", 18));
        broadData.add(new Tuple2<>("ls", 20));
        broadData.add(new Tuple2<>("ww", 17));

        DataSource<Tuple2<String, Integer>> dataSource = env.fromCollection(broadData);

        //1.1:处理需要广播的数据,把数据集转换成map类型，map中的key就是用户姓名，value就是用户年龄
        MapOperator<Tuple2<String, Integer>, HashMap<String, Integer>> broadCastSet = dataSource.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> hashMap = new HashMap<>();
                hashMap.put(value.f0, value.f1);
                return hashMap;
            }
        });

        //源数据
        DataSource<String> data = env.fromElements("zs", "ls", "ww");

        //注意：在这里需要使用到RichMapFunction获取广播变量
        MapOperator<String, String> stringStringMapOperator = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastList = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();


            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             * 所以，就可以在open方法中获取广播变量数据
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastList = getRuntimeContext().getBroadcastVariable("broadCastSet");

                for (HashMap map : broadCastList) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }

        }).withBroadcastSet(broadCastSet, "broadCastSet");//2：执行广播数据的操作

        stringStringMapOperator.print();


    }
}
