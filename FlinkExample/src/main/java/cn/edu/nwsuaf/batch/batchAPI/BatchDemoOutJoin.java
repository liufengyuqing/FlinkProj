package cn.edu.nwsuaf.batch.batchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * @ClassName: BatchDemoMapPartition
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 4:56 下午
 * <p>
 * 左外连接
 * 右外连接
 * 全外连接
 */

public class BatchDemoOutJoin {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));


        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(4, "guangzhou"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        /**
         * 左外连接
         * 注意：second这个tuple中的元素可能为null
         */
        text1.leftOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second == null) {
                            return new Tuple3<>(first.f0, first.f1, "null");
                        } else {
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }

                    }
                }).print();

        System.out.println("------------------------------------------");

        /**
         * 右外连接
         *
         * 注意：first这个tuple中的数据可能为null
         *
         */
        text1.rightOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return new Tuple3<>(second.f0, second.f1, "null");
                        } else {
                            return new Tuple3<>(second.f0, second.f1, first.f1);
                        }
                    }
                }).print();

        System.out.println("------------------------------------------");
        /**
         * 全外连接
         *
         * 注意：first和second这两个tuple都有可能为null
         *
         */
        text1.fullOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return new Tuple3<>(second.f0, second.f1, "null");
                        } else if (second == null) {
                            return new Tuple3<>(first.f0, "null", first.f1);
                        } else {
                            return new Tuple3<>(first.f0, second.f1, first.f1);
                        }

                    }
                }).print();

        System.out.println("------------------------------------------");
    }
}