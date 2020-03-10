package cn.edu.nwsuaf.streaming.custom.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @ClassName: MyParalleSource
 * @Description: 自定义实现一个支持并行度的source
 * @Create by: liuzhiwei
 * @Date: 2020/3/10 2:14 下午
 */

public class MyParalleSource implements ParallelSourceFunction<Long> {
    private boolean isRunning = true;
    private long count = 1L;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个cancel的时候会调用的方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
