package org.myorg.quickstart.shizhan01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


//并行度为1的source
public class MyNoParalleSource implements SourceFunction<String> {

    //private long count = 1L;
    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){
            //图书的排行榜
            List<String> books = new ArrayList<>();
            books.add("Pyhton从入门到放弃");//10
            books.add("Java从入门到放弃");//8
            books.add("Php从入门到放弃");//5
            books.add("C++从入门到放弃");//3
            books.add("Scala从入门到放弃");
            int i = new Random().nextInt(5);
            ctx.collect(books.get(i));

            //每2秒产生一条数据
            Thread.sleep(2000);
        }
    }
    //取消一个cancel的时候会调用的方法
    @Override
    public void cancel() {
        isRunning = false;
    }
}


class StreamingDemoWithMyNoPralalleSource {
    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1); //注意：针对此source，并行度只能设置为1
        DataStream<String> num = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                //System.out.println("接收到数据：" + value);
                return value;
            }
        });

        //每2秒钟处理一次数据 1 2 3 4 5 6 7 8 9 ...
        DataStream<String> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);
        String jobName = StreamingDemoWithMyNoPralalleSource.class.getSimpleName();
        env.execute(jobName);
    }
}
