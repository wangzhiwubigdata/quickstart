package org.myorg.quickstart.windowfunction26;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CounterTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("127.0.0.1", 9000, "\n");

        dataStream.map(new RichMapFunction<String, String>() {

            //定义累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //注册累加器
                getRuntimeContext().addAccumulator("counter", this.numLines);
            }

            @Override
            public String map(String s) throws Exception {
                //累加
                this.numLines.add(1);
                return s;
            }
        });

        dataStream.print();
        JobExecutionResult result = env.execute("counter");
        //第四步：结束后输出总量；如果不需要结束后持久化，可以省去
        Object counter = result.getAccumulatorResult("counter");
        System.out.println("累加器计算结果:" + counter);
    }
}
