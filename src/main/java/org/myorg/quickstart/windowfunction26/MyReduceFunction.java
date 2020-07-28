package org.myorg.quickstart.windowfunction26;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyReduceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> input = env.fromElements(courses);
        DataStream<Tuple2<String, Integer>> total = input.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
        total.printToErr();
        env.execute("ReduceFunction");
    }

    public static final Tuple2[] courses = new Tuple2[]{
            Tuple2.of("张三",100),
            Tuple2.of("李四",80),
            Tuple2.of("张三",80),
            Tuple2.of("李四",95),
            Tuple2.of("张三",90),
            Tuple2.of("李四",100),
    };
}//
