package org.myorg.quickstart.CEP11;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class StreamingCep {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource source = env.fromElements(
                //浏览记录
                Tuple3.of("Marry", "外套", 1L),

                Tuple3.of("Marry", "帽子",1L),
                Tuple3.of("Marry", "帽子",2L),
                Tuple3.of("Marry", "帽子",3L),

                Tuple3.of("Ming", "衣服",1L),

                Tuple3.of("Marry", "鞋子",1L),
                Tuple3.of("Marry", "鞋子",2L),

                Tuple3.of("LiLei", "帽子",1L),
                Tuple3.of("LiLei", "帽子",2L),
                Tuple3.of("LiLei", "帽子",3L)
        );
        //定义Pattern,寻找连续搜索帽子的用户
        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern
                .<Tuple3<String, String, Long>>begin("start")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                }) //.timesOrMore(3);
                .next("middle")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                });

//        Pattern.<LogInEvent>begin("start").where(new IterativeCondition<LogInEvent>() {
//            @Override
//            public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
//                return value.getIsSuccess().equals("fail");
//            }
//        }).next("next").where(new IterativeCondition<LogInEvent>() {
//            @Override
//            public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
//                return value.getIsSuccess().equals("fail");
//            }
//        }).within(Time.seconds(5));



        KeyedStream keyedStream = source.keyBy(0);
        PatternStream patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator matchStream = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
            @Override
            public String select(Map<String, List<Tuple3<String, String, Long>>> pattern) throws Exception {
                List<Tuple3<String, String, Long>> middle = pattern.get("middle");
                return middle.get(0).f0 + ":" + middle.get(0).f2 + ":" + "连续搜索两次帽子!";
            }
        });

        //////////////

//        Pattern.<PayEvent>
//                begin("begin")
//                .where(new IterativeCondition<PayEvent>() {
//                    @Override
//                    public boolean filter(PayEvent payEvent, Context context) throws Exception {
//                        return payEvent.getAction().equals("create");
//                    }
//                })
//                .next("next")
//                .where(new IterativeCondition<PayEvent>() {
//                    @Override
//                    public boolean filter(PayEvent payEvent, Context context) throws Exception {
//                        return payEvent.getAction().equals("pay");
//                    }
//                })
//                .within(Time.seconds(600));
//        OutputTag<PayEvent> orderTiemoutOutput = new OutputTag<PayEvent>("orderTimeout") {};
//
//        SingleOutputStreamOperator selectResult = patternStream.select(orderTiemoutOutput,
//                (PatternTimeoutFunction<PayEvent, ResultPayEvent>) (map, l) -> new ResultPayEvent(map.get("begin").get(0).getUserId(), "timeout"),
//                (PatternSelectFunction<PayEvent, ResultPayEvent>) map -> new ResultPayEvent(map.get("next").get(0).getUserId(), "success")
//        );
//        DataStream timeOutSideOutputStream = selectResult.getSideOutput(orderTiemoutOutput);

        ///////////

        Pattern.<TransactionEvent>begin("start").where(
                new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent transactionEvent) {
                        return transactionEvent.getAmount() > 0;
                    }
                }
        ).timesOrMore(5)
         .within(Time.hours(24));






        ////////////



        matchStream.printToErr();
        env.execute("execute cep");

    }


//    class ResultPayEvent{
//        private Long userId;
//        private String type;
//
//        public ResultPayEvent(Long userId, String type) {
//            this.userId = userId;
//            this.type = type;
//        }
//    }

}//
