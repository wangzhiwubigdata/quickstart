package org.myorg.quickstart.topn28;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.RedisSink27.RedisSink02;

import java.util.*;

public class TopN {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从最早开始消费
        consumer.setStartFromEarliest();

        DataStream<String> stream = env
                .addSource(consumer);

        DataStream<OrderDetail> orderStream = stream.map(message -> JSON.parseObject(message, OrderDetail.class));

        DataStream<OrderDetail> dataStream = orderStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<OrderDetail>() {

            private Long currentTimeStamp = 0L;
            //设置允许乱序时间
            private Long maxOutOfOrderness = 3000L;
            @Override
            public Watermark getCurrentWatermark() {

                return new Watermark(currentTimeStamp - maxOutOfOrderness);
            }
            @Override
            public long extractTimestamp(OrderDetail element, long previousElementTimestamp) {
                return element.getTimeStamp();
            }
        });

        DataStream<OrderDetail> reduce = dataStream
                .keyBy((KeySelector<OrderDetail, Object>) value -> value.getUserId())
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
                .reduce(new ReduceFunction<OrderDetail>() {
                    @Override
                    public OrderDetail reduce(OrderDetail value1, OrderDetail value2) throws Exception {
                        return new OrderDetail(
                                value1.getUserId(), value1.getItemId(), value1.getCiteName(), value1.getPrice() + value2.getPrice(), value1.getTimeStamp()
                        );
                    }
                });


        //每20秒计算一次
        DataStream<Tuple2<Double, OrderDetail>> process = reduce.windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(new ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow>() {
                             @Override
                             public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
                                 TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
                                     @Override
                                     public int compare(Double x, Double y) {
                                         return (x < y) ? -1 : 1;
                                     }
                                 });

                                 Iterator<OrderDetail> iterator = elements.iterator();
                                 if (iterator.hasNext()) {
                                     treeMap.put(iterator.next().getPrice(), iterator.next());
                                     if (treeMap.size() > 10) {
                                         treeMap.pollLastEntry();
                                     }
                                 }

                                 for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
                                     out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                                 }
                             }
                         }
                );
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        process.addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<Double, OrderDetail>>() {

            private final String TOPN_PREFIX = "TOPN:";
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,TOPN_PREFIX);
            }

            @Override
            public String getKeyFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f0);
            }

            @Override
            public String getValueFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f1.toString());
            }
        }));

        env.execute("execute topn");


    }
}//
