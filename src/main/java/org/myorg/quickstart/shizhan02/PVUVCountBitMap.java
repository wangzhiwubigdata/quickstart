package org.myorg.quickstart.shizhan02;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

public class PVUVCountBitMap {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 检查点配置略。但后面要用到状态，所以状态后端必须预先配置，在flink-conf.yaml或者这里均可
        env.setStateBackend(new MemoryStateBackend(true));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "10");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log_user_action", new SimpleStringSchema(), properties);
        //设置从最早的offset消费
        consumer.setStartFromEarliest();


        DataStream<UserClick> dataStream = env
                .addSource(consumer)
                .name("log_user_action")
                .map(message -> {
                    JSONObject record = JSON.parseObject(message);

                    return new UserClick(
                            record.getString("user_id"),
                            record.getLong("timestamp"),
                            record.getString("action")
                    );
                })
                .returns(TypeInformation.of(UserClick.class));


        SingleOutputStreamOperator<UserClick> userClickSingleOutputStreamOperator = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserClick>(Time.seconds(30)) {
            @Override
            public long extractTimestamp(UserClick element) {
                return element.getTimestamp();
            }
        });

        userClickSingleOutputStreamOperator
                .keyBy(new KeySelector<UserClick, String>() {
                    @Override
                    public String getKey(UserClick value) throws Exception {
                        return value.getUserId();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new MyProcessWindowFunction());





    }//






}//
