package org.myorg.quickstart.shizhan02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import scala.tools.nsc.doc.model.Val;

import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //设置消费组
        properties.setProperty("group.id", "group_test");

        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "10");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        //设置从最早的ffset消费
        consumer.setStartFromEarliest();

        env.addSource(consumer)
                .filter(new UserActionFilter())
                .flatMap(new MyFlatMapFunction())
                .returns(TypeInformation.of(String.class))
                .addSink(new FlinkKafkaProducer(
                        "127.0.0.1:9092",
                        "log_user_action",
                        new SimpleStringSchema()
                ));



        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("start consumer...");
    }
}
