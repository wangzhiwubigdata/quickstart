package org.myorg.quickstart.Distinct20;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class RedisSinkDistinct implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {

    }
}
