package org.myorg.quickstart.shizhan02;


import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;

public class MyProcessAllWindowFunction extends ProcessAllWindowFunction<UserClick,Tuple2<String,Integer>,TimeWindow> {
    @Override
    public void process(Context context, Iterable<UserClick> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

        HashSet<String> uv = Sets.newHashSet();
        Integer pv = 0;
        Iterator<UserClick> iterator = elements.iterator();
        while (iterator.hasNext()){
            String userId = iterator.next().getUserId();
            uv.add(userId);
            pv = pv + 1;
        }
        out.collect(Tuple2.of("pv",pv));
        out.collect(Tuple2.of("uv",uv.size()));
    }
}
