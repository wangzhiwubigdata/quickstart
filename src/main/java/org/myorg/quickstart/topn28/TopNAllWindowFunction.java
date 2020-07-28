package org.myorg.quickstart.topn28;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;
import scala.collection.Iterator;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class TopNAllWindowFunction extends ProcessAllWindowFunction<OrderDetail,Tuple2<Double,OrderDetail>,TimeWindow>{

    private int size = 10;

    public TopNAllWindowFunction(int size) {
        this.size = size;
    }


    @Override
    public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {

        TreeMap<Double,OrderDetail> treeMap = new TreeMap<Double,OrderDetail>(new Comparator<Double>() {
            @Override
            public int compare(Double x, Double y) {
                return (x < y) ? -1 : 1;
            }
        });

        Iterator<OrderDetail> iterator = elements.iterator();
        if(iterator.hasNext()){
            treeMap.put(iterator.next().getPrice(),iterator.next());
            if(treeMap.size() > 10){
                treeMap.pollLastEntry();
            }
        }

        for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
            out.collect(Tuple2.of(entry.getKey(),entry.getValue()));
        }


    }
}//
