package org.myorg.quickstart.shizhan02;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Iterator;

public class MyProcessWindowFunctionBitMap extends ProcessWindowFunction<UserClick,Tuple3<String,String, Integer>,String,TimeWindow>{

    private transient ValueState<Integer> uvState;
    private transient ValueState<Integer> pvState;
    private transient ValueState<Roaring64NavigableMap> bitMapState;


    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        uvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("pv", Integer.class));
        pvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("uv", Integer.class));
        bitMapState = this.getRuntimeContext().getState(new ValueStateDescriptor("bitMap", TypeInformation.of(new TypeHint<Roaring64NavigableMap>() {
        })));
    }

    @Override
    public void process(String s, Context context, Iterable<UserClick> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {

        Integer uv = uvState.value();
        Integer pv = pvState.value();
        Roaring64NavigableMap bitMap = bitMapState.value();

        if(bitMap == null){
            bitMap = new Roaring64NavigableMap();
            uv = 0;
            pv = 0;
        }

        Iterator<UserClick> iterator = elements.iterator();
        while (iterator.hasNext()){
            pv = pv + 1;
            String userId = iterator.next().getUserId();
            //如果userId可以转成long
            bitMap.add(Long.valueOf(userId));
        }

        out.collect(Tuple3.of(s,"uv",bitMap.getIntCardinality()));
        out.collect(Tuple3.of(s,"pv",pv));

    }
}
