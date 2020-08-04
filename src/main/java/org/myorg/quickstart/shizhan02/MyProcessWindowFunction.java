package org.myorg.quickstart.shizhan02;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class MyProcessWindowFunction extends ProcessWindowFunction<UserClick,Tuple3<String,String, Integer>,String,TimeWindow>{

    private transient MapState<String, String> uvState;
    private transient ValueState<Integer> pvState;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        uvState = this.getRuntimeContext().getMapState(new MapStateDescriptor<>("uv", String.class, String.class));
        pvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("pv", Integer.class));
    }

    @Override
    public void process(String s, Context context, Iterable<UserClick> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {

        Integer pv = 0;
        Iterator<UserClick> iterator = elements.iterator();
        while (iterator.hasNext()){
            pv = pv + 1;
            String userId = iterator.next().getUserId();
            uvState.put(userId,null);
        }
        pvState.update(pvState.value() + pv);

        Integer uv = 0;
        Iterator<String> uvIterator = uvState.keys().iterator();
        while (uvIterator.hasNext()){
            String next = uvIterator.next();
            uv = uv + 1;
        }

        Integer value = pvState.value();
        if(null == value){
            pvState.update(pv);
        }else {
            pvState.update(value + pv);
        }

        out.collect(Tuple3.of(s,"uv",uv));
        out.collect(Tuple3.of(s,"pv",pvState.value()));
    }
}
