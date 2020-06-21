package org.myorg.quickstart.Distinct20;


import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MapStateDistinctFunction extends KeyedProcessFunction<String,Tuple2<String,Integer>,Tuple2<String,Integer>> {

    private transient ValueState<Integer> counts;

    @Override
    public void open(Configuration parameters) throws Exception {

        //我们设置ValueState的TTL的生命周期为24小时，到期自动清除状态
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(org.apache.flink.api.common.time.Time.minutes(24 * 60))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        //设置ValueState的默认值
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("skuNum", Integer.class);
        descriptor.enableTimeToLive(ttlConfig);
        counts = getRuntimeContext().getState(descriptor);
        super.open(parameters);
    }


    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

        String f0 = value.f0;

        //如果不存在则新增
        if(counts.value() == null){
            counts.update(1);
        }else{
            //如果存在则加1
            counts.update(counts.value()+1);
        }

        out.collect(Tuple2.of(f0, counts.value()));

    }

}//
