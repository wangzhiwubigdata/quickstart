package org.myorg.quickstart.DataSkew;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class CountProcessFunction extends KeyedProcessFunction<String, CountRecord, CountRecord> {

    private ValueState<Long> state = this.getRuntimeContext().getState(new ValueStateDescriptor("count",Long.class));
    @Override
    public void processElement(CountRecord value, Context ctx, Collector<CountRecord> out) throws Exception {

        if(state.value()==0){
            state.update(value.count);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L * 5);
        }else{
            state.update(state.value() + value.count);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CountRecord> out) throws Exception {

        //这里可以做业务操作，例如每5分钟将统计结果发送出去
        //out.collect(...);
        //清除状态
        state.clear();

        //注册新的定时器
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L * 5);

    }
}
