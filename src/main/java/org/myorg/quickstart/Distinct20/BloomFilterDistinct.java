package org.myorg.quickstart.Distinct20;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BloomFilterDistinct extends KeyedProcessFunction<Long, String, Long> {

    private transient ValueState<BloomFilter> bloomState;
    private transient ValueState<Long> countState;


    @Override
    public void processElement(String value, Context ctx, Collector<Long> out) throws Exception {

        BloomFilter bloomFilter = bloomState.value();
        Long skuCount = countState.value();

        if(bloomFilter == null){
            BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000000);
        }

        if(skuCount == null){
            skuCount = 0L;
        }

        if(!bloomFilter.mightContain(value)){
            bloomFilter.put(value);
            skuCount = skuCount + 1;
        }

        bloomState.update(bloomFilter);
        countState.update(skuCount);
        out.collect(countState.value());
    }
}//
