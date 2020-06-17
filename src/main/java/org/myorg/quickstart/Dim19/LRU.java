package org.myorg.quickstart.Dim19;

import com.alibaba.fastjson.JSONObject;
import com.stumbleupon.async.Callback;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class LRU extends RichAsyncFunction<String,Order> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LRU.class);
    String table = "info";
    Cache<String, String> cache = null;
    private HBaseClient client = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //创建hbase客户端
        client = new HBaseClient("127.0.0.1","7071");
        cache = CacheBuilder.newBuilder()
                //最多存储10000条
                .maximumSize(10000)
                //过期时间为1分钟
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Order> resultFuture) throws Exception {

        JSONObject jsonObject = JSONObject.parseObject(input);
        Integer cityId = jsonObject.getInteger("city_id");
        String userName = jsonObject.getString("user_name");
        String items = jsonObject.getString("items");
        //读缓存
        String cacheCityName = cache.getIfPresent(cityId);
        //如果缓存获取失败再从hbase获取维度数据
        if(cacheCityName != null){
            Order order = new Order();
            order.setCityId(cityId);
            order.setItems(items);
            order.setUserName(userName);
            order.setCityName(cacheCityName);
            resultFuture.complete(Collections.singleton(order));
        }else {

            client.get(new GetRequest(table,String.valueOf(cityId))).addCallback((Callback<String, ArrayList<KeyValue>>) arg -> {
                for (KeyValue kv : arg) {
                    String value = new String(kv.value());
                    Order order = new Order();
                    order.setCityId(cityId);
                    order.setItems(items);
                    order.setUserName(userName);
                    order.setCityName(value);
                    resultFuture.complete(Collections.singleton(order));
                    cache.put(String.valueOf(cityId), value);
                }
                return null;
            });

        }
    }

}
