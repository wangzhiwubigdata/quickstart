package org.myorg.quickstart.shizhan02;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisSink implements RedisMapper<Tuple3<String,String, Integer>>{

    /**
     * 设置redis数据类型
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,"flink_pv_uv");
    }

    @Override
    public String getKeyFromData(Tuple3<String, String, Integer> data) {
        return data.f1;
    }

    @Override
    public String getValueFromData(Tuple3<String, String, Integer> data) {
        return data.f2.toString();
    }
}
