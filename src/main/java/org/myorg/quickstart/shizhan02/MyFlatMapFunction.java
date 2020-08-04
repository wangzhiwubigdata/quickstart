package org.myorg.quickstart.shizhan02;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class MyFlatMapFunction implements FlatMapFunction<String,String> {
    @Override
    public void flatMap(String input, Collector out) throws Exception {

        JSONObject jsonObject = JSON.parseObject(input);
        String user_id = jsonObject.getString("user_id");
        String action = jsonObject.getString("action");
        Long timestamp = jsonObject.getLong("timestamp");

        if(!StringUtils.isEmpty(user_id) || !StringUtils.isEmpty(action)){
            UserClick userClick = new UserClick();
            userClick.setUserId(user_id);
            userClick.setTimestamp(timestamp);
            userClick.setAction(action);

            out.collect(JSON.toJSONString(userClick));
        }
    }
}
