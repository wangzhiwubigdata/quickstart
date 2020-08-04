package org.myorg.quickstart.shizhan02;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class UserActionProcessFunction extends ProcessFunction<String, String> {
    @Override
    public void processElement(String input, Context ctx, Collector<String> out) throws Exception {

        if(! input.contains("CLICK") || input.startsWith("{") || input.endsWith("}")){
            return;
        }

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
