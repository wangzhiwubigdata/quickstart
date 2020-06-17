package org.myorg.quickstart.Dim19;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WholeLoad extends RichMapFunction<String,Order> {


    private static final Logger LOGGER = LoggerFactory.getLogger(WholeLoad.class);
    ScheduledExecutorService executor = null;
    private Map<String,String> cache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    load();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },5,5, TimeUnit.MINUTES);
    }

    @Override
    public Order map(String value) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        Integer cityId = jsonObject.getInteger("city_id");
        String userName = jsonObject.getString("user_name");
        String items = jsonObject.getString("items");
        String cityName = cache.get(cityId);
        return new Order(cityId,userName,items,cityName);
    }

    public void load() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/dim?characterEncoding=UTF-8", "admin", "admin");
        PreparedStatement statement = con.prepareStatement("select city_id,city_name from info");
        ResultSet rs = statement.executeQuery();
        //全量更新维度数据到内存
        while (rs.next()) {
            String cityId = rs.getString("city_id");
            String cityName = rs.getString("city_name");
            cache.put(cityId, cityName);
        }
        con.close();
    }
}
