package org.myorg.quickstart.Dim19;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DimSync extends RichMapFunction<String,Order> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DimSync.class);

    private Connection conn = null;
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/dim?characterEncoding=UTF-8", "admin", "admin");
    }

    public Order map(String in) throws Exception {

        JSONObject jsonObject = JSONObject.parseObject(in);
        Integer cityId = jsonObject.getInteger("city_id");
        String userName = jsonObject.getString("user_name");
        String items = jsonObject.getString("items");

        //根据city_id 查询 city_name
        PreparedStatement pst = conn.prepareStatement("select city_name from info where city_id = ?");
        pst.setInt(1,cityId);
        ResultSet resultSet = pst.executeQuery();
        String cityName = null;
        while (resultSet.next()){
            cityName = resultSet.getString(1);
        }
        pst.close();
        return new Order(cityId,userName,items,cityName);
    }

    public void close() throws Exception {
        super.close();
        conn.close();
    }

}//
