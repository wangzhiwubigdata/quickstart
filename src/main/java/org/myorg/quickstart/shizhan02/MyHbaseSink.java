package org.myorg.quickstart.shizhan02;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class MyHbaseSink extends RichSinkFunction<Tuple3<String, String, Integer>> {


    private transient Connection connection;
    private transient List<Put> puts = new ArrayList<>(100);



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181");
        connection = ConnectionFactory.createConnection(conf);
    }

    @Override
    public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception {

        String tableName = "database:pvuv_result";
        String family = "f";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(value.f0.getBytes());
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(value.f1),Bytes.toBytes(value.f2));
        puts.add(put);

        if(puts.size() == 100){
            table.put(puts);
            puts.clear();
        }
        table.close();
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}//
