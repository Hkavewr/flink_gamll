package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用FlinkSql方式读取MySQL变化数据
        tableEnv.executeSql("CREATE TABLE base_trademark ( " +
                " id string, " +
                " tm_name STRING, " +
                " logo_url STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = 'hadoop102', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '000000', " +
                " 'database-name' = 'gmall-210108-flink', " +
                " 'table-name' = 'base_trademark' " +
                ")");

        //3.打印
        Table table = tableEnv.sqlQuery("select * from base_trademark");
        tableEnv.toRetractStream(table, Row.class).print();

        //4.启动
        env.execute();

    }

}
