package com.atguigu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkDataStream {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置CK & 状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //2.使用CDC建立FlinkSource
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210108-flink")
                .tableList("gmall-210108-flink.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3.打印
        streamSource.print();

        //4.启动
        env.execute();

    }

}
