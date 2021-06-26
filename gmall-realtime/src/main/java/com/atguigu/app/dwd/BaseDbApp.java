package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDebeziumDeserializationSchema;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class BaseDbApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置CK & 状态后端
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //TODO 2.读取Kafka ods_base_db 主题数据并转换为JSONObject
        String topic = "ods_base_db";
        String groupId = "ods_base_db_210108";
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env
                .addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId))
                .map(JSON::parseObject);

        //TODO 3.过滤空值数据  {"database":"","tableName":"","type":"","data":{"id":"1001",...},"before":{"id":"1001",...}}
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {

                //获取after数据
                String data = value.getString("data");

                if (data == null || data.length() <= 2) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        //打印测试
        filterDS.print();

        //TODO 4.使用FlinkCDC读取配置信息表并将该流转换为广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210108-realtime")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        //TODO 5.将主流与广播流进行连接
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastStream);

        //TODO 6.根据广播流中发送来的数据将主流分为 Kafka事实数据流 和 HBase维度数据流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> result = connectDS.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //打印测试
        result.print("Kafka>>>>>>>>>>>");
        DataStream<JSONObject> hbaseDS = result.getSideOutput(hbaseTag);
        hbaseDS.print("HBase>>>>>>>>>>>");

        //TODO 7.将HBase数据写入Phoenix

        //TODO 8.将Kafka数据写入Kafka

        //TODO 9.启动
        env.execute();

    }

}
