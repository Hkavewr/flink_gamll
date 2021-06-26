package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        //TODO 5.将主流与广播流进行连接

        //TODO 6.根据广播流中发送来的数据将主流分为Kafka事实数据流和HBase维度数据流

        //TODO 7.将HBase数据写入Phoenix

        //TODO 8.将Kafka数据写入Kafka

        //TODO 9.启动
        env.execute();

    }

}
