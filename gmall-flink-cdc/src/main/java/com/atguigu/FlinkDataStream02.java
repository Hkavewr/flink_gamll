package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkDataStream02 {

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
                .deserializer(new MyDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3.打印
        streamSource.print();

        //4.启动
        env.execute();

    }

    public static class MyDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

        /**
         * {
         * "database":"",
         * "tableName":"",
         * "data":"{"id":"1001","name":"zhangsan"}",
         * "before":"",
         * "type":"insert"
         * }
         */
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //创建结果JSON
            JSONObject result = new JSONObject();

            //取出数据库&表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];
            String tableName = split[2];

            //取出数据本身
            Struct value = (Struct) sourceRecord.value();
            Struct after = value.getStruct("after");
            JSONObject afterData = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                List<Field> fields = schema.fields();
                for (Field field : fields) {
                    afterData.put(field.name(), after.get(field));
                }
            }

            Struct before = value.getStruct("before");
            JSONObject beforeData = new JSONObject();
            if (before != null) {
                Schema schema = before.schema();
                List<Field> fields = schema.fields();
                for (Field field : fields) {
                    beforeData.put(field.name(), before.get(field));
                }
            }

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)) {
                type = "insert";
            }

            //补充字段
            result.put("database", database);
            result.put("tableName", tableName);
            result.put("data", afterData);
            result.put("before", beforeData);
            result.put("type", type);

            //输出数据
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

}
