package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置CK & 状态后端
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //TODO 2.读取Kafka ods_base_log 主题数据创建流并转换为JSON对象
        String sourceTopic = "ods_base_log";
        String groupId = "ods_base_log_210108";

        OutputTag<String> dirtyData = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            //解析失败
                            ctx.output(dirtyData, value);
                        }
                    }
                });

        //测试
        jsonObjDS.print("Main>>>>>>>>>>>>>>>");
        jsonObjDS.getSideOutput(dirtyData).print("DirtyData>>>>>>>>>>");

        //TODO 3.按照Mid分组

        //TODO 4.新老用户校验

        //TODO 5.使用ProcessAPI中侧输出流进行分流处理

        //TODO 6.将不同流的数据写入不同的Kafka主题中

        //TODO 7.启动
        env.execute();

    }

}
