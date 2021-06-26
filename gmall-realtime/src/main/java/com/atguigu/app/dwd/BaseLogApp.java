package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


//数据流: web/app -> Nginx -> 日志服务器 -> Kafka(ods_base_log) -> FlinkApp   -> Kafka(dwd三个主题)
//程  序: mock    -> Nginx -> logger.sh -> Kafka(zk,ods_base_log) -> BaseLogApp -> Kafka(dwd三个主题)
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
//        jsonObjDS.print("Main>>>>>>>>>>>>>>>");
        jsonObjDS.getSideOutput(dirtyData).print("DirtyData>>>>>>>>>>");

        //TODO 3.按照Mid分组
        KeyedStream<JSONObject, String> midKeyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //TODO 4.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = midKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is-new", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取新老用户标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //判断前台校验为新用户
                if ("1".equals(isNew)) {

                    //取出状态数据
                    String state = valueState.value();

                    //判断状态是否有数据
                    if (state != null) {
                        //矫正新老用户标记
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //将状态更新
                        valueState.update("0");
                    }
                }

                //返回结果
                return value;
            }
        });

        //打印测试
        //jsonObjWithNewFlagDS.print();

        //TODO 5.使用ProcessAPI中侧输出流进行分流处理  将数据分成页面数据、启动数据、曝光数据
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                //尝试获取"start"数据
                String start = value.getString("start");
                if (start != null && start.length() > 0) {

                    //启动日志,将数据写入侧输出流
                    ctx.output(startTag, value.toJSONString());

                } else {

                    //页面日志,将数据写入主流
                    out.collect(value.toJSONString());

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {

                        //遍历获取单个曝光数据,并写出到侧输出流
                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);

                            //将页面的信息放入曝光数据中
                            String pageId = value.getJSONObject("page").getString("page_id");
                            display.put("page_id", pageId);

                            //将数据写出
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 6.将不同流的数据写入不同的Kafka主题中
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("Page>>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //TODO 7.启动
        env.execute();

    }

}
