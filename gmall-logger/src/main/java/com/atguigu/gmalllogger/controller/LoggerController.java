package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController // @Controller+@ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/test")
    //@ResponseBody
    public String test1() {
        System.out.println("success");
        return "success";
    }

    @RequestMapping("/test2")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String jsonStr) {

        //1.打印日志
        //System.out.println(jsonStr);

        //2.将日志写入磁盘
        log.info(jsonStr);

        //3.将日志写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        //4.返回
        return "success";
    }
}
