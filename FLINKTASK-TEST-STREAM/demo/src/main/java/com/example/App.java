package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class App {
    public static void main(String[] args) {
        try {
            // 创建本地配置
            Configuration conf = new Configuration();

            // Web UI 配置
            conf.setString("rest.bind-port", "8081"); // 设置Web UI端口
            conf.setString("rest.bind-address", "0.0.0.0"); // 绑定所有网络接口
            conf.setString("rest.address", "localhost"); // 设置Web UI地址
            conf.setString("rest.enable", "true"); // 启用REST服务
            conf.setString("web.submit.enable", "true"); // 允许通过Web UI提交作业
            conf.setString("web.upload.dir", System.getProperty("java.io.tmpdir")); // 设置上传目录
            conf.setString("web.access-control-allow-origin", "*"); // 允许跨域访问

            // 使用配置创建支持Web UI的执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

            // 设置为流处理模式
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

            // 基本配置
            env.setParallelism(1); // 设置并行度为1
            env.disableOperatorChaining(); // 禁用算子链，使执行更清晰

            // 禁用检查点，因为是简单的演示程序
            env.getCheckpointConfig().disableCheckpointing();

            // 创建周期性的数据源
            DataStream<String> text = env
                    .socketTextStream("localhost", 9999) // 从socket读取数据
                    .name("source-strings")
                    .setParallelism(1);

            // 转换算子 filter:过滤包含“Flink”的输入
            text.filter(line -> line.contains("Flink"))
                    .name("filter-flink-strings")
                    .setParallelism(1)
                    .map(String::toUpperCase)
                    .name("uppercase-mapper")
                    .setParallelism(1)
                    .print()
                    .name("printer");

            // 转换算子 map: 将每行数据前添加“Processed: ”并转为大写
            text.map(line -> "Processed: " + line.toUpperCase())
                    .name("map-processed-strings")
                    .setParallelism(1)
                    .print()
                    .name("printer-processed");

            // 转换算子 flatMap: 将每行数据拆分为单词
            text.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String line, Collector<String> out) {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                }
            }).name("flatmap-split-words")
                    .setParallelism(1)
                    .print()
                    .name("printer-split-words");

            // 转换算子 keyBy: 按单词分组并计数
            text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
                    for (String word : line.split(" ")) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }
            }).name("flatmap-split-words")
                    .setParallelism(1)
                    .keyBy(tuple -> tuple.f0) // 按单词分组
                    .sum(1) // 计算每个单词的出现次数
                    .print()
                    .name("printer-word-count");

            // 转换算子 reduce: 规约合并单词
            text.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String line, Collector<String> out) {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                }
            }).name("flatmap-split-words")
                    .setParallelism(1)
                    .keyBy(word -> word) // 按单词分组
                    .reduce((word1, word2) -> word1 + ", " + word2) // 合并单词
                    .print()
                    .name("printer-word-reduce");

            // 转换算子 union: 合并两个数据流
            DataStream<String> anotherText = env
                    .fromSequence(1, Long.MAX_VALUE) // 持续生成数据
                    .map(i -> {
                        try {
                            Thread.sleep(3000); // 每3秒生成一条消息
                            return "Stream2> Auto Message " + i + ": Hello Flink";
                        } catch (InterruptedException e) {
                            return "Stream2> Error occurred";
                        }
                    })
                    .name("source-another-strings")
                    .setParallelism(1);

            // 将两个流合并并处理
            text.map(str -> "Stream1> " + str) // 为第一个流添加前缀
                    .union(anotherText) // 合并两个数据流
                    .filter(str -> str.contains(":")) // 过滤掉不符合格式的数据
                    .map(str -> {
                        String[] parts = str.split(">");
                        return String.format("%-8s | %s",
                                parts[0].trim() + ">", // 对齐源标识
                                parts[1].trim()); // 消息内容
                    })
                    .print()
                    .name("printer-union");

            // 执行任务
            env.execute("Flink Streaming Java API Hello");

        } catch (Exception e) {
            System.err.println("任务执行失败：" + e.getMessage());
            e.printStackTrace();
        }
    }
}
