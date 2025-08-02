package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TestStreamWindow {
    public static void main(String[] args) throws Exception {
        // 禁用 Akka/Pekko 相关日志 - 使用正确的类型转换
        ch.qos.logback.classic.Logger akkaLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org.apache.flink.shaded.akka");
        akkaLogger.setLevel(Level.WARN);

        ch.qos.logback.classic.Logger pekkoLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org.apache.flink.runtime.rpc.pekko");
        pekkoLogger.setLevel(Level.WARN);

        ch.qos.logback.classic.Logger flinkRuntimeLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org.apache.flink.runtime");
        flinkRuntimeLogger.setLevel(Level.WARN);

        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.WARN);
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

        // 创建周期性的数据源
        DataStream<String> stream = env
                .socketTextStream("localhost", 9999) // 从socket读取数据
                .name("source-strings")
                .setParallelism(1);
        //  test(stream);
        //test1(stream);
        //test2(stream);
        test5(stream);
        test6(stream);
        env.execute();
    }

    private static void test(DataStream<String> stream) {
        /**
         * 滚动窗口（Tumbling Window）
         * 固定长度，无重叠。
         * 适用于周期性统计。
         */
        stream.print().name("printer");
        // 将字符串映射为元组 (word, 1)，然后进行窗口聚合
        stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                })
                .name("map-to-tuple")
                .keyBy(tuple -> tuple.f0) // 按照元组的第一个元素（即单词）分组
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5))) // 5秒的滚动窗口
                .sum(1) // 对元组的第二个字段（即计数）求和
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> tuple) throws Exception {
                        // 获取当前时间并格式化为 hh:mm:ss
                        String currentTime = LocalDateTime.now()
                                .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                        return String.format("[%s] %s: %d", currentTime, tuple.f0, tuple.f1);
                    }
                })
                .name("add-timestamp")
                .print("window-result")
                .name("window-aggregation");
    }

    /**
     * 滑动窗口（Sliding Window）
     * 固定长度，但可以重叠。
     * 滑动步长 < 窗口长度。
     */
    private static void test1(DataStream<String> stream) {
        stream.print().name("printer");
        // 窗口长10秒，每5秒滑动一次
        stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                })
                .name("map-to-sliding")
                .keyBy(tuple -> tuple.f0) // 按照元组的第一个元素（即单词）分组
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5))) // 5秒的滑动窗口
                .sum(1) // 对元组的第二个字段（即计数）求和
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> tuple) throws Exception {
                        // 获取当前时间并格式化为 hh:mm:ss
                        String currentTime = LocalDateTime.now()
                                .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                        return String.format("[%s] %s: %d", currentTime, tuple.f0, tuple.f1);
                    }
                })
                .name("add-timestamp")
                .print("window-result")
                .name("window-aggregation");
    }

    /**
     * 会话窗口（Session Window）
     * 基于“活跃期”划分，当一段时间内无数据到达时，自动关闭窗口。
     * 常用于用户行为分析（如一次会话）。
     */

    private static void test2(DataStream<String> stream) {
        stream.print().name("printer");
        // 会话间隔为7秒
        stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                })
                .name("map-to-session")
                .keyBy(tuple -> tuple.f0) // 按照元组的第一个元素（即单词）分组
                .window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(7))) // 会话间隔为7秒
                .sum(1) // 对元组的第二个字段（即计数）求和
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> tuple) throws Exception {
                        // 获取当前时间并格式化为 hh:mm:ss
                        String currentTime = LocalDateTime.now()
                                .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                        return String.format("[%s] %s: %d", currentTime, tuple.f0, tuple.f1);
                    }
                })
                .name("add-timestamp")
                .print("window-result")
                .name("window-aggregation");
    }

    /**
     * 滚动计数窗口
     * 每收集 N 个元素就触发一次计算。
     */
    private static void test5(DataStream<String> stream) {
        stream.print().name("printer");
        // 会话间隔为7秒
        stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                })
                .name("map-to-countWindowAll")
                .keyBy(tuple -> tuple.f0) // 按照元组的第一个元素（即单词）分组
                .countWindowAll(10) // 每10条数据触发一次
                .sum(1) // 对元组的第二个字段（即计数）求和
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> tuple) throws Exception {
                        // 获取当前时间并格式化为 hh:mm:ss
                        String currentTime = LocalDateTime.now()
                                .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                        return String.format("[%s] %s: %d", currentTime, tuple.f0, tuple.f1);
                    }
                })
                .name("add-timestamp")
                .print("window-result")
                .name("window-aggregation");
    }

    /**
     * 滑动计数窗口
     * 窗口大小为 N，滑动步长为 S。
     */
    private static void test6(DataStream<String> stream) {
        stream.print().name("printer");
        // 会话间隔为7秒
        stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                })
                .name("map-to-countWindowAll")
                .keyBy(tuple -> tuple.f0) // 按照元组的第一个元素（即单词）分组
                .countWindowAll(10,5) // 窗口大小为 N，滑动步长为 S。
                .sum(1) // 对元组的第二个字段（即计数）求和
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> tuple) throws Exception {
                        // 获取当前时间并格式化为 hh:mm:ss
                        String currentTime = LocalDateTime.now()
                                .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                        return String.format("[%s] %s: %d", currentTime, tuple.f0, tuple.f1);
                    }
                })
                .name("add-timestamp")
                .print("window-result")
                .name("window-aggregation");
    }

}
