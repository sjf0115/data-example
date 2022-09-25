package com.flink.example.stream.state.checkpoint;

import com.flink.example.stream.function.BehaviorParseMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：Checkpoint 参数示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class CheckpointExample {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 显示启动 Checkpoint 每1s触发(启动)一个新的 Checkpoint
        env.enableCheckpointing(1000);

        // Checkpoint 模式 默认为 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // Checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 最多同时执行的 Checkpoint 个数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 外部持久化 Checkpoint 即使作业取消也可以保留 Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 可容忍的 Checkpoint 失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        // 启用非对齐 Checkpoint
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9100, "\n")
                .setParallelism(1);
        source.map(new BehaviorParseMapFunction())
                .setParallelism(1)
                .uid("behavior-parse-map-function");

        env.execute("checkpoint-stream");
    }
}
