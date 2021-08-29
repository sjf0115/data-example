package com.flink.example.stream.state.statebackend;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：RocksDBStateBackend
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/7/11 下午9:19
 */
public class RocksDBStateBackendExample {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackendExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStateBackend(new RocksDBStateBackend("hdfs:///fink-checkpoints", true));

        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Stream of (key, timestamp)
        DataStream<Tuple3<String, Long, Integer>> stream = source.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String time = params[1];
                Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                LOG.info("[Element] Key: " + key + ", timeStamp: [" + timeStamp + "|" + time + "]");
                return new Tuple3(key, timeStamp, 1);
            }
        });
    }
}
