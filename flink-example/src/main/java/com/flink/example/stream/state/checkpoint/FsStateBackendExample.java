package com.flink.example.stream.state.checkpoint;

import com.flink.example.stream.connector.custom.SimpleCustomSource;
import com.flink.example.stream.function.MyStatefulMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FsStateBackend
 * Created by wy on 2020/12/9.
 */
public class FsStateBackendExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoint"));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Tuple2<String, Integer>> source = env.addSource(new SimpleCustomSource()).uid("simple-custom-source");
        source.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.getField(0);
                    }
                })
                .map(new MyStatefulMapFunction()).uid("my-stateful-map-function")
                .print();

        env.execute("fsStateBackend-example");
    }
}
