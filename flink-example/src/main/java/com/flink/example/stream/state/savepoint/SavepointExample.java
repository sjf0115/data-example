package com.flink.example.stream.state.savepoint;

import com.flink.example.stream.function.MyStatefulMapFunction;
import com.flink.example.stream.source.SimpleCustomSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Savepoint Example
 * Created by wy on 2020/12/26.
 */
public class SavepointExample {
    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);
        //final String checkpointDir = pt.getRequired("checkpoint.dir");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://localhost:9000/flink/checkpoint"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(1000L);
        env.getConfig().disableGenericTypes();

        env.addSource(new SimpleCustomSource()).uid("simple-custom-source")
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.getField(0);
                    }
                })
                .map(new MyStatefulMapFunction()).uid("my-stateful-map-function")
                .print();
        env.execute();
    }
}
