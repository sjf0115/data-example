package com.flink.example.stream.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Operator State Demo
 * Created by wy on 2020/11/21.
 */
public class OperatorStateExample {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorStateExample.class);

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        String topic = "weibo_behavior";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

        DataStreamSource<String> source = env.addSource(consumer).setParallelism(1);

        source.map(new BehaviorParseMapFunction()).setParallelism(1).uid("behavior-parse-map-function")
                .addSink(new BehaviorBufferingSink(10)).setParallelism(1).uid("behavior-buffering-sink");
        env.execute("operator-state-stream");
    }

    // 微博行为解析
    public static class BehaviorParseMapFunction extends RichMapFunction<String, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(String s) throws Exception {
            String[] params = s.split("\\s+");
            String uid = params[0];
            String wid = params[1];
            return new Tuple2<>(uid, wid);
        }
    }

    // 微博行为缓冲输出
    public static class BehaviorBufferingSink implements SinkFunction<Tuple2<String, String>>, CheckpointedFunction {

        private final int threshold;
        private transient ListState<Tuple2<String, String>> statePerPartition;
        private List<Tuple2<String, String>> bufferedElements;

        public BehaviorBufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Tuple2<String, String> behavior, Context context) throws Exception {
            bufferedElements.add(behavior);
            // 缓冲达到阈值输出
            if (bufferedElements.size() == threshold) {
                int index = 0;
                for (Tuple2<String, String> element: bufferedElements) {
                    // 输出 send it to the sink
                    LOG.info(index + ": " + element.toString());
                    index ++;
                }
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            LOG.info("snapshotState........");
            statePerPartition.clear();
            for (Tuple2<String, String> element : bufferedElements) {
                statePerPartition.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            LOG.info("initializeState........");
            ListStateDescriptor<Tuple2<String, String>> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})
            );

            statePerPartition = context.getOperatorStateStore().getListState(descriptor);

            // 状态恢复
            if (context.isRestored()) {
                for (Tuple2<String, String> element : statePerPartition.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
