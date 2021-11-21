package com.flink.example.stream.state.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：状态过期
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/6/26 下午12:46
 */
public class StateTTLExample {

    private static final Logger LOG = LoggerFactory.getLogger(StateTTLExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Long> stream = source.map(new RichMapFunction<String, Long>() {
            private ValueState<Long> counterState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // TTL 配置
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.minutes(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();

                // 状态描述符
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
                // 设置 TTL
                stateDescriptor.enableTimeToLive(ttlConfig);
                counterState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Long map(String behavior) throws Exception {
                Long count = counterState.value();
                if (Objects.equals(count, null)) {
                    count = 0L;
                }
                Long newCount = count + 1;
                LOG.info("[State] Counter: {}", newCount);
                counterState.update(newCount);
                return newCount;
            }
        });
        stream.print();

        env.execute("StateTTLExample");
    }
}
