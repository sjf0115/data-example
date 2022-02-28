package com.flink.example.stream.state.state;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
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
        // 在事件时间模式下使用处理时间语义
        env.getConfig().setAutoWatermarkInterval(0);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 设置Checkpoint存储
        env.enableCheckpointing(10000L);
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        FileSystemCheckpointStorage checkpointStorage = new FileSystemCheckpointStorage(checkpointPath);
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, Long>> stream = source.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String uid) throws Exception {
                long loginTime = System.currentTimeMillis();
                String date = DateUtil.timeStamp2Date(loginTime);
                LOG.info("[Login] uid: {}, LoginTime: [{}|{}]", uid, loginTime, date);
                return new Tuple2<String, Long>(uid, loginTime);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            // 记录有效期内的首次登录时间
            private ValueState<Long> lastLoginState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 状态描述符
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("LastLoginState", Long.class);
                // 设置 TTL
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.minutes(1))
                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        //.cleanupFullSnapshot()
                        .cleanupIncrementally(10, false)
                        //.cleanupInRocksdbCompactFilter(1000)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastLoginState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> tuple2) throws Exception {
                Long loginTime = tuple2.f1;
                String uid = tuple2.f0;
                Long lastLoginTime = lastLoginState.value();
                if (Objects.equals(lastLoginTime, null)) {
                    lastLoginTime = loginTime;
                }
                if (loginTime < lastLoginTime) {
                    lastLoginTime = loginTime;
                }
                lastLoginState.update(lastLoginTime);
                String date = DateUtil.timeStamp2Date(lastLoginTime);
                LOG.info("[State] uid: {}, LastLoginTime: [{}|{}]", uid, lastLoginTime, date);
                return new Tuple2<>(uid, lastLoginTime);
            }
        });

        stream.print();
        env.execute("StateTTLExample");
    }
}
// a
// b
// a
// 一分钟后
// a
// b