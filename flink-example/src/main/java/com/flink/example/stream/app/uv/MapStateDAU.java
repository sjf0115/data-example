package com.flink.example.stream.app.uv;

import com.common.example.bean.LoginUser;
import com.common.example.utils.DateUtil;
import com.flink.example.stream.connector.print.PrintLogSinkFunction;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/**
 * 功能：通过滚动窗口计算每天DAU 每1分钟输出一次
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/23 下午7:22
 */
public class MapStateDAU {
    private static final Logger LOG = LoggerFactory.getLogger(MapStateDAU.class);
    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 配置 Checkpoint 每40s触发一次Checkpoint 实际不用设置的这么大
        env.enableCheckpointing(40*1000);
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        // 配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // 配置 Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "user-login");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        String topic = "user_login";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        // Kafka Source
        DataStream<LoginUser> source = env.addSource(consumer).uid("KafkaSource")
                .map(new MapFunction<String, LoginUser>() {
            @Override
            public LoginUser map(String value) throws Exception {
                LoginUser user = gson.fromJson(value, LoginUser.class);
                LOG.info("map elements: {}", value);
                // 模拟脏数据失败
//                if(Objects.equals(user.getTimestamp(), 1665417356000L)) {
//                    LOG.error("出现脏数据失败");
//                    throw new RuntimeException("出现脏数据失败");
//                }
                return user;
            }
        });

        //DataStreamSource<LoginUser> source = env.addSource(new DAUMockSource());

        SingleOutputStreamOperator<Tuple2<String, Long>> result = source
                // 设置Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginUser>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginUser>() {
                                    @Override
                                    public long extractTimestamp(LoginUser user, long recordTimestamp) {
                                        return user.getTimestamp();
                                    }
                                })
                )
                .keyBy(new KeySelector<LoginUser, String>() {
                    @Override
                    public String getKey(LoginUser user) throws Exception {
                        String date = DateUtil.timeStamp2Date(user.getTimestamp(), "yyyyMMdd");
                        // 分区 Key 添加上了日期
                        return user.getAppId() + "#" + date;
                    }
                })
                // 事件时间滚动窗口 滚动大小一天
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 周期性事件时间触发器 每10s触发一次计算
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))
                // 优化项 删除之前已经参与计算过的记录
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                // UV 去重
                .process(new UVProcessWindowFunction());

        // 打印日志并输出到控制台
        result.addSink(new PrintLogSinkFunction());
        env.execute("MapStateDAU");
    }

    private static class UVProcessWindowFunction extends ProcessWindowFunction<LoginUser, Tuple2<String, Long>, String, TimeWindow> {
        // 存储每天的用户明细
        private MapState<Long, Boolean> userSetState;
        // 存储当日截止到当前的UV
        private ValueState<Long> uvState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 过期状态清除
            StateTtlConfig stateTtlConfig = StateTtlConfig
                    .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                    .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            // 用户明细状态
            MapStateDescriptor<Long, Boolean> userSetDescriptor = new MapStateDescriptor<>("user-set", Long.class, Boolean.class);
            userSetDescriptor.enableTimeToLive(stateTtlConfig);
            userSetState = getRuntimeContext().getMapState(userSetDescriptor);
            // 用户UV状态
            ValueStateDescriptor<Long> uvDescriptor = new ValueStateDescriptor<>("uv", Long.class);
            uvDescriptor.enableTimeToLive(stateTtlConfig);
            uvState = getRuntimeContext().getState(uvDescriptor);
        }

        @Override
        public void process(String key, Context context, Iterable<LoginUser> elements, Collector<Tuple2<String, Long>> out) throws Exception {
            if (Objects.equals(uvState.value(), null)) {
                uvState.update(0L);
            }

            Long uv = uvState.value();
            int size = 0;
            for (LoginUser user : elements) {
                size ++;
                Long uid = user.getUid();
                // 如果之前没有出现过
                if (!userSetState.contains(uid)) {
                    uv ++;
                    userSetState.put(uid, true);
                }
            }
            LOG.info("window process elements: {}", size);
            // 更新UV状态
            uvState.update(uv);
            // 窗口开始时间
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            String date = DateUtil.timeStamp2Date(start, "yyyyMMdd");
            LOG.info("window: {}, day: {}, uv: {}", "[" + start + "," + end + "]", date, uv);
            out.collect(Tuple2.of(date, uv));
        }

        @Override
        public void clear(Context context) throws Exception {
            LOG.info("clear............");
            super.clear(context);
            userSetState.clear();
            uvState.clear();
        }
    }

}
