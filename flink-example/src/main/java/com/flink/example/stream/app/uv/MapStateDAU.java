package com.flink.example.stream.app.uv;

import com.common.example.bean.LoginUser;
import com.common.example.utils.DateUtil;
import com.flink.example.stream.connector.print.PrintLogSinkFunction;
import com.flink.example.stream.source.simple.DAUMockSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

/**
 * 功能：通过滚动窗口计算每天DAU 每1分钟输出一次
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/23 下午7:22
 */
public class MapStateDAU {
    private static final Logger LOG = LoggerFactory.getLogger(MapStateDAU.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LoginUser> source = env.addSource(new DAUMockSource());

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
                .keyBy(new KeySelector<LoginUser, Integer>() {
                    @Override
                    public Integer getKey(LoginUser user) throws Exception {
                        return user.getAppId();
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

    // 未完
    private static class UVProcessWindowFunction extends ProcessWindowFunction<LoginUser, Tuple2<String, Long>, Integer, TimeWindow> {
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
        public void process(Integer integer, Context context, Iterable<LoginUser> elements, Collector<Tuple2<String, Long>> out) throws Exception {
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
            LOG.info("process elements: {}", size);
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
