package com.flink.example.stream.window;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * 功能：自定义窗口触发器
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/7/2 下午5:42
 */
public class CustomWindowTriggerExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomWindowTriggerExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Stream of (contentId, uid, clickCnt)
        DataStream<Tuple3<String, String, Long>> stream = source
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String contentId = params[0];
                        String uid = params[1];
                        Long clickCnt = Long.parseLong(params[2]);
                        LOG.info("[元素] ContentId: " + contentId + ", Uid: " + uid + ", ClickCnt: " + clickCnt);
                        return new Tuple3<>(contentId, uid, clickCnt);
                    }
                });

        // CountWindow
        DataStream<String> result = stream
                // 根据contentId分组
                .keyBy(new KeySelector<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                // 每3个用户一个窗口
                .window(GlobalWindows.create())
                .trigger(CustomCountTrigger.of(3))
                // 求和以及平均值
                .aggregate(new AverageAggregateFunction());

        result.print();
        env.execute("CustomWindowTriggerExample");
    }

    /**
     * 自定义Trigger
     * @param <W>
     */
    private static class CustomCountTrigger <W extends Window> extends Trigger<Object, W> {
        private Logger LOG = LoggerFactory.getLogger(CustomWindowTriggerExample.class);
        private Long maxCount;
        private ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("UidState", String.class);

        public CustomCountTrigger(Long maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
            // 获取uid信息
            Tuple3<String, String, Long> uidTuple = (Tuple3<String, String, Long>) element;
            String key = uidTuple.f0;
            String uid = uidTuple.f1;
            // 获取状态
            ListState<String> uidState = ctx.getPartitionedState(stateDescriptor);
            // 更新状态
            Iterable<String> iterable = uidState.get();
            List<String> uidList = Lists.newArrayList();
            if (!Objects.equals(iterable, null)) {
                uidList = Lists.newArrayList(iterable);
            }
            boolean isContains = uidList.contains(uid);
            if (!isContains) {
                uidList.add(uid);
                uidState.update(uidList);
            }

            // 大于等于3个用户触发计算
            if (uidList.size() >= maxCount) {
                LOG.info("[Trigger] Key: {} 触发计算并清除状态", key);
                uidState.clear();
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDescriptor).clear();
        }

        public static <W extends Window> CustomCountTrigger<W> of(long maxCount) {
            return new CustomCountTrigger<>(maxCount);
        }
    }

    /**
     * 自定义AggregateFunction
     */
    private static class AverageAggregateFunction implements AggregateFunction<Tuple3<String, String, Long>, Tuple3<String, Long, Long>, String> {

        // IN：Tuple2<String, Long>
        // ACC：Tuple3<String, Long, Long> -> <Key, Sum, Count>
        // OUT：String

        @Override
        public Tuple3<String, Long, Long> createAccumulator() {
            return new Tuple3<String, Long, Long>("", 0L, 0L);
        }

        @Override
        public Tuple3<String, Long, Long> add(Tuple3<String, String, Long> value, Tuple3<String, Long, Long> accumulator) {
            return new Tuple3<String, Long, Long>(value.f0, accumulator.f1 + value.f2, accumulator.f2 + 1L);
        }

        @Override
        public String getResult(Tuple3<String, Long, Long> accumulator) {
            String key = accumulator.f0;
            Long sum = accumulator.f1;
            Long count = accumulator.f2;
            double avg = ((double) sum) / count;
            String result = "Key: " + key + ", Sum: " + sum + ", Count: " + count + ", Avg: " + avg;
            LOG.info("[窗口] " + result);
            return result;
        }

        @Override
        public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> a, Tuple3<String, Long, Long> b) {
            return new Tuple3<String, Long, Long>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
// c10,ua,1
// c11,ua,2
// c10,ub,1
// c11,ub,4
// c10,uc,3
// c12,ua,5
// c11,uc,1
// c12,ub,2
// c12,uc,4