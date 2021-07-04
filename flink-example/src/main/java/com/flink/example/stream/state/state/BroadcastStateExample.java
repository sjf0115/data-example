package com.flink.example.stream.state.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：Broadcast State
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/7/4 上午11:42
 */
public class BroadcastStateExample {
    private static final Logger LOG = LoggerFactory.getLogger(BroadcastStateExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        DataStream<String> actionsSource = env.socketTextStream("localhost", 9100, "\n");
        DataStream<String> patternsSource = env.socketTextStream("localhost", 9101, "\n");

        // 行为流 (uid, action)
        DataStream<Action> actionsStream = actionsSource
                .map(new MapFunction<String, Action>() {
                    @Override
                    public Action map(String str) throws Exception {
                        String[] params = str.split(",");
                        String uid = params[0];
                        String action = params[1];
                        LOG.info("[行为流] uid: " + uid + ", action: " + action);
                        return new Action(uid, action);
                    }
                });

        // 模式流 (firstAction, secondAction)
        DataStream<Pattern> patternsStream = patternsSource
                .map(new MapFunction<String, Pattern>() {
                    @Override
                    public Pattern map(String str) throws Exception {
                        String[] params = str.split(",");
                        String firstAction = params[0];
                        String secondAction = params[1];
                        LOG.info("[模式流] firstAction: " + firstAction + ", secondAction: " + secondAction);
                        return new Pattern(firstAction, secondAction);
                    }
                });

        // 根据uid分组
        KeyedStream<Action, String> actionsByUser = actionsStream.keyBy(new KeySelector<Action, String>() {
            @Override
            public String getKey(Action action) throws Exception {
                return action.uid;
            }
        });

        // 广播状态
        MapStateDescriptor<Void, Pattern> stateDescriptor = new MapStateDescriptor<>(
                "patternsState", Types.VOID, Types.POJO(Pattern.class)
        );
        // 广播流
        BroadcastStream broadcastStream = patternsStream.broadcast(stateDescriptor);

        // 关联
        DataStream<Tuple2<Long, Pattern>> matches = actionsByUser
                .connect(broadcastStream)
                .process(new PatternEvaluatorProcessFunction());

        matches.print();
        env.execute("BroadcastStateExample");
    }

    /**
     * 模式匹配
     */
    public static class PatternEvaluatorProcessFunction
            extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {

        // 前一个行为
        private ValueState<String> prevActionState;
        // 模式
        private MapStateDescriptor<Void, Pattern> patternDesc;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化 KeyedState
            prevActionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastAction", Types.STRING)
            );
            patternDesc = new MapStateDescriptor<>("patternsState", Types.VOID, Types.POJO(Pattern.class));
        }

        @Override
        public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            String uid = action.uid;
            String lastAction = action.action;

            // 从广播状态中获取模式
            Pattern pattern = ctx.getBroadcastState(this.patternDesc)
                    // access MapState with null as VOID default value
                    .get(null);

            // 获取当前用户的前一个行为
            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                String firstAction = pattern.firstAction;
                String secondAction = pattern.secondAction;
                // 模式是否匹配
                boolean isMatch = false;
                if (firstAction.equals(prevAction) && secondAction.equals(lastAction)) {
                    isMatch = true;
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
                LOG.info("[Evaluation] uid: {}, Action: [{}->{}], Pattern: [{}->{}], IsMatch: {}",
                        uid, prevAction, lastAction, firstAction, secondAction, isMatch
                );
            }
            // 用最新行为更新状态
            prevActionState.update(lastAction);
        }

        @Override
        public void processBroadcastElement(Pattern pattern, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 如果有新模式则更新广播状态
            BroadcastState<Void, Pattern> broadcastState = ctx.getBroadcastState(patternDesc);
            broadcastState.put(null, pattern);
        }
    }

    public static class Action {
        public String uid;
        public String action;

        public Action() {}

        public Action(String uid, String action) {
            this.uid = uid;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "uid='" + uid + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String firstAction;
        public String secondAction;

        public Pattern() {}

        public Pattern(String firstAction, String secondAction) {
            this.firstAction = firstAction;
            this.secondAction = secondAction;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "firstAction='" + firstAction + '\'' +
                    ", secondAction='" + secondAction + '\'' +
                    '}';
        }
    }
}
// 模式流
// login,logout

// 行为流
// 1001,login
// 1003,pay
// 1002,cart
// 1001,logout
// 1002,pay

// 模式流
// cart,logout

// 行为流
// 1003,cart
// 1002,logout
// 1003,logout

