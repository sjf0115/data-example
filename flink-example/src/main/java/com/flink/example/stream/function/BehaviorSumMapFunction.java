package com.flink.example.stream.function;

import com.flink.example.bean.WBehavior;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.util.Objects;

/**
 * 微博求和
 * Created by wy on 2020/12/10.
 */
public class BehaviorSumMapFunction extends RichMapFunction<WBehavior, Long> {

    private ValueState<Long> counterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
        counterState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public Long map(WBehavior behavior) throws Exception {
        Long count = counterState.value();
        if (Objects.equals(count, null)) {
            count = 0L;
        }
        Long newCount = count + 1;
        counterState.update(newCount);
        System.out.println(behavior.getUid() + ":" + newCount);
        return newCount;
    }
}
