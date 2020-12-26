package com.flink.example.stream.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Objects;

/**
 * Stateful MapFunction
 * Created by wy on 2020/12/26.
 */
public class MyStatefulMapFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Long>> {
    private ValueState<Long> counterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
        counterState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public Tuple2<String, Long> map(Tuple2<String, Integer> tuple) throws Exception {
        Long count = counterState.value();
        if (Objects.equals(count, null)) {
            count = 0L;
        }
        Long newCount = count + tuple.f1;
        counterState.update(newCount);
        return new Tuple2<>(tuple.f0, newCount);
    }
}
