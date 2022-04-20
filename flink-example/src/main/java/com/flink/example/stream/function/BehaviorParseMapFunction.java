package com.flink.example.stream.function;

import com.common.example.bean.Behavior;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 微博数据集 数据解析 MapFunction
 * Created by wy on 2020/11/22.
 */
public class BehaviorParseMapFunction extends RichMapFunction<String, Behavior> {
    @Override
    public Behavior map(String s) throws Exception {
        String[] params = s.split("\\s+");
        int size = params.length;
        Behavior behavior = new Behavior();
        if (size > 0) {
            behavior.setUid(params[0]);
        }
        if (size > 1) {
            behavior.setWid(params[1]);
        }
        if (size > 3) {
            behavior.setTm(params[2] + " " + params[3]);
        }
        if (size > 4) {
            behavior.setContent(params[4]);
        }
        return behavior;
    }
}
