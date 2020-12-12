package com.flink.example.stream.function;

import com.flink.example.bean.WBehavior;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 微博数据集 数据解析 MapFunction
 * Created by wy on 2020/11/22.
 */
public class BehaviorParseMapFunction extends RichMapFunction<String, WBehavior> {
    @Override
    public WBehavior map(String s) throws Exception {
        String[] params = s.split("\\s+");
        int size = params.length;
        WBehavior behavior = new WBehavior();
        if (size > 0) {
            behavior.setUid(params[0]);
        }
        if (size > 1) {
            behavior.setWid(params[1]);
        }
        if (size > 3) {
            behavior.setTime(params[2] + " " + params[3]);
        }
        if (size > 4) {
            behavior.setContent(params[4]);
        }
        return behavior;
    }
}
