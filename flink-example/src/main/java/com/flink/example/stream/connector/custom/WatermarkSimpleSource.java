package com.flink.example.stream.connector.custom;


import com.flink.example.bean.Behavior;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * 生成Watermark的简单数据源
 * Created by wy on 2021/1/24.
 */
public class WatermarkSimpleSource extends RichParallelSourceFunction<Behavior> {

    private Random random = new Random();
    private volatile boolean cancel;

    @Override
    public void run(SourceContext<Behavior> ctx) throws Exception {
        while (!cancel) {
//            WBehavior next = ;
//            ctx.collectWithTimestamp(next, next.getEventTimestamp());
//
//            if (next.hasWatermarkTime()) {
//                ctx.emitWatermark(new Watermark(next.getWatermarkTime()));
//            }
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
