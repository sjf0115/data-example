package com.flink.example.stream.source.watermark;


import com.common.example.bean.UserBehavior;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;

/**
 * 生成Watermark的简单数据源
 * Created by wy on 2021/1/24.
 */
public class WatermarkSimpleSource extends RichParallelSourceFunction<UserBehavior> {

    private static Gson gson = new GsonBuilder().create();
    private volatile boolean cancel;

    private List<String> behaviors = Lists.newArrayList();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        behaviors.add("{\"uid\":1,\"pid\":3745169,\"cid\":2891509,\"type\":\"pv\",\"timestamp\":1511725471000,\"time\":\"2017-11-27 03:44:31\"}");
        behaviors.add("{\"uid\":1,\"pid\":1531036,\"cid\":2920476,\"type\":\"pv\",\"timestamp\":1511733732000,\"time\":\"2017-11-27 06:02:12\"}");
        behaviors.add("{\"uid\":1,\"pid\":2266567,\"cid\":4145813,\"type\":\"pv\",\"timestamp\":1511741471000,\"time\":\"2017-11-27 08:11:11\"}");
        behaviors.add("{\"uid\":1,\"pid\":2951368,\"cid\":1080785,\"type\":\"pv\",\"timestamp\":1511750828000,\"time\":\"2017-11-27 10:47:08\"}");
        behaviors.add("{\"uid\":1,\"pid\":3108797,\"cid\":2355072,\"type\":\"pv\",\"timestamp\":1511758881000,\"time\":\"2017-11-27 13:01:21\"}");
        behaviors.add("{\"uid\":1,\"pid\":1338525,\"cid\":149192,\"type\":\"pv\",\"timestamp\":1511773214000,\"time\":\"2017-11-27 17:00:14\"}");
        behaviors.add("{\"uid\":1,\"pid\":2286574,\"cid\":2465336,\"type\":\"pv\",\"timestamp\":1511797167000,\"time\":\"2017-11-27 23:39:27\"}");
        behaviors.add("{\"uid\":1,\"pid\":5002615,\"cid\":2520377,\"type\":\"pv\",\"timestamp\":1511839385000,\"time\":\"2017-11-28 11:23:05\"}");
        behaviors.add("{\"uid\":1,\"pid\":2734026,\"cid\":4145813,\"type\":\"pv\",\"timestamp\":1511842184000,\"time\":\"2017-11-28 12:09:44\"}");
        behaviors.add("{\"uid\":1,\"pid\":5002615,\"cid\":2520377,\"type\":\"pv\",\"timestamp\":1511844273000,\"time\":\"2017-11-28 12:44:33\"}");
        behaviors.add("{\"uid\":1,\"pid\":3239041,\"cid\":2355072,\"type\":\"pv\",\"timestamp\":1511855664000,\"time\":\"2017-11-28 15:54:24\"}");
        behaviors.add("{\"uid\":1,\"pid\":4615417,\"cid\":4145813,\"type\":\"pv\",\"timestamp\":1511870864000,\"time\":\"2017-11-28 20:07:44\"}");
    }

    @Override
    public void run(SourceContext<UserBehavior> ctx) throws Exception {
        int index = 0;
        while (!cancel) {
            UserBehavior behavior = gson.fromJson(behaviors.get(index), UserBehavior.class);
            // 事件时间戳提取
            ctx.collectWithTimestamp(behavior, behavior.getTimestamp());
            // 生成 Watermark
            ctx.emitWatermark(new Watermark(behavior.getTimestamp() - 10*10000));
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
