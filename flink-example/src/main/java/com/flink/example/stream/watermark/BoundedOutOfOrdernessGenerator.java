package com.flink.example.stream.watermark;

import com.common.example.bean.Behavior;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Created by wy on 2020/12/15.
 */
public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<Behavior> {

    private final long maxOutOfOrderness = 3500;

    private long currentMaxTimestamp;

    @Override
    public void onEvent(Behavior wBehavior, long eventTimestamp, WatermarkOutput watermarkOutput) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }
}
