package com.flink.example.stream.source.simple;

import com.common.example.utils.DateUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 功能：OutOfOrderSource
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/4 下午11:51
 */
public class OutOfOrderSource extends RichParallelSourceFunction<Tuple4<Integer, String, Integer, Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(OutOfOrderSource.class);
    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;
    private volatile boolean cancel;
    private List<Tuple4<Integer, String, Integer, Long>> elements = Lists.newArrayList(
            // 行为唯一标识Id, 单词, 出现次数, 事件时间戳
            Tuple4.of(1, "a", 2, 1662303772840L), // 23:02:52
            Tuple4.of(2, "a", 1, 1662303770844L), // 23:02:50
            Tuple4.of(3, "a", 3, 1662303773848L), // 23:02:53
            Tuple4.of(4, "a", 2, 1662303774866L), // 23:02:54
            Tuple4.of(5, "a", 1, 1662303777839L), // 23:02:57
            Tuple4.of(6, "a", 2, 1662303784887L), // 23:03:04
            Tuple4.of(7, "a", 3, 1662303776894L), // 23:02:56
            Tuple4.of(8, "a", 1, 1662303786891L), // 23:03:06
            Tuple4.of(9, "a", 5, 1662303778877L), // 23:02:58
            Tuple4.of(10, "a", 4, 1662303791904L), // 23:03:11
            Tuple4.of(11, "a", 1, 1662303795918L), // 23:03:15
            Tuple4.of(12, "a", 6, 1662303779883L), // 23:02:59
            Tuple4.of(13, "a", 2, 1662303846254L)  // 23:04:06
    );

    public OutOfOrderSource() {
    }

    public OutOfOrderSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    @Override
    public void run(SourceContext<Tuple4<Integer, String, Integer, Long>> ctx) throws Exception {
        int index = 0;
        while (!cancel && index < elements.size()) {
            synchronized (ctx.getCheckpointLock()) {
                Tuple4<Integer, String, Integer, Long> element = elements.get(index++);
                LOG.info("id: {}, word: {}, count: {}, eventTime: {}|{}",
                        element.f0, element.f1, element.f2,
                        element.f3, DateUtil.timeStamp2Date(element.f3)
                );
                ctx.collect(element);
            }
            Thread.sleep(sleepInterval);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
