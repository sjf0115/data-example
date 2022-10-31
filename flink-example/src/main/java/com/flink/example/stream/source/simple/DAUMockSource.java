package com.flink.example.stream.source.simple;

import com.common.example.bean.LoginUser;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 功能：模拟计算 DAU 数据
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午10:57
 */
public class DAUMockSource extends RichParallelSourceFunction<LoginUser> {

    private static final Logger LOG = LoggerFactory.getLogger(DAUMockSource.class);
    private volatile boolean cancel = false;
    private List<String> userLogin = Lists.newArrayList(
            "1,10001,android,1665417052000",  // 23:50:52
            "1,10002,iOS,1665417005000",      // 23:50:05
            "1,10003,android,1665417076000",  // 23:51:16
            "1,10002,android,1665417144000",  // 23:52:24
            "1,10001,android,1665417217000",  // 23:53:37
            "1,10004,iOS,1665417284000",      // 23:54:44
            "1,10003,android,1665417356000",  // 23:55:56
            "1,10005,android,1665417366000",  // 23:56:06
            "1,10006,android,1665417555000",  // 23:59:15
            "1,10007,iOS,1665417659000",      // 00:00:59
            "1,10001,android,1665417685000",  // 00:01:25
            "1,10008,android,1665417756000"   // 00:02:36
    );

    public DAUMockSource() {
    }

    @Override
    public void run(SourceContext<LoginUser> ctx) throws Exception {
        int index = 0;
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                String login = userLogin.get(index);
                String[] params = login.split(",");
                Integer appId = Integer.parseInt(params[0]);
                Long uid = Long.parseLong(params[1]);
                String os = params[2];
                Long timestamp = Long.parseLong(params[3]);
                LOG.info("AppId: {}, uid: {}, os: {}, timestamp: {}", appId, uid, os, timestamp);
                ctx.collect(new LoginUser(appId, uid, os, timestamp));
            }
            if (index++ >= userLogin.size()-1) {
                cancel = true;
            }
            Thread.sleep(10*1000);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
