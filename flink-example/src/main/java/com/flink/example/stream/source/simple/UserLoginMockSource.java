package com.flink.example.stream.source.simple;

import com.common.example.bean.LoginUser;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 功能：模拟用户登录数据
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午10:57
 */
public class UserLoginMockSource extends RichParallelSourceFunction<LoginUser> {

    private static final Logger LOG = LoggerFactory.getLogger(UserLoginMockSource.class);
    private volatile boolean cancel = false;
    private List<String> userLogin = Lists.newArrayList(
            "10001,android,1662303772840",  // 23:02:52
            "10002,iOS,1662303770844",      // 23:02:50
            "10003,android,1662303773848",  // 23:02:53
            "10002,android,1662303774866",  // 23:02:54
            "10001,android,1662303777839",  // 23:02:57
            "10004,iOS,1662303784887",      // 23:03:04
            "10007,android,1662303776894",  // 23:02:56
            "10001,android,1662303786891",  // 23:03:06
            "10005,android,1662303778877",  // 23:02:58
            "10004,iOS,1662303791904",      // 23:03:11
            "10003,android,1662303795918",  // 23:03:15
            "10006,iOS,1662303779883",      // 23:02:59
            "10002,iOS,1662303846254"       // 23:04:06

    );

    public UserLoginMockSource() {
    }

    @Override
    public void run(SourceContext<LoginUser> ctx) throws Exception {
        int index = 0;
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                String login = userLogin.get(index);
                String[] params = login.split(",");
                Long uid = Long.parseLong(params[0]);
                String os = params[1];
                Long timestamp = Long.parseLong(params[2]);
                LOG.info("uid: {}, os: {}, timestamp: {}", uid, os, timestamp);
                ctx.collect(new LoginUser(uid, os, timestamp));
            }
            if (index++ >= userLogin.size()-1) {
                cancel = true;
            }
            Thread.sleep(5*1000);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
