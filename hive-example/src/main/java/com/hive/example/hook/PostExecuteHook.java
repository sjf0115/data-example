package com.hive.example.hook;

import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * 功能：使用 PostExecute 实现 Task 执行后调用 Hook
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/12/29 下午5:28
 */
public class PostExecuteHook implements PostExecute {
    private static final Logger LOG = LoggerFactory.getLogger(PostExecuteHook.class);
    @Override
    public void run(SessionState ss, Set<ReadEntity> inputs, Set<WriteEntity> outputs, LineageInfo lInfo, UserGroupInformation ugi) throws Exception {
        LOG.info("Command: {}", ss.getLastCommand());
    }
}
