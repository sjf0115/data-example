package com.hive.example.hook;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：使用 ExecuteWithHookContext 实现 Task 执行后调用 Hook
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/24 下午9:31
 */
public class PostExecuteWithHookContextHook implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(PostExecuteWithHookContextHook.class);
    @Override
    public void run(HookContext context) throws Exception {
        HookContext.HookType hookType = context.getHookType();
        // Task 执行之后调用
        if (!Objects.equals(hookType, HookContext.HookType.POST_EXEC_HOOK)) {
            return;
        }
        // 执行计划
        QueryPlan plan = context.getQueryPlan();
        // 操作名称
        String operationName = plan.getOperationName();
        // 查询
        String query = plan.getQueryString();

        LOG.info("OperationName: {}", operationName);
        LOG.info("Query: {}", query);
    }
}
