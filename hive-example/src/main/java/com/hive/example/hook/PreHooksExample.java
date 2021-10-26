package com.hive.example.hook;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

/**
 * 功能：Pre Hook Example
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/24 下午2:09
 */
public class PreHooksExample implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {

    }
}
