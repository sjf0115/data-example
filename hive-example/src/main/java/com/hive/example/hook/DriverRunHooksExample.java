package com.hive.example.hook;

import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;

/**
 * 功能：Driver Run Hooks Example
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/24 下午9:34
 */
public class DriverRunHooksExample implements HiveDriverRunHook {
    @Override
    public void preDriverRun(HiveDriverRunHookContext hiveDriverRunHookContext) throws Exception {

    }

    @Override
    public void postDriverRun(HiveDriverRunHookContext hiveDriverRunHookContext) throws Exception {

    }
}
