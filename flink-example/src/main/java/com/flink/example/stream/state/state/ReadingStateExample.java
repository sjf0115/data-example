package com.flink.example.stream.state.state;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/4 上午8:56
 */
public class ReadingStateExample {
    private static final Logger LOG = LoggerFactory.getLogger(ReadingStateExample.class);

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "hdfs://path/", new HashMapStateBackend());

        DataSet<Integer> listState  = savepoint.readListState("my-uid", "list-state", Types.INT);
    }
}
