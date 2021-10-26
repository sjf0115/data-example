package com.hive.example.hook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreInitContext;
import org.apache.hadoop.hive.metastore.MetaStoreInitListener;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * 功能：MetaStore Init Hooks Example
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/24 下午9:32
 */
public class MetaStoreInitHooksExample extends MetaStoreInitListener {

    public MetaStoreInitHooksExample(Configuration config) {
        super(config);
    }

    @Override
    public void onInit(MetaStoreInitContext metaStoreInitContext) throws MetaException {

    }
}
