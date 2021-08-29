package com.flink.connector.redis.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * 功能：Redis DynamicTableSinkFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/8/18 下午10:10
 */
public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final ConfigOption<String> MODE = ConfigOptions
            .key("mode")
            .stringType()
            .defaultValue("string");
    public static final ConfigOption<String> HOST = ConfigOptions
            .key("host")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("port")
            .intType()
            .defaultValue(6379);
    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .defaultValue("");
    public static final ConfigOption<Integer> DB_NUM = ConfigOptions
            .key("dbNum")
            .intType()
            .defaultValue(0);
    public static final ConfigOption<Boolean> IGNORE_DELETE = ConfigOptions
            .key("ignoreDelete")
            .booleanType()
            .defaultValue(false);
    public static final ConfigOption<Boolean> CLUSTER_MODE = ConfigOptions
            .key("clusterMode")
            .booleanType()
            .defaultValue(false);

    /**
     * 创建 DynamicTableSink
     * @param context
     * @return
     */
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        return new RedisDynamicTableSink(options);
    }

    /**
     * Connector 唯一标识符
     * @return
     */
    public String factoryIdentifier() {
        return "redis";
    }

    /**
     * 必填参数
     * @return
     */
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(MODE);
        requiredOptions.add(HOST);
        return requiredOptions;
    }

    /**
     * 可选参数
     * @return
     */
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(PORT);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(DB_NUM);
        optionalOptions.add(IGNORE_DELETE);
        optionalOptions.add(CLUSTER_MODE);
        return optionalOptions;
    }
}
