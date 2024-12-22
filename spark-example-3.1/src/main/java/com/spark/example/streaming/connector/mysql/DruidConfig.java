package com.spark.example.streaming.connector.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 功能：Druid 连接池配置类
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/19 23:02
 */
public class DruidConfig implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DruidConfig.class);

    public static DruidDataSource getDataSource() {
        LOG.info("[INFO] 创建 DruidDataSource");
        DruidDataSource dataSource = new DruidDataSource();

        // 配置数据库连接参数
        dataSource.setUrl("jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC");
        dataSource.setUsername("root");
        dataSource.setPassword("root");

        // 配置连接池参数
        dataSource.setInitialSize(5); // 初始化连接数
        dataSource.setMinIdle(5);     // 最小空闲连接数
        dataSource.setMaxActive(20);  // 最大活跃连接数
        dataSource.setMaxWait(60000); // 获取连接的最大等待时间（毫秒）

        // 其他可选配置
        dataSource.setTimeBetweenEvictionRunsMillis(60000); // 连接池检查连接的间隔时间
        dataSource.setMinEvictableIdleTimeMillis(300000);  // 连接池中连接空闲的最小时间
        dataSource.setValidationQuery("SELECT 1");         // 验证连接是否有效的 SQL 语句
        dataSource.setTestWhileIdle(true);                 // 空闲时是否进行连接的验证

        return dataSource;
    }
}
