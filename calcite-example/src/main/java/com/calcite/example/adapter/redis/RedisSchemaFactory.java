package com.calcite.example.adapter.redis;

import com.google.common.base.Preconditions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

/**
 * 功能：RedisSchemaFactory
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/28 下午11:43
 */
public class RedisSchemaFactory implements SchemaFactory {

    public RedisSchemaFactory() {

    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        // operand 自定义的参数
        Object tablesOp = operand.get("tables");
        Object hostOp = operand.get("host");
        Object portOp = operand.get("port");
        Object databaseOp = operand.get("database");
        Object passwordOp = operand.get("password");

        Preconditions.checkArgument(tablesOp != null, "tables must be specified");
        Preconditions.checkArgument(hostOp != null, "host must be specified");
        Preconditions.checkArgument(portOp != null, "port must be specified");
        Preconditions.checkArgument(databaseOp != null, "database must be specified");

        List<Map<String, Object>> tables = (List) tablesOp;
        String host = hostOp.toString();
        int port = Integer.parseInt(portOp.toString());
        int database = Integer.parseInt(databaseOp.toString());
        String password = passwordOp == null ? null : passwordOp.toString();
        return new RedisSchema(host, port, database, password, tables);
    }
}
