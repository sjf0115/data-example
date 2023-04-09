package com.calcite.example.adapter.csv;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;

/**
 * 功能：CsvSchema 工厂类 - 简单实现
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/8 下午5:43
 */
public class SimpleCsvSchemaFactory implements SchemaFactory {
    // 单例模式
    public static final SimpleCsvSchemaFactory INSTANCE = new SimpleCsvSchemaFactory();
    private SimpleCsvSchemaFactory() {

    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        final String directory = (String) operand.get("directory");
        // CSV 文件
        final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
        File directoryFile = new File(directory);
        if (base != null && !directoryFile.isAbsolute()) {
            directoryFile = new File(base, directory);
        }
        // 创建 CsvSchema
        return new SimpleCsvSchema(directoryFile);
    }
}
