package com.calcite.example.adapter.csv;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.util.Map;

/**
 * 功能：CsvSchema 简单实现
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/8 下午5:59
 */
public class SimpleCsvSchema extends AbstractSchema {
    private final File directoryFile;

    public SimpleCsvSchema(File directoryFile) {
        super();
        this.directoryFile = directoryFile;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        // 寻找指定目录下以 .csv 结尾的文件
        final Source baseSource = Sources.of(directoryFile);
        File[] files = directoryFile.listFiles((dir, name) -> {
            return name.endsWith(".csv");
        });
        if (files == null) {
            System.out.println("directory " + directoryFile + " not found");
            files = new File[0];
        }
        // 文件与 Table 映射
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (File file : files) {
            Source source = Sources.of(file);
            final Source csvSource = source.trimOrNull(".csv");
            if (csvSource == null) {
                continue;
            }
            // 根据文件创建对应的 Table
            final Table table = new SimpleCsvTable(source);
            builder.put(csvSource.relative(baseSource).path(), table);
        }
        return builder.build();
    }
}
