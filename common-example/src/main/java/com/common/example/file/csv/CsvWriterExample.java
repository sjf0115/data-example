package com.common.example.file.csv;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * 功能：写入 CSV 文件
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/24 下午11:28
 */
public class CsvWriterExample {
    public static void main(String[] args) {
        CSVWriter writer = null;
        try {
            writer = new CSVWriter(
                    new FileWriter("/opt/data/emps-output.csv"),
                    CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.NO_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END
            );
            // 写入文件的数据
            List<String[]> records = Lists.newArrayList();
            // 添加标题行
            records.add(new String[] { "id", "name", "age", "country" });
            // 添加数据行
            records.add(new String[] {"1", "Pankaj Kumar", "20", "India"});
            records.add(new String[] {"2", "David Dan", "40", "USA"});
            records.add(new String[] {"3", "Lisa Ray", "28", "Germany"});

            // 写入
            writer.writeAll(records);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
