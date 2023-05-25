package com.common.example.file.csv;

import com.common.example.bean.Employee;
import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import com.opencsv.bean.BeanToCsv;
import com.opencsv.bean.ColumnPositionMappingStrategy;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/25 上午7:38
 */
public class BeanToCsvExample {
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

            // 映射策略
            ColumnPositionMappingStrategy<Employee> mappingStrategy = new ColumnPositionMappingStrategy();
            mappingStrategy.setType(Employee.class);
            mappingStrategy.setColumnMapping(new String[] {"id","name","age","country"});

            // 写入文件Bean
            List<Employee> emps = Lists.newArrayList();
            Employee emp1 = new Employee("1", "Pankaj Kumar", 20, "India");
            Employee emp2 = new Employee("2", "David Dan", 40, "USA");
            Employee emp3 = new Employee("3", "Lisa Ray", 28, "Germany");
            emps.add(emp1);
            emps.add(emp2);
            emps.add(emp3);

            // 写入 CSV 文件
            BeanToCsv<Employee> beanToCsv = new BeanToCsv<>();
            beanToCsv.write(mappingStrategy, writer, emps);
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
