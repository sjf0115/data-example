package com.common.example.file.csv;

import com.common.example.bean.Employee;
import com.common.example.bean.Person;
import com.opencsv.CSVReader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * 功能：将 CSV 转换为 Java 对象
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/21 下午11:10
 */
public class CsvToBeanExample {
    public static void main(String[] args) {
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader("/opt/data/emps.csv"));
            // 列映射策略-根据列位置进行映射
            ColumnPositionMappingStrategy<Employee> beanStrategy = new ColumnPositionMappingStrategy<>();
            beanStrategy.setType(Employee.class);
            beanStrategy.setColumnMapping(new String[] {"id","name","age","country"});
            // Csv 转换为 Employee
            CsvToBean<Employee> csvToBean = new CsvToBean<>();
            List<Employee> employees = csvToBean.parse(beanStrategy, reader);
            for (Employee emp : employees) {
                System.out.println(emp);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
