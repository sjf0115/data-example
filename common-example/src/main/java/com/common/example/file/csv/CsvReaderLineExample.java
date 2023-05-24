package com.common.example.file.csv;

import com.common.example.bean.Employee;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * 功能：逐行读取 Csv 文件
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/21 下午3:38
 */
public class CsvReaderLineExample {
    public static void main(String[] args) {
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader("/opt/data/emps.csv"), ',');
            List<Employee> employees = Lists.newArrayList();
            String[] record;
            while ((record = reader.readNext()) != null) {
                Employee emp = new Employee();
                emp.setId(record[0]);
                emp.setName(record[1]);
                emp.setAge(Integer.parseInt(record[2]));
                emp.setCountry(record[3]);
                System.out.println(emp);
                employees.add(emp);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
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
