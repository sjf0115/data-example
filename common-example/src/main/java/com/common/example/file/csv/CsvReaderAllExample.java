package com.common.example.file.csv;

import com.common.example.bean.Employee;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 功能：一次性读取 Csv 文件全部数据
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/21 下午11:06
 */
public class CsvReaderAllExample {
    public static void main(String[] args) {
        CSVReader reader = null;
        try {
            reader = new CSVReaderBuilder(new FileReader("/opt/data/emps.csv")).build();
            List<Employee> employees = Lists.newArrayList();
            // 一次性读取全部数据
            List<String[]> records = reader.readAll();
            Iterator<String[]> iterator = records.iterator();
            // 遍历
            while (iterator.hasNext()) {
                String[] record = iterator.next();
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
