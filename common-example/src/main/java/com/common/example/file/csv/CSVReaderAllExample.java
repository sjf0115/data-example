package com.common.example.file.csv;

import com.common.example.bean.Person;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.compress.utils.Lists;

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
public class CSVReaderAllExample {
    public static void main(String[] args) {
        CSVReader reader = null;
        try {
            reader = new CSVReaderBuilder(new FileReader("person.csv")).build();
            List<Person> persons = Lists.newArrayList();
            // 一次性读取全部数据
            List<String[]> records = reader.readAll();
            Iterator<String[]> iterator = records.iterator();
            // 遍历
            while (iterator.hasNext()) {
                String[] record = iterator.next();
                Person person = new Person();
                person.setName(record[0]);
                person.setAge(Integer.valueOf(record[1]));
                persons.add(person);
            }
            System.out.println(persons);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
