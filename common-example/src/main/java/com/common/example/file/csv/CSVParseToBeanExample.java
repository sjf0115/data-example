package com.common.example.file.csv;

import com.common.example.bean.Person;
import com.opencsv.CSVReader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/21 下午11:10
 */
public class CSVParseToBeanExample {
    public static void main(String[] args) {
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader("emps.csv"));

            ColumnPositionMappingStrategy<Person> beanStrategy = new ColumnPositionMappingStrategy<>();
            beanStrategy.setType(Person.class);
            beanStrategy.setColumnMapping(new String[] {"name","age"});

            CsvToBean<Person> csvToBean = new CsvToBean<Person>();

            //List<Person> persons = csvToBean.parse(beanStrategy, reader);
            //System.out.println(persons);

        } catch (FileNotFoundException e) {
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
