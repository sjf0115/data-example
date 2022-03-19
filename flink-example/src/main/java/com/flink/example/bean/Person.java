package com.flink.example.bean;
/**
 * 功能：Pojo 类 Person
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/19 下午3:52
 */
// (1) 必须是 Public 修饰且必须独立定义，不能是内部类
public class Person {
    // (4) 字段类型必须是 Flink 支持的
    private String name;
    private int age;

    // (2) 必须包含一个 Public 修饰的无参构造器
    public Person() {
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    // (3) 所有的字段必须是 Public 或者具有 Public 修饰的 getter 和 setter 方法
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "name: " + name + ", age: " + age;
    }
}