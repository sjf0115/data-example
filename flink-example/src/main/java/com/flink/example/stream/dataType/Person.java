package com.flink.example.stream.dataType;

/**
 * 功能：Pojo Class
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 上午11:46
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
}
