package com.common.example.bean;
/**
 * 功能：ResourceItem JMX Bean
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午8:55
 */
public class ResourceItem {
    private String name;
    private int age;

    public ResourceItem() {
    }

    public ResourceItem(String name, int age) {
        this.name = name;
        this.age = age;
    }

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
        return "ResourceItem{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
