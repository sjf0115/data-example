package com.common.example.jvm.classLoader;

import java.lang.reflect.Method;

/**
 * 功能：自定义 ClassLoader 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/7 下午11:01
 */
public class CustomClassLoaderExample {
    public static void main(String[] args) throws Exception {
        CustomClassLoader myClassLoader = new CustomClassLoader("/opt/data");
        Class<?> myClass = myClassLoader.loadClass("com.common.example.bean.Car");
        // 创建对象实例
        Object o = myClass.newInstance();
        // 调用方法
        Method print = myClass.getDeclaredMethod("print", null);
        print.invoke(o, null);
        // 输出类加载器
        System.out.println("ClassLoader: " + o.getClass().getClassLoader());
    }
}
