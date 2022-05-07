package com.common.example.jvm.classLoader;
/**
 * 功能：ClassLoader 父亲
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/6 下午10:24
 */
public class ClassLoaderParent {
    public static void main(String[] args) {
        // 1. 示例1 加载器层级结构

        // 应用程序类加载器(系统类加载器)
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        System.out.println(systemClassLoader); // sun.misc.Launcher$AppClassLoader@49476842

        // 获取上层加载器:扩展类加载器
        ClassLoader extClassLoader = systemClassLoader.getParent();
        System.out.println(extClassLoader); // sun.misc.Launcher$ExtClassLoader@5acf9800
        
        // 获取上层加载器:启动类加载器
        ClassLoader bootstrapClassLoader = extClassLoader.getParent();
        System.out.println(bootstrapClassLoader); // null

        // 2. 示例2 加载器场景

        // 自定义类使用应用程序类加载器
        ClassLoader classLoader1 = ClassLoaderParent.class.getClassLoader();
        System.out.println(classLoader1); // sun.misc.Launcher$AppClassLoader@49476842

        // Java 核心类库使用启动类加载器
        ClassLoader classLoader2 = Object.class.getClassLoader();
        System.out.println(classLoader2); // null
    }
}
