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
        // 应用程序类加载器(系统类加载器)
        // ClassLoader systemClassLoader = ClassLoaderParent.class.getClassLoader();
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        System.out.println(systemClassLoader); // sun.misc.Launcher$AppClassLoader@49476842

        // 获取上层加载器:扩展类加载器
        ClassLoader extClassLoader = systemClassLoader.getParent();
        System.out.println(extClassLoader); // sun.misc.Launcher$ExtClassLoader@5acf9800
        
        // 获取上层加载器:启动类加载器
        ClassLoader bootstrapClassLoader = extClassLoader.getParent();
        System.out.println(bootstrapClassLoader); // null
    }
}
