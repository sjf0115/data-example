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
        // 应用程序类加载器 sun.misc.Launcher$AppClassLoader@49476842
        ClassLoader classLoader = ClassLoaderParent.class.getClassLoader();
        System.out.println(classLoader);
        // 扩展类加载器 sun.misc.Launcher$ExtClassLoader@5acf9800
        System.out.println(classLoader.getParent());
        // 启动类加载器 null
        System.out.println(classLoader.getParent().getParent());
    }
}
